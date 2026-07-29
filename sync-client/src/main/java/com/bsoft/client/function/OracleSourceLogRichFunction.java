package com.bsoft.client.function;

import com.bsoft.client.scn.DataSourceScnCommitEvent;
import com.bsoft.common.config.properties.ClientInfo;

import com.bsoft.common.zk.service.ClientJobZkService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Oracle Source 日志增强 MapFunction
 *
 * 运行位置：
 * 1. 运行在 Flink 任务内部
 * 2. 位于 Oracle CDC Source 之后、Kafka Sink 之前
 *
 * 职责：
 * 1. 透传 CDC JSON 消息，不改变消息体
 * 2. 从消息中提取 source.scn
 * 3. 将 SCN 写入当前业务库对应的 job 级 ZK scn 节点
 *
 * 当前版本设计：
 * 1. 一个业务库 job 对应一个 OracleSourceLogRichMapFunction 实例
 * 2. 每个实例只负责一个 dataSourceId
 * 3. 不再兼容旧版“全客户端共享一个 scn 节点”的模式
 *
 * SCN 路径：
 * /clients/{clientId}/jobs/{dataSourceId}/scn
 */
public class OracleSourceLogRichFunction extends ProcessFunction<String, String> {

    private static final Logger logger = LogManager.getLogger(OracleSourceLogRichFunction.class);

    /**
     * JSON 解析器
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 客户端配置
     */
    private final ClientInfo clientInfo;

    /**
     * 当前业务库 ID
     *
     * 说明：
     * 该函数只服务于一个 dataSourceId 对应的 job。
     */
    private final String dataSourceId;


    // 数据源机构名称
    private final String dataSourceOrg;


    /**
     * 当前 job 对应的 job 级 ZK 服务。
     *
     * 说明：
     * 1. 当前类直接依赖 ClientJobZkService
     * 2. 不直接接触底层 ZkClient
     */
    private transient ClientJobZkService jobZkService;

    /**
     * 当前窗口内最新观察到的 SCN。
     *
     * 说明：
     * 1. 每条消息都会尝试更新它
     * 2. 到达 flush 时间窗口后，把它写入 ZK
     * 3. 这里只是监控用途，不要求单调递增
     */
    private transient String latestScn;

    /**
     * 上一次写 ZK 的时间戳。
     *
     * 用于控制写入频率。
     */
    private transient long lastScnFlushTs;

    /**
     * 当前 SCN 写入间隔，单位毫秒。
     *
     * 默认值由 BaseInfo.scnFlushIntervalMs 提供。
     */
    private transient long scnFlushIntervalMs;

    private static final long DEFAULT_SCN_FLUSH_INTERVAL_SECONDS = 60L;

    /**
     * 最近一次识别到的阶段。
     *
     * 取值建议：
     * - SNAPSHOT
     * - INCREMENTAL
     *
     * 目前仅用于日志观察，不触发额外 flush 行为。
     */
    private transient String currentPhase;

    /**
     * 仅用于日志观察的记录计数。
     */
    private long recordCount = 0L;


    // snapshot 完成控制消息侧输出标签
    private OutputTag<String> snapshotControlOutputTag;

    public OracleSourceLogRichFunction(ClientInfo clientInfo,
                                          String dataSourceId,
                                          String dataSourceOrg,
                                          OutputTag<String> snapshotControlOutputTag) {
        this.clientInfo = clientInfo;
        this.dataSourceId = dataSourceId;
        this.dataSourceOrg = dataSourceOrg;
        this.snapshotControlOutputTag = snapshotControlOutputTag;
    }

    /**
     * Flink 生命周期：算子初始化。
     *
     * 说明：
     * 1. 当前算子直接使用 ClientJobZkService 作为 job 级正式入口
     * 2. 底层 ZkClient 是否复用，由 ClientJobZkService.open(...) 内部决定
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.jobZkService = ClientJobZkService.open(clientInfo, dataSourceId);

        // 初始化 SCN 监控相关状态。
        this.latestScn = null;
        this.lastScnFlushTs = System.currentTimeMillis();
        this.currentPhase = "UNKNOWN";

        // 直接从 clientInfo 读取。
        this.scnFlushIntervalMs = resolveScnFlushIntervalMs();

        logger.info("OracleSourceLogRichMapFunction initialized, clientId={}, dataSourceId={}, scnFlushIntervalMs={}",
                clientInfo.getClientId(),
                dataSourceId,
                scnFlushIntervalMs);
    }


    /**
     * 解析 SCN flush 间隔。
     *
     * 规则：
     * 1. 直接使用 clientInfo.getScnFlushIntervalMs()
     * 2. 若值小于等于 0，则兜底回退到 60000 毫秒
     *
     * 说明：
     * BaseInfo 已经提供了默认值 60 秒，这里的兜底只是防御性保护。
     */
    private long resolveScnFlushIntervalMs() {
        long configuredSeconds = clientInfo.getScnFlushIntervalSeconds();
        if (configuredSeconds <= 0L) {
            logger.warn(
                    "Invalid scnFlushIntervalSeconds in clientInfo, fallback to {}s. clientId={}, dataSourceId={}, configured={}",
                    DEFAULT_SCN_FLUSH_INTERVAL_SECONDS,
                    clientInfo.getClientId(),
                    dataSourceId,
                    configuredSeconds
            );
            configuredSeconds = DEFAULT_SCN_FLUSH_INTERVAL_SECONDS;
        }
        return Duration.ofSeconds(configuredSeconds).toMillis();
    }

    /**
     * 主处理逻辑
     *
     * 流程：
     * 1. 透传原始 JSON
     * 2. 从 JSON 中读取 source.scn
     * 3. 将 scn 写入当前业务库自己的 ZK scn 节点
     *
     * 注意：
     * 1. 这里只做“尽量更新”，不改变消息内容
     * 2. 如果消息中没有 source.scn，则直接透传
     */
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {


        logger.info(" {}->{} 输出到kafka消息数量: {}",dataSourceId,dataSourceOrg, ++recordCount);

        // 每 10000 条打印一次消息预览，避免日志过多。
        if (recordCount % 10000 == 0) {
            logger.info("CDC message preview, clientId={}, dataSourceId={}, count={}, payload={}",
                    clientInfo.getClientId(),
                    dataSourceId,
                    recordCount,
                    value);
        }


        // 解析 JSON 根节点
        JsonNode root = OBJECT_MAPPER.readTree(value);


        // 如果是控制消息，则输出到控制侧流，不再进入普通数据主流
        if (isControlMessage(root)) {
            // 如果是 snapshot 完成控制消息，则输出到 snapshot 控制侧流
            if (isSnapshotCompleteControlMessage(root)) {

                // 快照完成后，将 job 状态切换为增量运行。
                markSnapshotCompletedAndIncrementalRunningQuietly(value);

                // 输出控制消息到侧输出流
                ctx.output(snapshotControlOutputTag, value);

                // 记录 snapshot 完成控制消息日志
                logger.info("Snapshot complete control message routed to side output, clientId={}, dataSourceId={}, payload={}",
                        clientInfo.getClientId(),
                        dataSourceId,
                        value);
            } else {
                // 记录未知控制消息日志
                logger.warn("Unknown control message ignored, clientId={}, dataSourceId={}, payload={}",
                        clientInfo.getClientId(),
                        dataSourceId,
                        value);
            }

            // 控制消息不再进入主流，避免写入 Kafka
            return;
        }

        JsonNode sourceNode = root.path("source");

        // 识别当前阶段。
        // 规则：
        // 1. op = r 视为快照
        // 2. source.snapshot = true 也视为快照
        // 3. 其余视为增量
        String phase = resolvePhase(root, sourceNode);
        this.currentPhase = phase;

        // 读取 source.scn。
        if (sourceNode.has("scn")) {
            String scn = sourceNode.get("scn").asText();

            if (scn != null && !scn.trim().isEmpty()) {
                // 只更新内存中的 latestScn，不要求单调递增。
                this.latestScn = scn.trim();
            }
        }

        // 按时间窗口写入 ZK。
        // 这里只用于监控，因此只做定时写入，不做 close flush，不做阶段切换 flush。
        long now = System.currentTimeMillis();
        if (latestScn != null && (now - lastScnFlushTs) >= scnFlushIntervalMs) {
            jobZkService.updateScn(latestScn);
            lastScnFlushTs = now;

            logger.info("SCN flushed to zk, clientId={}, dataSourceId={}, phase={}, scn={}, flushIntervalMs={}",
                    clientInfo.getClientId(),
                    dataSourceId,
                    currentPhase,
                    latestScn,
                    scnFlushIntervalMs);
        }



        // 透传原始消息，不修改内容。
        out.collect(value);

    }


    // 判断当前消息是否为控制消息
    private boolean isControlMessage(JsonNode root) {
        // control_type 存在就认为是控制消息
        return root != null && root.hasNonNull("control_type");
    }

    // 判断当前控制消息是否为 snapshot 完成消息
    private boolean isSnapshotCompleteControlMessage(JsonNode root) {
        // 如果不是控制消息，直接返回 false
        if (!isControlMessage(root)) {
            return false;
        }

        // 读取控制消息类型
        String controlType = root.path("control_type").asText(null);

        // 判断是否为 snapshot 完成
        return "SNAPSHOT_COMPLETE".equalsIgnoreCase(controlType);
    }




    /**
     * 识别当前消息所属阶段。
     *
     * 规则：
     * 1. op = r => SNAPSHOT
     * 2. source.snapshot = true => SNAPSHOT
     * 3. 其余 => INCREMENTAL
     *
     * 这里仅用于监控日志分类，不触发特殊控制逻辑。
     */
    private String resolvePhase(JsonNode root, JsonNode sourceNode) {
        String op = root.path("op").asText(null);
        if ("r".equalsIgnoreCase(op)) {
            return "SNAPSHOT";
        }

        String snapshot = sourceNode.path("snapshot").asText(null);
        if ("true".equalsIgnoreCase(snapshot)) {
            return "SNAPSHOT";
        }

        return "INCREMENTAL";
    }





    /**
     * 快照完成后，静默切换 job 状态为增量运行。
     */
    private void markSnapshotCompletedAndIncrementalRunningQuietly(String payload) {
        try {
            // 使用当前算子已持有的 job 级 ZK 服务更新状态。
            jobZkService.markSnapshotCompletedAndIncrementalRunning();
        } catch (Exception e) {
            // ZK 状态更新失败只记录日志，不中断 Flink 主处理链路。
            logger.warn("Failed to switch job status after snapshot complete, clientId={}, dataSourceId={}, payload={}",
                    clientInfo.getClientId(),
                    dataSourceId,
                    payload,
                    e);
        }
    }



    /**
     * Flink 生命周期：算子关闭。
     *
     * 说明：
     * 1. 按当前约定，close() 阶段不额外 flush SCN
     * 2. 这里只关闭 job 级服务，释放共享连接引用
     */
    @Override
    public void close() throws Exception {
        try {
            if (jobZkService != null) {
                jobZkService.close();
            }

            logger.info("OracleSourceLogRichMapFunction closed, clientId={}, dataSourceId={}, totalCount={}, latestScn={}, phase={}",
                    clientInfo.getClientId(),
                    dataSourceId,
                    recordCount,
                    latestScn,
                    currentPhase);
        } finally {
            super.close();
        }
    }

}