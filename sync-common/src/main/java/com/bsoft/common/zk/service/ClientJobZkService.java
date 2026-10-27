package com.bsoft.common.zk.service;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.enums.JobLaunchMode;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.bsoft.common.zk.ZkClientRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.Map;
import com.bsoft.common.enums.JobStartupMode;

/**
 * client job 级 ZooKeeper 服务。
 *
 * 职责：
 * 1. 管理单个 dataSourceId 对应的 job 级 ZK 节点。
 * 2. 管理 job status / scn 等 job 运行态数据。
 * 3. 封装 job 启动、快照完成、增量运行等状态转换语义。
 */
public class ClientJobZkService {

    private static final Logger logger = LogManager.getLogger(ClientJobZkService.class);

    /**
     * client 配置。
     */
    private final ClientInfo clientInfo;

    /**
     * 当前 job 对应的数据源 ID。
     */
    private final String dataSourceId;


    /**
     * 当前 job 对应的数据源机构名称。
     */
    private final String dataSourceOrg;

    /**
     * 当前 job 使用的底层 ZK 客户端。
     */
    private final ZkClient zkClient;

    /**
     * 当前实例是否持有共享连接引用。
     */
    private final boolean sharedClient;

    /**
     * 当前 job 的根路径。
     */
    private final String jobRootPath;

    /**
     * 当前 job 的 status 路径。
     */
    private final String jobStatusPath;

    /**
     * 当前 job 的 scn 路径。
     */
    private final String jobScnPath;

    /**
     * 当前 job 的在线标记路径。
     */
    private final String jobAlivePath;

    // 构造当前 dataSourceId 对应的 job 级 ZK 服务。
    private ClientJobZkService(ClientInfo clientInfo,
                               String dataSourceId,
                               String dataSourceOrg,
                               ZkClient zkClient,
                               boolean sharedClient) {
        // 校验 client 配置不能为空。
        if (clientInfo == null) {
            throw new IllegalArgumentException("ClientInfo cannot be null");
        }

        // 校验 dataSourceId 不能为空。
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("dataSourceId cannot be blank");
        }

        // 校验 ZK 客户端不能为空。
        if (zkClient == null) {
            throw new IllegalArgumentException("ZkClient cannot be null");
        }

        // 保存 client 配置。
        this.clientInfo = clientInfo;

        // 保存标准化后的数据源 ID。
        this.dataSourceId = dataSourceId.trim();

        // 保存标准化后的业务数据源机构名称。
        this.dataSourceOrg = dataSourceOrg == null ? "" : dataSourceOrg.trim();

        // 保存底层 ZK 客户端。
        this.zkClient = zkClient;

        // 保存当前服务是否持有共享连接引用。
        this.sharedClient = sharedClient;

        // 初始化当前 job 根路径。
        this.jobRootPath = clientInfo.getJobRootPath(this.dataSourceId);

        // 初始化当前 job 状态路径。
        this.jobStatusPath = clientInfo.getJobStatusPath(this.dataSourceId);

        // 初始化当前 job SCN 路径。
        this.jobScnPath = clientInfo.getJobScnPath(this.dataSourceId);

        // 初始化当前 job 在线标记路径。
        this.jobAlivePath = clientInfo.getJobAlivePath(this.dataSourceId);
    }

    // 打开不携带 dataSourceOrg 的 job 级 ZK 服务，兼容现有调用方。
    public static ClientJobZkService open(ClientInfo clientInfo, String dataSourceId) throws Exception {
        // 复用三参 open，未提供机构名称时写空字符串。
        return open(clientInfo, dataSourceId, "");
    }

    // 打开携带 dataSourceOrg 的 job 级 ZK 服务。
    public static ClientJobZkService open(ClientInfo clientInfo, String dataSourceId, String dataSourceOrg) throws Exception {
        // 从 JVM 共享注册表获取 ZK 客户端。
        ZkClient zkClient = ZkClientRegistry.acquire(clientInfo.getZooKeeperConfig());

        // 创建持有共享连接引用的 job 级服务。
        return new ClientJobZkService(clientInfo, dataSourceId, dataSourceOrg, zkClient, true);
    }

    // 使用外部 ZkClient 创建不携带 dataSourceOrg 的 job 级 ZK 服务，兼容现有调用方。
    public static ClientJobZkService using(ClientInfo clientInfo, String dataSourceId, ZkClient zkClient) {
        // 复用四参 using，未提供机构名称时写空字符串。
        return using(clientInfo, dataSourceId, "", zkClient);
    }

    // 使用外部 ZkClient 创建携带 dataSourceOrg 的 job 级 ZK 服务。
    public static ClientJobZkService using(ClientInfo clientInfo,
                                           String dataSourceId,
                                           String dataSourceOrg,
                                           ZkClient zkClient) {
        // 创建不持有共享连接引用的 job 级服务。
        return new ClientJobZkService(clientInfo, dataSourceId, dataSourceOrg, zkClient, false);
    }

    // 初始化当前 job 的节点结构。
    public void initJobNodes() throws Exception {
        // 确保当前 client 的 jobs 根节点存在。
        zkClient.createJsonIfAbsent(clientInfo.getClientJobsRootPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);

        // 每次初始化都刷新 dataSourceId 根节点的数据。
        zkClient.createOrSetJson(jobRootPath, JsonUtil.jobRootToMap(dataSourceOrg), CreateMode.PERSISTENT);

        // 确保当前 job 的 status 持久化节点存在。
        zkClient.createJsonIfAbsent(jobStatusPath, JsonUtil.emptyObject(), CreateMode.PERSISTENT);

        // 确保当前 job 的 scn 持久化节点存在。
        zkClient.createJsonIfAbsent(jobScnPath, JsonUtil.emptyObject(), CreateMode.PERSISTENT);

        // 确保当前 job 的在线临时节点存在。
        zkClient.createJsonIfAbsent(jobAlivePath, JsonUtil.emptyObject(), CreateMode.EPHEMERAL);

        // 记录初始化完成日志。
        logger.info("Initialized client job nodes, clientId={}, dataSourceId={}, dataSourceOrg={}",
                clientInfo.getClientId(),
                dataSourceId,
                dataSourceOrg);
    }


    /**
     * 更新当前 job 状态节点。
     */
    public void updateJobStatus(JobStatus jobStatus) throws Exception {
        // JobStatus 转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkClient.createOrSetJson(
                jobStatusPath,
                JsonUtil.jobStatusToMap(jobStatus),
                CreateMode.PERSISTENT
        );
    }

    /**
     * 更新当前 job 状态节点。
     */
    public void updateJobStatusData(Map<String, String> data) throws Exception {
        // 调用方传入的状态数据必须是结构化对象。
        zkClient.createOrSetJson(
                jobStatusPath,
                data == null ? JsonUtil.emptyObject() : data,
                CreateMode.PERSISTENT
        );
    }


    /**
     * 写入当前 job 的最新 SCN。
     */
    public void updateScn(String scn) throws Exception {
        // 空 SCN 不写入，避免覆盖有效监控值。
        if (scn == null || scn.trim().isEmpty()) {
            return;
        }

        // SCN 转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkClient.createOrSetJson(
                jobScnPath,
                JsonUtil.scnToMap(scn.trim()),
                CreateMode.PERSISTENT
        );
    }



    /**
     * 清空 SCN。
     */
    public void clearScn() throws Exception {
        // 清空也写 {}，不写空字节。
        zkClient.createOrSetJson(jobScnPath, JsonUtil.emptyObject(), CreateMode.PERSISTENT);
    }








    /**
     * 清空当前 job 的运行态数据。
     */
    public void clearJobRuntimeState() throws Exception {

        clearScn();
    }



    // 准备当前 job 的运行态 ZK 节点，默认按首次启动语义清空 scn。
    public void prepareJobRuntimeState() throws Exception {
        // 兼容旧调用方，保持原有行为。
        prepareJobRuntimeState(JobLaunchMode.INITIAL_START);
    }

    // 按启动场景准备当前 job 的运行态 ZK 节点。
    public void prepareJobRuntimeState(JobLaunchMode launchMode) throws Exception {
        // 启动场景不能为空，避免隐式使用错误清理策略。
        if (launchMode == null) {
            throw new IllegalArgumentException("JobLaunchMode cannot be null");
        }

        // 创建当前 job 所需的持久化节点和 alive 临时节点。
        initJobNodes();

        // 只有首次启动才清空 scn，失败重启保留最近一次观测到的 scn。
        if (launchMode.isClearScnBeforeStart()) {
            clearJobRuntimeState();
        }

        // 记录 job 运行态准备完成日志。
        logger.info("Prepared client job runtime state, clientId={}, dataSourceId={}, launchMode={}, clearScnBeforeStart={}",
                clientInfo.getClientId(),
                dataSourceId,
                launchMode,
                launchMode.isClearScnBeforeStart());
    }

    /**
     * 根据启动模式写入当前 job 的运行状态。
     */
    public void updateRunningStatus(JobStartupMode startupMode) throws Exception {
        // 启动模式不能为空。
        if (startupMode == null) {
            throw new IllegalArgumentException("JobStartupMode cannot be null");
        }

        // 根据启动模式写入对应的 job 状态。
        switch (startupMode) {
            case NORMAL:
                // 普通启动模式进入增量运行状态。
                markIncrementalRunning();
                break;

            case SNAPSHOT:
                // 快照启动模式进入快照运行状态。
                markSnapshotRunning();
                break;

            default:
                // 未支持的启动模式直接失败。
                throw new IllegalStateException("Unsupported startup mode: " + startupMode);
        }
    }

    /**
     * 标记当前 job 进入快照运行状态。
     */
    public void markSnapshotRunning() throws Exception {
        // 写入快照运行状态。
        updateJobStatus(JobStatus.SNAPSHOT_RUNNING);

        // 记录状态更新日志。
        logger.info("Updated job status to SNAPSHOT_RUNNING, clientId={}, dataSourceId={}",
                clientInfo.getClientId(),
                dataSourceId);
    }

    /**
     * 标记当前 job 进入增量运行状态。
     */
    public void markIncrementalRunning() throws Exception {
        // 写入增量运行状态。
        updateJobStatus(JobStatus.INCREMENTAL_RUNNING);

        // 记录状态更新日志。
        logger.info("Updated job status to INCREMENTAL_RUNNING, clientId={}, dataSourceId={}",
                clientInfo.getClientId(),
                dataSourceId);
    }

    /**
     * 标记快照完成，并将当前 job 切换为增量运行状态。
     */
    public void markSnapshotCompletedAndIncrementalRunning() throws Exception {
        // 快照完成后的最终 job 运行状态就是增量运行。
        markIncrementalRunning();

        // 记录快照完成后的状态切换日志。
        logger.info("Snapshot completed, switched job status to INCREMENTAL_RUNNING, clientId={}, dataSourceId={}",
                clientInfo.getClientId(),
                dataSourceId);
    }



















    /**
     * 关闭当前 job 服务。
     *
     * 如果底层连接来自共享注册表，则释放一份引用。
     */
    public void close() {
        if (sharedClient) {
            ZkClientRegistry.release(clientInfo.getZooKeeperConfig());
        }
    }

    public String getJobStatusPath() {
        return jobStatusPath;
    }


    public String getJobScnPath() {
        return jobScnPath;
    }


    public String getDataSourceId() {
        return dataSourceId;
    }
}
