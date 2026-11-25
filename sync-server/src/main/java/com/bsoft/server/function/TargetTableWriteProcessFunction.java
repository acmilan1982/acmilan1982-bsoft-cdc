package com.bsoft.server.function;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.OperationResult;
import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.bean.SyncLogEntry;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.DorisTableModel;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.k2db.TargetTableWriteContext;
import com.bsoft.server.k2db.TopicRuntimeState;
import com.bsoft.server.offset.TopicOffsetCommitEvent;
import com.bsoft.server.sql.ddl.DDLStrategy;
import com.bsoft.server.sql.delete.DeleteStrategy;
import com.bsoft.server.sql.factory.DDLStrategyFactory;
import com.bsoft.server.sql.factory.DeleteStrategyFactory;
import com.bsoft.server.sql.factory.InsertStrategyFactory;
import com.bsoft.server.sql.factory.SelectStrategyFactory;
import com.bsoft.server.sql.factory.UpdateStrategyFactory;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.select.SelectStrategy;
import com.bsoft.server.sql.update.UpdateStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.util.DateFormatUtils;
import com.bsoft.server.api.SyncClientHTTPClient;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.bsoft.common.enums.TopicSourceKind;



// 目标表级写入算子，一个实例负责多个 topic 写入同一张目标表
// 目标表级写入算子。
// 使用 KeyedProcessFunction 是为了支持 Flink processing-time timer。
// 输入 key 为 KafkaToDB 中生成的目标表固定 key。
public class TargetTableWriteProcessFunction
        extends KeyedProcessFunction<String, KafkaPayload, OperationResult> {
    // 日志对象
    private static final Logger logger = LogManager.getLogger(TargetTableWriteProcessFunction.class);

    // JSON 工具
    private static final ObjectMapper mapper = new ObjectMapper();

    // 服务端配置
    private final ServerInfo serverInfo;

    // 目标表写入上下文
    private final TargetTableWriteContext writeContext;

    // 日志侧输出标签
    private final OutputTag<String> syncLogEntryOutputTag;

    // 监控侧输出标签，当前保留
    private final OutputTag<String> metricEventOutputTag;

    // offset commit 侧输出标签
    private final OutputTag<TopicOffsetCommitEvent> topicOffsetCommitOutputTag;

    // 目标数据源配置
    private final TargetDataSourceConfig targetDataSourceConfig;

    // 目标表名
    private final String tableName;

    // 目标表对应的 topic 运行态
    private final Map<String, TopicRuntimeState> topicRuntimeStateByTopic = new LinkedHashMap<>();



    // 日志缓冲队列
    private final List<SyncLogEntry> pendingLogEntries = new ArrayList<>();

    // 表行删除策略
    private final TableRowDeleteStrategy tableRowDeleteStrategy;

    // 快照批量大小
    private final int snapshotBatchSize;

    // 批次大小管理器
    private transient BatchSizeManager batchSizeManager;

    // 查询策略
    private transient SelectStrategy selectStrategy;

    // 插入策略
    private transient InsertStrategy insertStrategy;

    // 删除策略
    private transient DeleteStrategy deleteStrategy;

    // 更新策略
    private transient UpdateStrategy updateStrategy;

    // DDL 策略
    private transient DDLStrategy ddlStrategy;

    // 目标表字段列表
    private transient List<ColumnInfo> columnInfoList;

    // Doris 表模型
    private transient DorisTableModel tableModel;

    // 宽表字段数量阈值，用于判断是否需要字段补齐
    private static final int WIDE_TABLE_COLUMN_THRESHOLD = 100;

    // 字段结构分组数量阈值，用于判断是否需要字段补齐
    private static final int PADDING_GROUP_THRESHOLD = 10;

    // 用于向探针端请求建表
    private transient SyncClientHTTPClient clientHTTPClient;

    // PIPELINE_SQL_RESULT + Doris UNIQUE 表时，从目标表读取到的 UNIQUE KEY
    private final LinkedHashSet<String> targetUniqueKeySet = new LinkedHashSet<>();

    // 目标表检查间隔，和 DatabaseRichProcessBatchFunction 保持一致
    private static final long TABLE_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3);


    // 快照缓冲区，按目标表聚合，允许混合多个 topic
    private final List<KafkaPayload> snapshotBuffer = new ArrayList<>();

    // 实时新增数据缓冲区。
    // 只缓存非快照阶段 op 为 r/c 的数据。
    // 该缓冲区与 snapshotBuffer 完全独立。
    private final List<KafkaPayload> realtimeInsertBuffer = new ArrayList<>();

    // 当前已注册的实时新增 flush timer 时间戳。
    // -1 表示当前没有 active timer。
    // 每个 TargetTableWriteProcessFunction 实例最多维护一个实时 timer。
    private transient long realtimeInsertFlushTimerTs = -1L;


    // 实时更新、删除的固定批量条数阈值，各自独立计数。
    private static final int REALTIME_MUTATION_BATCH_SIZE = 500;

    // 实时更新缓冲区，按消息到达顺序保存。
    private final List<KafkaPayload> realtimeUpdateBuffer = new ArrayList<>();

    // 实时删除缓冲区，按消息到达顺序保存。
    private final List<KafkaPayload> realtimeDeleteBuffer = new ArrayList<>();

    // 实时增删改是否采用批量缓冲，在 open 中读取一次配置。
    // 当前 writer 运行期间保持不变，配置变更在服务重启后生效。
    private transient boolean realtimeBatchEnabled;

    // 创建目标表级写入算子
    public TargetTableWriteProcessFunction(ServerInfo serverInfo,
                                           TargetTableWriteContext writeContext,
                                           OutputTag<String> syncLogEntryOutputTag,
                                           OutputTag<String> metricEventOutputTag,
                                           OutputTag<TopicOffsetCommitEvent> topicOffsetCommitOutputTag) {
        // 保存服务端配置
        this.serverInfo = serverInfo;

        // 保存目标表写入上下文
        this.writeContext = writeContext;

        // 保存目标数据源配置
        this.targetDataSourceConfig = writeContext.getTargetDataSourceConfig();

        // 保存目标表名
        this.tableName = writeContext.getTargetTableName();

        // 保存日志侧输出标签
        this.syncLogEntryOutputTag = syncLogEntryOutputTag;

        // 保存监控侧输出标签
        this.metricEventOutputTag = metricEventOutputTag;

        // 保存 offset commit 侧输出标签
        this.topicOffsetCommitOutputTag = topicOffsetCommitOutputTag;

        // 保存删除策略
        this.tableRowDeleteStrategy = serverInfo.getTableRowDeleteStrategy();

        // 保存快照批量大小
        this.snapshotBatchSize = serverInfo.getSnapshotBatchSize();
    }

    // 初始化目标表 writer 运行期资源
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化查询策略
        this.selectStrategy = SelectStrategyFactory.getStrategy(targetDataSourceConfig);

        // 初始化插入策略
        this.insertStrategy = InsertStrategyFactory.getStrategy(targetDataSourceConfig);

        // 初始化删除策略
        this.deleteStrategy = DeleteStrategyFactory.getStrategy(targetDataSourceConfig, tableRowDeleteStrategy);

        // 初始化更新策略
        this.updateStrategy = UpdateStrategyFactory.getStrategy(targetDataSourceConfig);

        // 初始化 DDL 策略
        this.ddlStrategy = DDLStrategyFactory.getStrategy(targetDataSourceConfig);

        // 初始化批次大小管理器
        this.batchSizeManager = new BatchSizeManager(
                targetDataSourceConfig.getDataSourceType(),
                targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS ? -1 : snapshotBatchSize
        );

        // 初始化最后一次 flush 时间
        this.batchSizeManager.setLastFlushTime(System.currentTimeMillis());

        // 初始化每个 topic 的运行态
        initTopicRuntimeStates();


        // 初始化探针端 HTTP client，用于代表 topic 发起建表请求
        this.clientHTTPClient = new SyncClientHTTPClient(this.serverInfo);

        // 初始化 HTTP client 内部资源
        this.clientHTTPClient.init();

        // 只做一次目标表检查，表不存在时由代表 topic 发起建表
        ensureTableSchemaReady();

        // 读取目标表字段列表
        this.columnInfoList = this.ddlStrategy.getTableColumns(tableName);

        // 读取 Doris 表模型
        this.tableModel = this.targetDataSourceConfig.getTableModel(this.tableName);

        // 初始化实时增删改批量模式，仅在 writer 初始化时读取配置。
        // 未配置默认全部开启，显式空列表全部关闭，运行期间使用保存的结果。
        this.realtimeBatchEnabled = this.serverInfo != null
                && this.targetDataSourceConfig != null
                && this.serverInfo.isRealtimeBatchEnabled(
                this.targetDataSourceConfig.getDataSourceType());

        // 初始化 PIPELINE_SQL_RESULT 场景下的目标表 UNIQUE KEY
        initPipelineSqlResultTargetUniqueKeys();
    }

    private void initTopicRuntimeStates() {
        // 遍历当前目标表对应的所有 topic
        for (ServerTopicInfo topicInfo : writeContext.getTopicInfos()) {
            // 跳过空 topic 信息
            if (topicInfo == null || topicInfo.getTopic() == null) {
                continue;
            }

            // 为每个 topic 创建独立运行态
            topicRuntimeStateByTopic.put(topicInfo.getTopic().trim(), new TopicRuntimeState());
        }
    }

    // 处理单条 KafkaPayload
    @Override
    public void processElement(KafkaPayload row, Context ctx, Collector<OperationResult> out) throws Exception {
        // 构造操作结果对象
        OperationResult result = buildOperationResult(row);

        // 记录开始时间
        long startTime = System.currentTimeMillis();

        try {
            // 快照数据进入目标表级缓冲区
            if (row.isSnapshot()) {
                // 处理快照数据
                handleSnapshotData(row);
            } else {
                // 进入 实时数据 处理时，snapshotBuffer一定已经被清空了，因为最后一条快照的lastRecordInTable为true

                // 标记当前 topic 快照阶段结束
                markSnapshotPhaseEnded(row);

//                // 处理实时数据
//                processRealTimeData(row);

                // 处理实时数据。
                // 实时 r/c 如果启用批量写入，需要使用 ctx 注册 processing-time timer。
                processRealTimeData(row, ctx);
            }

            // 保持旧语义，内部失败也不抛出时结果视为完成
            result.setSuccess(true);
        } catch (Exception e) {
            // 记录异常日志
            logger.error("target table write failed, table={}, topic={}, offset={}, err={}",
                    tableName, row.getKafkaTopic(), row.getOffset(), e.getMessage(), e);

            // 设置失败结果
            result.setSuccess(false);

            // 设置错误信息
            result.setErrorMessage(e.getMessage());
        } finally {
            // 设置执行耗时,日志用
            result.setExecutionTimeMs(System.currentTimeMillis() - startTime);

            // 输出积压日志 to 侧输出流
            drainLogs(ctx);

            // 输出可提交 offset to 侧输出流
            emitOffsetCommitEvents(ctx);

            // 输出操作结果 to 侧输出流
            out.collect(result);
        }
    }

    // 构造操作结果
    private OperationResult buildOperationResult(KafkaPayload row) {
        // 创建操作结果
        OperationResult result = new OperationResult();

        // 设置 serverId
        result.setServerId(serverInfo.getServerId());

        // 设置来源业务数据源
        result.setDataFromSourceId(row.getBusinessDataSourceId());

        // 设置目标数据源
        result.setDateToSourceId(targetDataSourceConfig.getDataSourceId());

        // 设置目标表
        result.setTableName(tableName);

        // 设置操作类型
        result.setOperation(row.getOp());

        // 设置原始 payload
        result.setOriginalPayload(row);

        // 返回操作结果
        return result;
    }


    // 缓存快照数据，条件满足时候，持久化快照数据
    private synchronized void handleSnapshotData(KafkaPayload row) throws Exception {
        // 将当前快照数据加入目标表级缓冲区
        snapshotBuffer.add(row);

        // 达到 flush 条件时批量写入
        if (shouldFlushSnapshotBuffer(row)) {
            // flush 当前目标表快照缓冲区
            flushSnapshotBuffer();
        }
    }

    // 判断是否需要 flush 快照缓冲区
    private boolean shouldFlushSnapshotBuffer(KafkaPayload row) {
        // 获取当前 批量 阈值
        int currentBatchSize = batchSizeManager.getCurrentBatchSize();

        // 行数达到阈值时 flush
        if (snapshotBuffer.size() >= currentBatchSize) {
            return true;
        }

        // 超过最大等待时间时 flush
        if (isFlushTimeoutExceeded()) {
            return true;
        }

        // 当前 topic 的表快照结束时 flush
        return row.lastRecordInTable();
    }

    // 判断是否超过 flush 等待时间
    private boolean isFlushTimeoutExceeded() {
        // 获取当前时间
        long currentTime = System.currentTimeMillis();

        // 判断距离上次 flush 是否超过阈值
        return currentTime - batchSizeManager.getLastFlushTime() >= batchSizeManager.getFlushIntervalMs();
    }



    // flush 当前目标表快照缓冲区
    private synchronized void flushSnapshotBuffer() throws Exception {
        // 缓冲区为空时直接返回
        if (snapshotBuffer.isEmpty()) {
            return;
        }

        // 执行目标表级批量快照写入
        processBatchSnapshot(snapshotBuffer);

        // 清空快照缓冲区
        snapshotBuffer.clear();

        // 更新最后 flush 时间
        batchSizeManager.setLastFlushTime(System.currentTimeMillis());
    }
//
//    // 处理实时数据
//    private void processRealTimeData(KafkaPayload row) throws Exception {
//        // 获取操作类型
//        String op = row.getOp();
//
//        // 按操作类型分发
//        switch (op) {
//            case "r":
//            case "c":
//                // 执行插入
//                invokeInsert(row);
//                break;
//            case "u":
//                // 执行更新
//                invokeUpdate(row);
//                break;
//            case "d":
//                // 执行删除
//                invokeDelete(row);
//                break;
//            case "ddl":
//                // DDL 暂时保持原处理方式
//                invokeDDL(row);
//                break;
//            default:
//                // 未知操作类型只记录日志
//                logger.warn("unknown operation type skipped, op={}, table={}, topic={}",
//                        op, tableName, row.getKafkaTopic());
//                break;
//        }
//
//        // 保持旧语义，单条失败也推进 offset
//        advanceOffset(row);
//    }

    // 标记当前 topic 快照阶段结束
    private void markSnapshotPhaseEnded(KafkaPayload row) {
        // 获取当前 topic 运行态
        TopicRuntimeState state = getTopicRuntimeState(row.getKafkaTopic());

        // 没有运行态时直接返回
        if (state == null || state.isSnapshotPhaseEnded()) {
            return;
        }

        // 标记快照阶段结束
        state.setSnapshotPhaseEnded(true);
    }

    // 推进当前消息对应 topic 的 offset
    private void advanceOffset(KafkaPayload row) {
        // DDL 当前暂不携带 topic，无法推进 offset
        if (row.getKafkaTopic() == null || row.getKafkaTopic().trim().isEmpty()) {
            return;
        }

        // 获取当前 topic 运行态
        TopicRuntimeState state = getTopicRuntimeState(row.getKafkaTopic());

        // 未知 topic 不推进 offset
        if (state == null) {
            logger.warn("skip offset advance for unknown topic, topic={}, table={}",
                    row.getKafkaTopic(), tableName);
            return;
        }

        // 推进当前 topic 的最大可提交 offset
        state.advanceLatestOffsetCommitCandidate(row.getOffset());
    }

    // 批量推进快照 offset
    private void advanceOffsetsByBatch(List<BatchInsertItem> batchItems) {
        // 空批次直接返回
        if (batchItems == null || batchItems.isEmpty()) {
            return;
        }

        // 当前批次的数据，可能由多个 topic 混合而成
        // 按 topic 保存本批次最大 offset
        Map<String, Long> maxOffsetByTopic = new LinkedHashMap<>();

        // 遍历批次数据，每个topic，保留最大offset
        for (BatchInsertItem item : batchItems) {
            // 跳过空 item
            if (item == null || item.getPayload() == null) {
                continue;
            }

            // 获取 payload
            KafkaPayload payload = item.getPayload();

            // 跳过无 topic 或无 offset 的数据
            if (payload.getKafkaTopic() == null || payload.getOffset() == null) {
                continue;
            }

            // 记录当前 topic 的最大 offset
            maxOffsetByTopic.merge(payload.getKafkaTopic(), payload.getOffset(), Math::max);
        }

        // 按 topic 推进 offset
        for (Map.Entry<String, Long> entry : maxOffsetByTopic.entrySet()) {
            // 获取 topic 运行态
            TopicRuntimeState state = getTopicRuntimeState(entry.getKey());

            // 跳过未知 topic
            if (state == null) {
                continue;
            }

            // 推进当前 topic 的最大可提交 offset
            state.advanceLatestOffsetCommitCandidate(entry.getValue());
        }
    }

    // 输出可提交 offset 事件
    private void emitOffsetCommitEvents(Context ctx) {
        // 遍历每个 topic 的运行态
        for (Map.Entry<String, TopicRuntimeState> entry : topicRuntimeStateByTopic.entrySet()) {
            // 获取 topic
            String topic = entry.getKey();

            // 获取运行态
            TopicRuntimeState state = entry.getValue();

            // 没有新 offset 时跳过
            if (!state.hasNewOffsetToReport()) {
                continue;
            }

            // 构造 offset commit 事件
            TopicOffsetCommitEvent event = TopicOffsetCommitEvent.of(
                    serverInfo.getServerId(),
                    topic,
                    state.getNextOffsetToCommit()
            );

            // 输出到 offset side output
            ctx.output(topicOffsetCommitOutputTag, event);

            // 标记当前 offset 已输出
            state.markOffsetReported();
        }
    }

    // 获取 topic 运行态
    private TopicRuntimeState getTopicRuntimeState(String topic) {
        // 空 topic 直接返回 null
        if (topic == null || topic.trim().isEmpty()) {
            return null;
        }

        // 返回对应 topic 运行态
        return topicRuntimeStateByTopic.get(topic.trim());
    }

    // 输出积压日志
    private void drainLogs(Context ctx) throws Exception {
        // 没有日志时直接返回
        if (pendingLogEntries.isEmpty()) {
            return;
        }

        // 遍历日志列表
        for (SyncLogEntry logEntry : pendingLogEntries) {
            // 输出日志到侧输出
            ctx.output(syncLogEntryOutputTag, mapper.writeValueAsString(logEntry));
        }

        // 清空日志缓冲
        pendingLogEntries.clear();
    }

    // 循环检查目标表是否就绪，表不存在时由代表 topic 发起建表请求
    private void ensureTableSchemaReady() {
        // 获取代表 topic
        ServerTopicInfo representativeTopicInfo = writeContext.getRepresentativeTopicInfo();

        // 没有代表 topic 时直接失败
        if (representativeTopicInfo == null) {
            throw new IllegalStateException("no representative topic found, targetTable=" + tableName);
        }

        // 持续检查目标表是否存在
        while (true) {
            try {
                // 检查目标表是否存在
                boolean exists = this.selectStrategy.checkTableExists(this.tableName);

                // 表已存在则结束等待
                if (exists) {
                    logger.info("target table exists, table={}, topics={}",
                            tableName, writeContext.getTopicInfoByTopic().keySet());
                    return;
                }

                // 表不存在时发送建表请求
                requestCreateTableFromRepresentativeTopic(representativeTopicInfo);

                // 记录等待日志
                logger.info("target table not exists, creation requested, wait {} seconds before retry, table={}, representativeTopic={}",
                        TimeUnit.MILLISECONDS.toSeconds(TABLE_CHECK_INTERVAL_MS),
                        tableName,
                        representativeTopicInfo.getTopic());

                // 固定时间后再次检查
                TimeUnit.MILLISECONDS.sleep(TABLE_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                // 恢复中断标记
                Thread.currentThread().interrupt();

                // 中断时退出等待
                throw new IllegalStateException("interrupted while waiting target table ready, table=" + tableName, e);
            } catch (Exception e) {
                // 记录异常并继续等待
                logger.warn("target table ready check failed, will retry, table={}, representativeTopic={}, err={}",
                        tableName,
                        representativeTopicInfo.getTopic(),
                        e.getMessage());

                // 异常后等待固定时间再重试
                sleepBeforeRetry();
            }
        }
    }

    // 使用代表 topic 向探针端发起建表请求
    private void requestCreateTableFromRepresentativeTopic(ServerTopicInfo representativeTopicInfo) throws Exception {
        // 构造建表请求
        TableDDLRequest request = new TableDDLRequest();

        // 设置 client 信息
        request.setClientInfo(representativeTopicInfo.getClientInfo());

        // 设置源 schema
        request.setSchemaName(representativeTopicInfo.getSchemaName());

        // 设置源表名
        request.setTableName(representativeTopicInfo.getTableName());


        // 设置业务库数据源 ID
        request.setBusinessDataSourceId(representativeTopicInfo.getBusinessDataSourceId());

        // 设置 目标数据源 ID
        request.setTargetDataSourceId(targetDataSourceConfig.getDataSourceId());

        // 向代表 topic 所属探针端发起建表请求
        this.clientHTTPClient.requestTableDdlExport(request);

        // 记录建表请求日志
        logger.info("建表请求已经发送, targetTable={}, representativeTopic={}, source={}.{}",
                tableName,
                representativeTopicInfo.getTopic(),
                representativeTopicInfo.getSchemaName(),
                representativeTopicInfo.getTableName());
    }

    // 异常后等待固定时间再重试
    private void sleepBeforeRetry() {
        try {
            // 固定时间后重试
            TimeUnit.MILLISECONDS.sleep(TABLE_CHECK_INTERVAL_MS);
        } catch (InterruptedException e) {
            // 恢复中断标记
            Thread.currentThread().interrupt();

            // 中断时抛出运行时异常
            throw new IllegalStateException("interrupted while waiting target table ready, table=" + tableName, e);
        }
    }


    // 生成字段结构签名
    private String generateFieldSignature(Map<String, Object> afterData) {
        // 按字段名排序后拼接，保证相同字段集合签名一致
        return afterData.keySet().stream()
                .sorted()
                .collect(Collectors.joining(","));
    }

    // 判断是否需要字段补齐
    private boolean shouldPadFields(int groupCount) {
        // 目标表字段为空时不补齐
        if (this.columnInfoList == null || this.columnInfoList.isEmpty()) {
            return false;
        }

        // 获取目标表字段数量
        int columnCount = this.columnInfoList.size();

        // 字段数或分组数未达到阈值时不补齐
        if (columnCount < WIDE_TABLE_COLUMN_THRESHOLD || groupCount < PADDING_GROUP_THRESHOLD) {
            return false;
        }

        // 记录触发字段补齐
        logger.info("trigger field padding, table={}, columnCount={}, groupCount={}",
                tableName, columnCount, groupCount);

        // 返回需要补齐
        return true;
    }


//    // 执行现有新增批次，逐条记录结果，并在处理完成后推进 offset。
//// 策略异常按整批失败记录，不自动重试，不阻断后续操作入队。
//    private void invokeBatchInsert(List<BatchInsertItem> batchItems,
//                                   boolean needPadding) throws Exception {
//        // 空批次无需处理。
//        if (batchItems == null || batchItems.isEmpty()) {
//            return;
//        }
//
//        // 保存策略返回的逐条状态，或本次批量调用的失败详情。
//        Map<Long, String> statusMap = null;
//        String batchFailureDetail = null;
//
//        // 仅捕获策略执行异常，日志和 offset 处理放在调用结束之后。
//        try {
//            // 沿用现有新增策略、字段补齐及批量大小管理。
//            statusMap = insertStrategy.executeBatchInsert(
//                    tableName,
//                    batchItems,
//                    batchSizeManager,
//                    columnInfoList,
//                    needPadding);
//
//            // 缺少结果时不能确认批次成功。
//            if (statusMap == null) {
//                throw new IllegalStateException("批量新增策略未返回执行结果");
//            }
//        } catch (Exception exception) {
//            // 执行失败或结果无法确认时，当前调用中的全部消息记录失败。
//            batchFailureDetail = "批量新增失败或结果未确认: "
//                    + ExceptionDetailUtil.buildRootCauseDetail(exception, 2000);
//
//            // 输出批次级摘要，不展开业务数据。
//            logger.warn(
//                    "batch insert failed, table={}, rows={}, err={}",
//                    tableName, batchItems.size(), batchFailureDetail);
//        }
//
//        // 为批次中的每条原始消息生成同步日志。
//        for (BatchInsertItem item : batchItems) {
//            KafkaPayload payload = item.getPayload();
//
//            // 策略抛异常时，不再采用可能不完整的逐条状态。
//            boolean success;
//            String logDetail;
//            if (batchFailureDetail != null) {
//                success = false;
//                logDetail = batchFailureDetail;
//            } else {
//                // 策略正常返回时，沿用现有状态约定。
//                String status = statusMap.get(payload.getOffset());
//                if (status == null) {
//                    status = "UNKNOWN_ERROR";
//                }
//
//                // 只有明确的 SUCCESS 才记录为成功。
//                success = "SUCCESS".equals(status);
//                logDetail = getBatchLogDetail(success, status);
//            }
//
//            // 成功或失败均记录，失败原始消息按现有日志策略保存。
//            pendingLogEntries.add(buildSyncLogEntry(payload, success, logDetail));
//        }
//
//        // 当前批次已记录处理结果，按 topic 推进 offset。
//        // 调用方随后正常清空缓冲区，避免异常批次被再次提交。
//        advanceOffsetsByBatch(batchItems);
//    }


    // 执行批量新增，按输入位置关联结果，支持不同 topic 的相同 offset。
// 策略异常记录为整批失败，处理完成后推进 offset，不自动重试。
    private void invokeBatchInsert(List<BatchInsertItem> batchItems,
                                   boolean needPadding) throws Exception {
        // 空批次无需处理。
        if (batchItems == null || batchItems.isEmpty()) {
            return;
        }

        // 保存按输入位置排列的结果，或整批失败详情。
        List<String> statuses = null;
        String batchFailureDetail = null;

        // 捕获策略执行及结果校验异常，统一交给后续日志处理。
        try {
            // 调用新接口，结果第 i 项对应输入第 i 条消息。
            statuses = insertStrategy.executeBatchInsertOrdered(
                    tableName,
                    batchItems,
                    batchSizeManager,
                    columnInfoList,
                    needPadding);

            // 数量不匹配时无法可靠关联结果，整批按结果未确认处理。
            if (statuses == null || statuses.size() != batchItems.size()) {
                throw new IllegalStateException(
                        "批量新增结果数量与输入数量不一致，inputCount="
                                + batchItems.size()
                                + "，resultCount="
                                + (statuses == null ? "null" : statuses.size()));
            }
        } catch (Exception exception) {
            // 数据库失败或结果无法确认时，不采用可能不完整的状态列表。
            batchFailureDetail = "批量新增失败或结果未确认: "
                    + ExceptionDetailUtil.buildRootCauseDetail(exception, 2000);

            // 记录批次级摘要，不展开业务数据。
            logger.warn(
                    "batch insert failed, table={}, rows={}, err={}",
                    tableName, batchItems.size(), batchFailureDetail);
        }

        // 按原输入位置逐条生成日志，不使用 offset 查找执行结果。
        for (int itemIndex = 0; itemIndex < batchItems.size(); itemIndex++) {
            KafkaPayload payload = batchItems.get(itemIndex).getPayload();

            // 整批异常时，每条消息都记录同一批次失败原因。
            boolean success;
            String logDetail;
            if (batchFailureDetail != null) {
                success = false;
                logDetail = batchFailureDetail;
            } else {
                // 读取当前输入位置对应的结果。
                String status = statuses.get(itemIndex);

                // 缺少单条状态时，当前消息明确记录失败。
                if (status == null || status.trim().isEmpty()) {
                    success = false;
                    logDetail = "批量新增缺少单条执行结果，输入索引=" + itemIndex;
                } else {
                    // 只有明确的 SUCCESS 才记录成功。
                    success = "SUCCESS".equals(status);
                    logDetail = getBatchLogDetail(success, status);
                }
            }

            // 使用原始消息生成日志，保留各自的 topic 来源及 offset 信息。
            pendingLogEntries.add(buildSyncLogEntry(payload, success, logDetail));
        }

        // 所有消息已记录处理结果，按各自 topic 推进 offset。
        // 调用方随后清空缓冲区，失败批次不会自动重新提交。
        advanceOffsetsByBatch(batchItems);
    }
//    // 执行批量插入
//    private void invokeBatchInsert(List<BatchInsertItem> batchItems,
//                                   boolean needPadding) throws Exception {
//        // 空批次直接返回
//        if (batchItems == null || batchItems.isEmpty()) {
//            return;
//        }
//
//        // 调用现有插入策略执行批量写入，statusMap用于记录每条消息的插入结果
//        Map<Long, String> statusMap = this.insertStrategy.executeBatchInsert(
//                this.tableName,
//                batchItems,
//                this.batchSizeManager,
//                this.columnInfoList,
//                needPadding
//        );
//
//        // 为每条数据生成日志
//        for (BatchInsertItem item : batchItems) {
//            // 获取 payload
//            KafkaPayload payload = item.getPayload();
//
//            // 获取 offset
//            Long offset = payload.getOffset();
//
//            // 获取写入状态
//            String status = statusMap.getOrDefault(offset, "UNKNOWN_ERROR");
//
//            // 判断是否成功
//            boolean success = "SUCCESS".equals(status);
//
//            // 构造日志详情
//            String logDetail = getBatchLogDetail(success, status);
//
//            // 添加日志
//            pendingLogEntries.add(buildSyncLogEntry(payload, success, logDetail));
//        }
//
//        // 保持旧语义，批次执行结束后按 topic 推进 offset
//        advanceOffsetsByBatch(batchItems);
//    }


//    // 处理实时消息，操作切换时先处理全部非当前操作的缓冲数据。
//    // 新增、更新、删除分别进入各自处理入口，共用实时刷新定时器。
//    private void processRealTimeData(KafkaPayload row, Context ctx) throws Exception {
//        // 获取当前消息的操作类型。
//        String op = row.getOp();
//
//        // 在接收当前消息前，刷新全部非当前操作的缓冲区。
//        // r/c 视为同一种新增操作；DDL 和未知操作会刷新全部缓冲区。
//        // 刷新完成后，由该方法统一维护共享定时器。
//        flushNonCurrentRealtimeBuffers(op, ctx);
//
//        // 按操作类型分别处理，保持消息到达顺序。
//        switch (op) {
//            case "r":
//            case "c":
//                // 沿用现有配置，启用批量新增时进入新增缓冲区。
//                if (isRealtimeInsertBatchEnabled()) {
//                    handleRealtimeInsert(row, ctx);
//                } else {
//                    // 未启用批量新增时，执行单条新增。
//                    invokeInsert(row);
//
//                    // 单条处理完成后推进 offset，包括已经记录的失败。
//                    advanceOffset(row);
//                }
//                break;
//
//            case "u":
//                // 更新进入独立缓冲区，固定条数阈值为 500。
//                // Doris 遇到重复 cdc_rowid 时，先执行已有批次再缓存当前消息。
//                // 缓存时不推进 offset，由实际批次处理完成后统一推进。
//                handleRealtimeUpdate(row, ctx);
//                break;
//
//            case "d":
//                // 删除进入独立缓冲区，固定条数阈值为 500。
//                // Doris 遇到重复 cdc_rowid 时，先执行已有批次再缓存当前消息。
//                // 缓存时不推进 offset，由实际批次处理完成后统一推进。
//                handleRealtimeDelete(row, ctx);
//                break;
//
//            case "ddl":
//                // 前序实时缓冲已刷新，调用现有 DDL 入口。
//                // 当前 invokeDDL 仍只记录暂未处理，不代表执行了目标 DDL。
//                invokeDDL(row);
//
//                // 沿用现有 DDL 消息的 offset 推进规则。
//                advanceOffset(row);
//                break;
//
//            default:
//                // 前序实时缓冲已刷新，未知操作仅记录日志。
//                logger.warn(
//                        "unknown operation type skipped, op={}, table={}, topic={}",
//                        op, tableName, row.getKafkaTopic());
//
//                // 未知操作跳过后推进 offset。
//                advanceOffset(row);
//                break;
//        }
//    }

    // 使用初始化时确定的模式处理实时新增、更新和删除。
    // 开启时缓存后批量执行，关闭时通过相同入口立即执行单条批次。
    private void processRealTimeData(KafkaPayload row, Context ctx) throws Exception {
        // 获取当前消息操作类型。
        String op = row.getOp();

        // 切换操作时先处理全部非当前操作的缓冲数据。
        // 运行期间模式固定，无需处理批量开关动态切换。
        flushNonCurrentRealtimeBuffers(op, ctx);

        // 分别处理各类操作，保持消息到达顺序。
        switch (op) {
            case "r":
            case "c":
                if (realtimeBatchEnabled) {
                    // 批量新增沿用现有阈值和共享定时器。
                    handleRealtimeInsert(row, ctx);
                } else {
                    // 单条模式直接入队并立即刷新，不注册定时器。
                    realtimeInsertBuffer.add(row);

                    // 复用新增数据构造、执行、日志及 offset 推进。
                    flushRealtimeInsertBufferIfNecessary();
                }
                break;

            case "u":
                if (realtimeBatchEnabled) {
                    // 批量更新使用 500 条阈值及 Doris 重复 key 拆批规则。
                    handleRealtimeUpdate(row, ctx);
                } else {
                    // 单条更新也使用批量接口，当前批次仅包含一条消息。
                    realtimeUpdateBuffer.add(row);

                    // 复用字段补齐、存在性检查和结果处理。
                    flushRealtimeUpdateBufferIfNecessary();
                }
                break;

            case "d":
                if (realtimeBatchEnabled) {
                    // 批量删除使用 500 条阈值及 Doris 重复 key 拆批规则。
                    handleRealtimeDelete(row, ctx);
                } else {
                    // 单条删除直接入队并立即执行。
                    realtimeDeleteBuffer.add(row);

                    // 复用表模型判断、删除方式选择和结果处理。
                    flushRealtimeDeleteBufferIfNecessary();
                }
                break;

            case "ddl":
                // 前序实时缓冲已刷新，调用现有 DDL 入口。
                // 当前入口只记录暂未处理，不代表已执行目标 DDL。
                invokeDDL(row);

                // 沿用 DDL 消息的 offset 推进规则。
                advanceOffset(row);
                break;

            default:
                // 未知操作只记录日志，前序实时缓冲已经处理。
                logger.warn(
                        "unknown operation type skipped, op={}, table={}, topic={}",
                        op, tableName, row.getKafkaTopic());

                // 跳过当前消息后推进 offset。
                advanceOffset(row);
                break;
        }
    }

    // 刷新实时更新缓冲区，批次成功、失败或明确跳过后均推进 offset。
    // 数据库执行异常记录为批次失败，不重试，继续处理后续消息。
    private void flushRealtimeUpdateBufferIfNecessary() throws Exception {
        // 空缓冲区无需处理。
        if (realtimeUpdateBuffer.isEmpty()) {
            return;
        }

        // 保存当前批次，后续逐条记录结果时使用原始消息身份。
        List<KafkaPayload> rows = new ArrayList<>(realtimeUpdateBuffer);

        // 默认将正常返回的批次标记为成功。
        boolean success = true;
        String logDetail = "批量更新成功";

        // 执行批次，数据库或数据校验异常统一转为失败结果。
        try {
            // Doris DUPLICATE 表按业务约定跳过更新。
            if (shouldSkipMutationForDorisDuplicate()) {
                // 跳过属于已处理消息，不访问目标数据库。
                logDetail = "Doris DUPLICATE 表跳过 UPDATE，未执行实际写库";
                logger.info("Doris DUPLICATE 表跳过 UPDATE，未执行实际写库，table={}, rows={}",
                        tableName, rows.size());
            } else {
                // 所有目标库均要求真正的批量实现，不模拟逐条更新。
                if (!updateStrategy.supportsBatch()) {
                    throw new UnsupportedOperationException(
                            "当前更新策略尚未实现批量更新: "
                                    + updateStrategy.getClass().getName());
                }

                // 在调用数据库前统一检查当前批次。
                for (KafkaPayload row : rows) {
                    // 更新缓冲区只能包含更新消息。
                    if (!"u".equals(row.getOp())) {
                        throw new IllegalArgumentException(
                                "更新缓冲区包含非更新消息，op=" + row.getOp());
                    }

                    // 本轮更新统一使用 cdc_rowid 定位目标记录。
                    // 每条消息使用独立可修改集合，避免相互影响。
                    LinkedHashSet<String> primaryKeys = new LinkedHashSet<>();
                    primaryKeys.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
                    row.setPrimaryKeySet(primaryKeys);

                    // 更新从 after 中读取主键，缺失时明确失败。
                    validatePrimaryKeyValuesPresentForMutation(row);
                }

                // 传递完整消息及目标字段元数据。
                // 日期单位、字段过滤、补齐和参数绑定由具体更新策略负责。
                updateStrategy.executeBatchUpdate(tableName, rows, columnInfoList);
            }
        } catch (Exception exception) {
            // 批次执行失败或结果无法确认时，整批记录为失败。
            success = false;
            logDetail = "批量更新失败或结果未确认: "
                    + ExceptionDetailUtil.buildRootCauseDetail(exception, 2000);

            // 输出批次级错误摘要，不在日志中展开整批业务数据。
            logger.warn(
                    "batch update failed, table={}, rows={}, err={}",
                    tableName, rows.size(), logDetail);
        }

        // 按原始消息分别记录结果，避免不同 topic 的相同 offset 相互覆盖。
        for (KafkaPayload row : rows) {
            // 成功、失败及明确跳过均生成同步日志。
            pendingLogEntries.add(buildSyncLogEntry(row, success, logDetail));

            // 已记录处理结果后推进该消息所属 topic 的 offset。
            advanceOffset(row);
        }

        // 当前批次已处理完成，清空缓冲区，避免再次提交。
        realtimeUpdateBuffer.clear();
        logger.info("实时更新批次处理完成，table={}, rows={}, success={}, detail={}",
                tableName, rows.size(), success, logDetail);
    }


    // 在处理当前消息前，刷新全部非当前操作的实时缓冲区。
    // r/c 共用新增缓冲区，其他操作分别使用独立缓冲区。
    // DDL 和未知操作不对应任何缓冲区，因此会刷新全部实时缓冲区。
    private void flushNonCurrentRealtimeBuffers(String currentOp, Context ctx)
            throws Exception {
        // r/c 均属于新增，切换为其他操作时刷新新增缓冲区。
        boolean isInsert = "r".equals(currentOp) || "c".equals(currentOp);
        if (!isInsert) {
            flushRealtimeInsertBufferIfNecessary();
        }

        // 当前不是更新时，先处理此前缓存的更新消息。
        if (!"u".equals(currentOp)) {
            flushRealtimeUpdateBufferIfNecessary();
        }

        // 当前不是删除时，先处理此前缓存的删除消息。
        if (!"d".equals(currentOp)) {
            flushRealtimeDeleteBufferIfNecessary();
        }

        // 全部缓冲区为空时，取消此前注册的实时刷新定时器。
        // 当前消息随后入队时，由对应处理入口重新注册定时器。
        // 当前操作仍有缓存时保留原定时器，避免持续来消息导致刷新时间后移。
        if (realtimeInsertBuffer.isEmpty()
                && realtimeUpdateBuffer.isEmpty()
                && realtimeDeleteBuffer.isEmpty()) {
            deleteRealtimeInsertFlushTimer(ctx);
        }
    }



    // 接收实时更新消息，达到固定阈值或遇到 Doris 重复主键时刷新。
// 消息入队时不推进 offset，实际处理结果由批次刷新方法统一记录。
    private void handleRealtimeUpdate(KafkaPayload row, Context ctx) throws Exception {
        // Doris UNIQUE 表需要保证一次 HTTP 更新批次中没有重复主键。
        // DUPLICATE 表的跳过处理由批次刷新方法负责。
        if (isDorisTarget() && !shouldSkipMutationForDorisDuplicate()) {
            // 按字段名忽略大小写读取主键，主键值本身保持原样。
            Object currentRowId = findValueIgnoreCase(row.getAfterData(), "cdc_rowid");

            // 缺失或非法主键交给批次处理阶段校验并记录失败。
            // 此处只对有效字符串主键判断是否需要拆批。
            if (currentRowId instanceof String
                    && !((String) currentRowId).trim().isEmpty()) {
                // 按到达顺序检查已有缓冲消息。
                for (KafkaPayload bufferedRow : realtimeUpdateBuffer) {
                    // 获取已有消息的主键，不转换大小写或去除主键中的空格。
                    Object bufferedRowId = findValueIgnoreCase(
                            bufferedRow.getAfterData(), "cdc_rowid");

                    // 重复主键出现前先处理此前所有更新，当前消息留给下一批。
                    if (currentRowId.equals(bufferedRowId)) {
                        // 刷新会清空缓冲区，因此刷新后立即退出遍历。
                        flushRealtimeUpdateBufferIfNecessary();

                        // 旧批次已处理完成，取消其定时器。
                        deleteRealtimeInsertFlushTimer(ctx);
                        break;
                    }
                }
            }
        }

        // 将当前消息加入更新缓冲区，保持原始到达顺序。
        realtimeUpdateBuffer.add(row);

        // 复用现有定时器注册入口，已有定时器时不会重复注册或推迟时间。
        registerRealtimeInsertFlushTimer(ctx);

        // Oracle、MySQL、Doris 的更新均使用固定 500 条阈值。
        if (realtimeUpdateBuffer.size() >= REALTIME_MUTATION_BATCH_SIZE) {
            // 执行当前更新批次，并由刷新方法处理日志、offset 和清空缓冲区。
            flushRealtimeUpdateBufferIfNecessary();

            // 当前批次已经处理完成，取消尚未触发的定时器。
            deleteRealtimeInsertFlushTimer(ctx);
        }
    }

    // 接收实时删除消息，达到固定阈值或遇到 Doris 重复主键时刷新。
// 消息入队时不推进 offset，实际处理结果由批次刷新方法统一记录。
    private void handleRealtimeDelete(KafkaPayload row, Context ctx) throws Exception {
        // Doris UNIQUE 表的一次 HTTP 删除批次中不包含重复主键。
        // DUPLICATE 表的跳过处理由批次刷新方法负责。
        if (isDorisTarget() && !shouldSkipMutationForDorisDuplicate()) {
            // 删除从 before 中读取主键，字段名忽略大小写。
            Object currentRowId = findValueIgnoreCase(
                    row.getBeforeData(), "cdc_rowid");

            // 缺失或非法主键交给批次处理阶段校验并记录失败。
            // 有效主键保持原值，不转换大小写或去除空格。
            if (currentRowId instanceof String
                    && !((String) currentRowId).trim().isEmpty()) {
                // 按消息到达顺序检查已有删除缓冲。
                for (KafkaPayload bufferedRow : realtimeDeleteBuffer) {
                    // 获取已缓存删除消息的主键。
                    Object bufferedRowId = findValueIgnoreCase(
                            bufferedRow.getBeforeData(), "cdc_rowid");

                    // 重复主键出现时，先处理此前批次。
                    if (currentRowId.equals(bufferedRowId)) {
                        // 刷新会清空缓冲区，完成后立即退出遍历。
                        flushRealtimeDeleteBufferIfNecessary();

                        // 旧批次已处理完成，取消其定时器。
                        deleteRealtimeInsertFlushTimer(ctx);
                        break;
                    }
                }
            }
        }

        // 当前删除消息加入缓冲区，保持到达顺序。
        realtimeDeleteBuffer.add(row);

        // 复用共享定时器，已有定时器时不重复注册或推迟触发时间。
        registerRealtimeInsertFlushTimer(ctx);

        // Oracle、MySQL、Doris 的删除均使用固定 500 条阈值。
        if (realtimeDeleteBuffer.size() >= REALTIME_MUTATION_BATCH_SIZE) {
            // 执行删除批次，由刷新方法记录结果、推进 offset 并清空缓冲区。
            flushRealtimeDeleteBufferIfNecessary();

            // 当前批次已处理完成，取消尚未触发的定时器。
            deleteRealtimeInsertFlushTimer(ctx);
        }
    }





    // 刷新实时删除缓冲区，批次成功、失败或明确跳过后均推进 offset。
// 按原始消息记录结果，不使用仅以 offset 为键的结果映射。
    private void flushRealtimeDeleteBufferIfNecessary() throws Exception {
        // 空缓冲区无需处理。
        if (realtimeDeleteBuffer.isEmpty()) {
            return;
        }

        // 保存当前批次，保持消息顺序和所属 topic 信息。
        List<KafkaPayload> rows = new ArrayList<>(realtimeDeleteBuffer);

        // 默认将正常返回的批次标记为成功。
        boolean success = true;
        String logDetail = "批量删除成功";

        // 执行批次，数据库或数据校验异常统一转为失败结果。
        try {
            // Doris DUPLICATE 表按业务约定跳过删除。
            if (shouldSkipMutationForDorisDuplicate()) {
                // 跳过属于已处理消息，不访问目标数据库。
                logDetail = "Doris DUPLICATE 表跳过 DELETE，未执行实际写库";
                logger.info("Doris DUPLICATE 表跳过 DELETE，未执行实际写库，table={}, rows={}",
                        tableName, rows.size());
            } else {
                // 在调用数据库前统一检查当前批次。
                for (KafkaPayload row : rows) {
                    // 删除缓冲区只能包含删除消息。
                    if (!"d".equals(row.getOp())) {
                        throw new IllegalArgumentException(
                                "删除缓冲区包含非删除消息，op=" + row.getOp());
                    }

                    // 本轮删除统一使用 cdc_rowid 定位目标记录。
                    // 每条消息使用独立可修改集合。
                    LinkedHashSet<String> primaryKeys = new LinkedHashSet<>();
                    primaryKeys.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
                    row.setPrimaryKeySet(primaryKeys);

                    // 删除从 before 中读取主键，缺失时明确失败。
                    validatePrimaryKeyValuesPresentForMutation(row);
                }

                // 沿用现有业务规则：PIPELINE_SQL_RESULT 的 Doris 删除强制物理删除。
                // 其他场景由策略按配置选择物理删除或逻辑删除。
                boolean forcePhysicalDelete =
                        shouldForcePhysicalDeleteForPipelineSqlResult();

                // 调用待补充的批量入口，整批确认成功才允许正常返回。
                // 删除不转换源日期字段，也不要求源日期单位映射。
                deleteStrategy.executeBatchDelete(
                        tableName, rows, forcePhysicalDelete);
            }
        } catch (Exception exception) {
            // 批次失败或结果无法确认时，整批记录为失败。
            success = false;
            logDetail = "批量删除失败或结果未确认: "
                    + ExceptionDetailUtil.buildRootCauseDetail(exception, 2000);

            // 输出批次级错误摘要，不展开整批业务数据。
            logger.warn(
                    "batch delete failed, table={}, rows={}, err={}",
                    tableName, rows.size(), logDetail);
        }

        // 直接遍历原始消息，跨 topic 的相同 offset 不会互相覆盖。
        for (KafkaPayload row : rows) {
            // 成功、失败及明确跳过均生成同步日志。
            pendingLogEntries.add(buildSyncLogEntry(row, success, logDetail));

            // 已记录处理结果后推进该消息所属 topic 的 offset。
            advanceOffset(row);
        }

        // 当前批次已处理完成，清空缓冲区，避免再次提交。
        realtimeDeleteBuffer.clear();

        logger.info("实时删除批次处理完成，table={}, rows={}, success={}, detail={}",
                tableName, rows.size(), success, logDetail);
    }















    // 注册实时新增 flush timer。
    // 每个 writer 同一时间最多存在一个 active timer。
    private void registerRealtimeInsertFlushTimer(Context ctx) {
        // 已经存在 active timer 时不重复注册。
        if (realtimeInsertFlushTimerTs > 0L) {
            return;
        }

        // 计算 timer 到期时间。
        long timerTs = ctx.timerService().currentProcessingTime() + batchSizeManager.getRealtimeInsertFlushIntervalMs();

        // 注册 processing-time timer。
        ctx.timerService().registerProcessingTimeTimer(timerTs);

        // 保存 active timer 时间戳。
        realtimeInsertFlushTimerTs = timerTs;
    }

    // 删除实时新增 flush timer。
    // 主动 flush 时调用；timer 自己触发时不需要删除。
    private void deleteRealtimeInsertFlushTimer(Context ctx) {
        // 没有 active timer 时直接返回。
        if (realtimeInsertFlushTimerTs <= 0L) {
            return;
        }

        // 删除还未触发的 processing-time timer。
        ctx.timerService().deleteProcessingTimeTimer(realtimeInsertFlushTimerTs);

        // 清空 active timer 标记。
        realtimeInsertFlushTimerTs = -1L;
    }

















    // 执行单条插入
    private void invokeInsert(KafkaPayload row) {
        // 默认成功
        boolean success = true;

        // 默认日志详情
        String logDetail = "插入成功";

        // 执行插入
        try {
            // 调用插入策略
            this.insertStrategy.executeSingleInsert(this.tableName, row, this.columnInfoList);
        } catch (Exception e) {
            // 标记失败
            success = false;

            // 记录安全异常摘要
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);

            // 记录错误日志
            logger.warn("insert failed, table={}, topic={}, offset={}, err={}",
                    tableName, row.getKafkaTopic(), row.getOffset(), e.getMessage());
        } finally {
            // 记录同步日志
            pendingLogEntries.add(buildSyncLogEntry(row, success, logDetail));
        }
    }

    // 执行单条更新
    private void invokeUpdate(KafkaPayload row) {
        // 默认成功
        boolean success = true;

        // 默认日志详情
        String logDetail = "更新成功";

        // 执行更新
        try {
            // Doris DUPLICATE 表跳过 update
            if (shouldSkipMutationForDorisDuplicate()) {
                // 设置跳过日志
                logDetail = "Doris DUPLICATE 表跳过 UPDATE，未执行实际写库";

                // 结束更新
                return;
            }

            // 规约主键字段
            normalizePrimaryKeysForWrite(row);

            // 校验主键值存在
            validatePrimaryKeyValuesPresentForMutation(row);

            // 调用更新策略
            this.updateStrategy.executeUpdate(tableName, row);
        } catch (Exception e) {
            // 标记失败
            success = false;

            // 记录安全异常摘要
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);

            // 记录错误日志
            logger.warn("update failed, table={}, topic={}, offset={}, err={}",
                    tableName, row.getKafkaTopic(), row.getOffset(), e.getMessage());
        } finally {
            // 记录同步日志
            pendingLogEntries.add(buildSyncLogEntry(row, success, logDetail));
        }
    }

    // 执行单条删除
    private void invokeDelete(KafkaPayload row) {
        // 默认成功
        boolean success = true;

        // 默认日志详情
        String logDetail = "删除成功";

        // 执行删除
        try {
            // Doris DUPLICATE 表跳过 delete
            if (shouldSkipMutationForDorisDuplicate()) {
                // 设置跳过日志
                logDetail = "Doris DUPLICATE 表跳过 DELETE，未执行实际写库";

                // 结束删除
                return;
            }

            // 规约主键字段
            normalizePrimaryKeysForWrite(row);

            // 校验主键值存在
            validatePrimaryKeyValuesPresentForMutation(row);

            // PIPELINE_SQL_RESULT + Doris 强制使用物理删除
            if (shouldForcePhysicalDeleteForPipelineSqlResult()) {
                // 执行物理删除
                this.deleteStrategy.executePhysicalDelete(tableName, row);
            } else {
                // 执行默认删除策略
                this.deleteStrategy.executeDelete(tableName, row);
            }
        } catch (Exception e) {
            // 标记失败
            success = false;

            // 记录安全异常摘要
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);

            // 记录错误日志
            logger.warn("delete failed, table={}, topic={}, offset={}, err={}",
                    tableName, row.getKafkaTopic(), row.getOffset(), e.getMessage());
        } finally {
            // 记录同步日志
            pendingLogEntries.add(buildSyncLogEntry(row, success, logDetail));
        }
    }

    // 执行 DDL，当前目标表级 writer 暂不处理 DDL
    private void invokeDDL(KafkaPayload row) {
        // 记录跳过 DDL
        logger.info("skip ddl in target table writer, table={}, ddl={}",
                tableName, row.getDdlStatement());

        // 添加同步日志
        pendingLogEntries.add(buildSyncLogEntry(row, true, "DDL 暂未处理"));
    }

    // 构造同步日志
    private SyncLogEntry buildSyncLogEntry(KafkaPayload payload,
                                           boolean success,
                                           String logDetail) {
        // 使用 builder 构造日志
        return SyncLogEntry.builder()
                .setInstructionType(payload.getOp())
                .setSourceTime(payload.getDmlTime())
                .setTargetTime(LocalDateTime.now().format(com.bsoft.common.util.DateTimeConstants.FMT_DATETIME))
                .setLogDetail(logDetail)
                .setSourceDataSourceId(payload.getBusinessDataSourceId())
                .setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId())
                .setSourceTableName(payload.getTableName())
                .setTargetTableName(this.tableName)
                .setResultCode(success ? 0 : 1)
                .setOffset(payload.getOffset())
                .setKafkaEnqueueTime(payload.getKafkaEnqueueTime())
                .setRawMessage(shouldStoreRawMessage(success) ? payload.getRawMessage() : null)
                .setSourceSchemaName(payload.getSchemaName())
                .build();
    }

    // 判断是否需要保存原始消息
    private boolean shouldStoreRawMessage(boolean success) {
        // 失败时始终保存原始消息
        if (!success) {
            return true;
        }

        // 非 NONE 策略时保存原始消息
        return this.serverInfo.getRawMessageStorageStrategy() != RawMessageStorageStrategy.NONE;
    }

    // 获取批量写入日志详情
    private String getBatchLogDetail(boolean success, String status) {
        // 成功时返回成功信息
        if (success) {
            return "批量插入成功";
        }

        // 参数绑定失败
        if (status.startsWith("BINDING_FAILED:")) {
            return "参数绑定失败: " + status.substring("BINDING_FAILED:".length()).trim();
        }

        // 批量执行失败
        if (status.startsWith("BATCH_EXECUTE_FAILED:")) {
            return "批量执行失败: " + status.substring("BATCH_EXECUTE_FAILED:".length()).trim();
        }

        // 执行失败
        if ("EXECUTE_FAILED".equals(status)) {
            return "数据库执行失败";
        }

        // 连接失败
        if (status.startsWith("CONNECTION_FAILED:")) {
            return "数据库连接失败: " + status.substring("CONNECTION_FAILED:".length()).trim();
        }

        // 其他未知错误
        return "未知错误: " + status;
    }

    // 规约写入主键
    private void normalizePrimaryKeysForWrite(KafkaPayload row) {
        // 解析当前写入应使用的有效主键
        LinkedHashSet<String> effectiveKeys = resolveEffectivePrimaryKeys();

        // 设置 payload 主键
        row.setPrimaryKeySet(new LinkedHashSet<>(effectiveKeys));
    }

    // 解析当前写入应使用的有效主键
    private LinkedHashSet<String> resolveEffectivePrimaryKeys() {
        // PIPELINE_SQL_RESULT + Doris UNIQUE 使用目标表 UNIQUE KEY
        if (shouldUseTargetUniqueKeyForPipelineSqlResult()) {
            // UNIQUE KEY 为空时直接失败
            if (this.targetUniqueKeySet.isEmpty()) {
                throw new IllegalStateException("targetUniqueKeySet is empty, table=" + this.tableName);
            }

            // 返回目标表 UNIQUE KEY
            return new LinkedHashSet<>(this.targetUniqueKeySet);
        }

        // 普通 TABLE_CDC 固定使用 cdc_rowid
        LinkedHashSet<String> fallbackKeys = new LinkedHashSet<>();

        // 添加 cdc_rowid 主键
        fallbackKeys.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);

        // 返回默认主键
        return fallbackKeys;
    }

    // 校验更新或删除所需主键值是否存在
    private void validatePrimaryKeyValuesPresentForMutation(KafkaPayload row) {
        // 获取有效主键
        Set<String> effectiveKeys = row.getPrimaryKeySet();

        // 主键集合为空时直接失败
        if (effectiveKeys == null || effectiveKeys.isEmpty()) {
            throw new IllegalStateException("effectiveKeys is empty, table=" + this.tableName);
        }

        // 删除使用 beforeData，更新使用 afterData
        Map<String, Object> data = "d".equalsIgnoreCase(row.getOp())
                ? row.getBeforeData()
                : row.getAfterData();

        // 数据为空时直接失败
        if (data == null) {
            throw new IllegalStateException("payload data is null, table=" + this.tableName
                    + ", op=" + row.getOp());
        }

        // 保存缺失的主键
        LinkedHashSet<String> missingKeys = new LinkedHashSet<>();

        // 逐个检查主键
        for (String key : effectiveKeys) {
            // 忽略大小写查找字段值
            Object value = findValueIgnoreCase(data, key);

            // 字段值为空则认为缺失
            if (value == null) {
                missingKeys.add(key);
            }
        }

        // 存在缺失主键时直接失败
        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException("missing primary key values for mutation, table=" + this.tableName
                    + ", op=" + row.getOp()
                    + ", keys=" + effectiveKeys
                    + ", missingKeys=" + missingKeys
                    + ", payloadFields=" + data.keySet());
        }
    }

    // 忽略大小写从数据中查找字段值
    private Object findValueIgnoreCase(Map<String, Object> data, String key) {
        // 空数据直接返回 null
        if (data == null || key == null) {
            return null;
        }

        // 先直接查找
        Object direct = data.get(key);

        // 直接命中则返回
        if (direct != null) {
            return direct;
        }

        // 转成小写 key
        String keyLower = key.toLowerCase(Locale.ROOT);

        // 遍历字段
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            // 忽略空字段名
            if (entry.getKey() == null) {
                continue;
            }

            // 忽略大小写匹配字段名
            if (entry.getKey().toLowerCase(Locale.ROOT).equals(keyLower)) {
                return entry.getValue();
            }
        }

        // 未找到返回 null
        return null;
    }

    // 判断 Doris DUPLICATE 表是否跳过 update/delete
    private boolean shouldSkipMutationForDorisDuplicate() {
        // 仅 Doris DUPLICATE 表跳过 mutation
        return this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS
                && this.tableModel == DorisTableModel.DUPLICATE;
    }

    // 判断当前目标表 writer 是否包含 PIPELINE_SQL_RESULT topic
    private boolean containsPipelineSqlResultTopic() {
        // 遍历当前目标表下的 topic
        for (ServerTopicInfo topicInfo : writeContext.getTopicInfos()) {
            // 跳过空对象
            if (topicInfo == null) {
                continue;
            }

            // 命中 PIPELINE_SQL_RESULT
            if (topicInfo.getSourceKind() == TopicSourceKind.PIPELINE_SQL_RESULT) {
                return true;
            }
        }

        // 未命中
        return false;
    }

    // 判断是否应使用目标表 UNIQUE KEY
    private boolean shouldUseTargetUniqueKeyForPipelineSqlResult() {
        // 仅 PIPELINE_SQL_RESULT + Doris + UNIQUE 表使用目标表唯一键
        return containsPipelineSqlResultTopic()
                && this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS
                && this.tableModel == DorisTableModel.UNIQUE;
    }

    // 判断是否应对 PIPELINE_SQL_RESULT 使用物理删除
    private boolean shouldForcePhysicalDeleteForPipelineSqlResult() {
        // 仅 PIPELINE_SQL_RESULT + Doris 强制物理删除
        return containsPipelineSqlResultTopic()
                && this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS;
    }

    // 初始化 PIPELINE_SQL_RESULT 目标表 UNIQUE KEY
    private void initPipelineSqlResultTargetUniqueKeys() throws Exception {
        // 非 PIPELINE_SQL_RESULT 场景不处理
        if (!containsPipelineSqlResultTopic()) {
            return;
        }

        // 当前逻辑只支持 Doris
        if (this.targetDataSourceConfig == null
                || this.targetDataSourceConfig.getDataSourceType() != DatabaseType.DORIS) {
            throw new IllegalStateException("PIPELINE_SQL_RESULT currently requires Doris target, table="
                    + this.tableName);
        }

        // 当前逻辑要求 Doris UNIQUE 表
        if (this.tableModel != DorisTableModel.UNIQUE) {
            throw new IllegalStateException("PIPELINE_SQL_RESULT requires Doris UNIQUE table model, table="
                    + this.tableName + ", tableModel=" + this.tableModel);
        }

        // 从目标表读取 UNIQUE KEY
        LinkedHashSet<String> resolvedKeys = this.selectStrategy.getTargetUniqueKeys(this.tableName);

        // UNIQUE KEY 不能为空
        if (resolvedKeys == null || resolvedKeys.isEmpty()) {
            throw new IllegalStateException("PIPELINE_SQL_RESULT target table has no readable UNIQUE KEY, table="
                    + this.tableName);
        }

        // 保存 UNIQUE KEY
        this.targetUniqueKeySet.clear();

        // 保存读取到的 UNIQUE KEY
        this.targetUniqueKeySet.addAll(resolvedKeys);

        // 校验 UNIQUE KEY 是否存在于目标表字段中
        validateTargetUniqueKeysExistInTargetColumns();

        // 记录初始化结果
        logger.info("initialized PIPELINE_SQL_RESULT target UNIQUE KEY, table={}, keys={}",
                this.tableName, this.targetUniqueKeySet);
    }

    // 校验目标表 UNIQUE KEY 是否存在于目标表字段列表中
    private void validateTargetUniqueKeysExistInTargetColumns() {
        // 目标表字段为空时直接失败
        if (this.columnInfoList == null || this.columnInfoList.isEmpty()) {
            throw new IllegalStateException("columnInfoList is empty, table=" + this.tableName);
        }

        // 保存目标表字段名
        Set<String> targetColumns = new LinkedHashSet<>();

        // 遍历目标表字段
        for (ColumnInfo columnInfo : this.columnInfoList) {
            // 跳过空字段
            if (columnInfo == null || columnInfo.getColumnName() == null) {
                continue;
            }

            // 保存小写字段名
            targetColumns.add(columnInfo.getColumnName().trim().toLowerCase(Locale.ROOT));
        }

        // 保存缺失的 key
        LinkedHashSet<String> missingKeys = new LinkedHashSet<>();

        // 遍历 UNIQUE KEY
        for (String key : this.targetUniqueKeySet) {
            // 跳过空 key
            if (key == null) {
                continue;
            }

            // 不存在于目标字段中则记录
            if (!targetColumns.contains(key.trim().toLowerCase(Locale.ROOT))) {
                missingKeys.add(key);
            }
        }

        // 有缺失 key 时直接失败
        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException("resolved Doris UNIQUE KEY contains columns missing from target schema"
                    + ", table=" + this.tableName
                    + ", missingKeys=" + missingKeys
                    + ", targetColumns=" + targetColumns);
        }
    }






    //--------------------------------------------------------------------------------------------------------

    // 处理目标表级快照批次。
    // 快照插入与实时新增插入使用统一的批量 insert 字段语义。
    private void processBatchSnapshot(List<KafkaPayload> rows) throws Exception {
        // 调用统一批量处理模板。
        processPayloadBatch(rows, "snapshot");
    }




    // 处理一批 KafkaPayload。
    // 统一封装字段结构分组、Doris padding 判断、分组批量写入流程。
    // 快照插入和实时新增插入都使用统一 insert 字段语义。
    private void processPayloadBatch(List<KafkaPayload> rows,
                                     String batchScene) throws Exception {
        // 空批次直接返回。
        if (rows == null || rows.isEmpty()) {
            return;
        }

        // 按 afterData 字段结构分组。
        Map<String, List<KafkaPayload>> groupedByFields = groupByFieldStructure(rows, batchScene);

        // 没有有效数据时直接返回。
        if (groupedByFields.isEmpty()) {
            logger.warn("skip empty payload batch, scene={}, table={}, rowCount={}",
                    batchScene, tableName, rows.size());
            return;
        }

//        // 记录批量处理摘要。
//        logger.info("start insert payload batch, scene={}, table={}, rowCount={}, fieldGroupCount={}",
//                batchScene, tableName, rows.size(), groupedByFields.size());

        // 判断 Doris 宽表场景是否需要字段补齐。
        boolean needPadding = isDorisTarget() && shouldPadFields(groupedByFields.size());

        // 需要字段补齐时，把有效 payload 合并成一批处理。
        if (needPadding) {
            processSameStructurePayloadBatch(rows, true, batchScene);
            return;
        }

        // 不需要字段补齐时，按字段结构分别处理。
        for (List<KafkaPayload> sameStructureRows : groupedByFields.values()) {
            processSameStructurePayloadBatch(sameStructureRows, false, batchScene);
        }
    }



    // 按 afterData 字段结构对 payload 分组。
    // batchScene 用于日志区分快照批次和实时新增批次。
    private Map<String, List<KafkaPayload>> groupByFieldStructure(List<KafkaPayload> payloads,
                                                                  String batchScene) {
        // 创建分组结果。
        Map<String, List<KafkaPayload>> grouped = new LinkedHashMap<>();

        // 空输入直接返回空分组。
        if (payloads == null || payloads.isEmpty()) {
            return grouped;
        }

        // 遍历 payload。
        for (KafkaPayload payload : payloads) {
            // 跳过空 payload。
            if (payload == null) {
                continue;
            }

            // 获取 afterData。
            Map<String, Object> afterData = payload.getAfterData();

            // afterData 为空时跳过。
            if (afterData == null || afterData.isEmpty()) {
                logger.warn("skip empty afterData, scene={}, table={}, topic={}, offset={}",
                        batchScene, tableName, payload.getKafkaTopic(), payload.getOffset());
                continue;
            }

            // 生成字段结构签名。
            String fieldSignature = generateFieldSignature(afterData);

            // 加入对应字段结构分组。
            grouped.computeIfAbsent(fieldSignature, ignored -> new ArrayList<>()).add(payload);
        }

        // 返回分组结果。
        return grouped;
    }

    // 处理 相同字段结构的 一批 payload。
    // 该方法只负责把 payload 转成 BatchInsertItem 并调用统一批量写入。
    private void processSameStructurePayloadBatch(List<KafkaPayload> sameStructurePayloads,
                                                  boolean needPadding,   // doris在某些场景下，这些KafkaPayload的字段需要补齐
                                                  String batchScene)     // 没吊用，纯粹用于日志
            throws Exception {
        // 空批次直接返回。
        if (sameStructurePayloads == null || sameStructurePayloads.isEmpty()) {
            return;
        }

        // 创建批量插入 item 列表。
        List<BatchInsertItem> batchItems = new ArrayList<>();

        // 遍历当前批次。
        for (KafkaPayload payload : sameStructurePayloads) {
            // 跳过空 payload。
            if (payload == null) {
                continue;
            }

            // 构造用于 批量insert 的item，相当于一个bean。
            BatchInsertItem item = buildInsertBatchItem(payload);

            // 构造结果为空时跳过。
            if (item == null) {
                logger.warn("skip payload when build insert batch item, scene={}, table={}, topic={}, offset={}",
                        batchScene, tableName, payload.getKafkaTopic(), payload.getOffset());
                continue;
            }

            // 加入批量写入列表。
            batchItems.add(item);
        }

        // 没有可写入 item 时直接返回。
        if (batchItems.isEmpty()) {
            return;
        }

        // 调用现有批量写入逻辑。
        invokeBatchInsert(batchItems, needPadding);
    }




    // 处理实时新增数据。
    // op 为 r/c 且目标库类型启用批量时，只进入缓冲区，不立即写库，不立即推进 offset。
    private void handleRealtimeInsert(KafkaPayload row, Context ctx) throws Exception {
        // 判断 buffer 是否从空变为非空。
        // 只有这个时机注册 timer，避免每条数据注册一个 timer。
        boolean wasEmpty = realtimeInsertBuffer.isEmpty();

        // 缓存当前实时新增数据。
        realtimeInsertBuffer.add(row);

        // buffer 从空变为非空时注册一个 processing-time timer。
        if (wasEmpty) {
            registerRealtimeInsertFlushTimer(ctx);
        }

        // 达到当前批量阈值时立即刷新。
        if (realtimeInsertBuffer.size() >= batchSizeManager.getCurrentBatchSize()) {
            // 刷新实时新增缓冲区。
            flushRealtimeInsertBufferIfNecessary();

            // 主动刷新后删除还未触发的 timer。
            deleteRealtimeInsertFlushTimer(ctx);
        }
    }



    // 如果实时新增缓冲区非空，则批量写入目标库。
    // 该方法只处理 realtimeInsertBuffer，不处理 snapshotBuffer。
    private void flushRealtimeInsertBufferIfNecessary() throws Exception {
        // 缓冲区为空时无需处理。
        if (realtimeInsertBuffer.isEmpty()) {
            return;
        }

        // 复制当前缓冲区，避免处理过程中直接依赖原 list。
        List<KafkaPayload> rows = new ArrayList<>(realtimeInsertBuffer);

        // 使用统一批量处理模板处理实时新增数据。
        processPayloadBatch(rows, "realtime-insert");

        // 批量处理完成后清空实时新增缓冲区。
        realtimeInsertBuffer.clear();
    }



    // 构造 insert 数据的 BatchInsertItem。
    // 快照 insert 和实时新增 insert 使用同一套目标字段语义。
    // 所有 insert 的 cdc_op 都写 i。
    // 所有 insert 都不写 cdc_update_time。
    private BatchInsertItem buildInsertBatchItem(KafkaPayload payload) {
        // afterData 为空时跳过。
        if (payload.getAfterData() == null || payload.getAfterData().isEmpty()) {
            return null;
        }

        // 复制 afterData，避免修改原 payload。
        Map<String, Object> afterData = new HashMap<>(payload.getAfterData());

        // insert 统一语义：无论源端 op 是 r 还是 c，目标 cdc_op 都写 i。
        afterData.put("cdc_op", "i");

        // insert 统一语义：写入源端 DML 时间。
        afterData.put("cdc_dml_Time", payload.getDmlTime());

        // insert 统一语义：写入 Kafka 入队时间。
        afterData.put("cdc_kafka_enqueue_time", payload.getKafkaEnqueueTime());

        // insert 统一语义：写入目标插入时间。
        afterData.put("cdc_insert_time", DateFormatUtils.getCurrentTimeString());

        // insert 统一语义：不写 cdc_update_time，目标字段允许为空。
        return new BatchInsertItem(payload, afterData);
    }

    // 判断当前目标库是否为 Doris。
    // 字段补齐策略目前只对 Doris 生效。
    private boolean isDorisTarget() {
        // 目标库配置为空时不是 Doris。
        if (this.targetDataSourceConfig == null) {
            return false;
        }

        // 判断目标库类型。
        return this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS;
    }



    // 实时数据共用的 processing-time 定时器回调。
    // 到期时刷新新增、更新、删除缓冲区，并输出同步日志和 offset 提交事件。
    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<OperationResult> out) throws Exception {
        // 忽略已经失效的定时器回调。
        if (timestamp != realtimeInsertFlushTimerTs) {
            return;
        }

        // 当前定时器已经触发，清除注册标记。
        realtimeInsertFlushTimerTs = -1L;

        // 刷新尚未达到新增批量阈值的数据。
        flushRealtimeInsertBufferIfNecessary();

        // 刷新尚未达到 500 条的更新数据。
        flushRealtimeUpdateBufferIfNecessary();

        // 刷新尚未达到 500 条的删除数据。
        flushRealtimeDeleteBufferIfNecessary();

        // 输出本次刷新生成的逐条同步日志。
        drainLogs(ctx);

        // 输出处理完成后的 offset 提交事件。
        emitOffsetCommitEvents(ctx);
    }

}