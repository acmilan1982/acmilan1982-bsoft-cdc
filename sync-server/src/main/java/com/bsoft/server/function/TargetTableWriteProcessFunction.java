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
//                // 遇到任意实时数据时，先 flush 当前目标表快照缓冲区
//                flushSnapshotBufferIfNecessary();

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



    // 执行批量插入
    private void invokeBatchInsert(List<BatchInsertItem> batchItems,
                                   boolean needPadding) throws Exception {
        // 空批次直接返回
        if (batchItems == null || batchItems.isEmpty()) {
            return;
        }

        // 调用现有插入策略执行批量写入
        Map<Long, String> statusMap = this.insertStrategy.executeBatchInsert(
                this.tableName,
                batchItems,
                this.batchSizeManager,
                this.columnInfoList,
                needPadding
        );

        // 为每条数据生成日志
        for (BatchInsertItem item : batchItems) {
            // 获取 payload
            KafkaPayload payload = item.getPayload();

            // 获取 offset
            Long offset = payload.getOffset();

            // 获取写入状态
            String status = statusMap.getOrDefault(offset, "UNKNOWN_ERROR");

            // 判断是否成功
            boolean success = "SUCCESS".equals(status);

            // 构造日志详情
            String logDetail = getBatchLogDetail(success, status);

            // 添加日志
            pendingLogEntries.add(buildSyncLogEntry(payload, success, logDetail));
        }

        // 保持旧语义，批次执行结束后按 topic 推进 offset
        advanceOffsetsByBatch(batchItems);
    }

















    // 处理实时数据。
    // 如果当前目标库类型启用了实时新增批量写入，则 op 为 r/c 的数据进入实时批量缓冲区。
    // 未启用批量写入的目标库继续保持原有单条 insert 行为。
    private void processRealTimeData(KafkaPayload row, Context ctx) throws Exception {
        // 获取操作类型。
        String op = row.getOp();

        // 按操作类型分发。
        switch (op) {
            case "r":
            case "c":
                // 当前目标库类型启用实时新增批量写入时，缓存 r/c 数据。
                if (isRealtimeInsertBatchEnabled()) {
                    handleRealtimeInsert(row, ctx);
                } else {
                    // 未启用批量写入时，保持旧逻辑：单条插入。
                    invokeInsert(row);

                    // 单条处理完成后推进 offset。
                    advanceOffset(row);
                }
                break;

            case "u":
                // update 前先刷新实时新增缓冲区，保证同一 topic 内 insert 先于 update 落库。
                flushRealtimeInsertBufferIfNecessary();

                // 主动 flush 后删除还未触发的 timer。
                deleteRealtimeInsertFlushTimer(ctx);

                // 执行原有单条 update。
                invokeUpdate(row);

                // update 处理完成后推进 offset。
                advanceOffset(row);
                break;

            case "d":
                // delete 前先刷新实时新增缓冲区，保证同一 topic 内 insert 先于 delete 落库。
                flushRealtimeInsertBufferIfNecessary();

                // 主动 flush 后删除还未触发的 timer。
                deleteRealtimeInsertFlushTimer(ctx);

                // 执行原有单条 delete。
                invokeDelete(row);

                // delete 处理完成后推进 offset。
                advanceOffset(row);
                break;

            case "ddl":
                // ddl 前先刷新实时新增缓冲区。
                flushRealtimeInsertBufferIfNecessary();

                // 主动 flush 后删除还未触发的 timer。
                deleteRealtimeInsertFlushTimer(ctx);

                // 执行原有 DDL 处理。
                invokeDDL(row);

                // DDL 如果携带 topic/offset，则保持原有推进语义。
                advanceOffset(row);
                break;

            default:
                // 未知操作前也先刷新实时新增缓冲区，避免前序 insert 长时间滞留。
                flushRealtimeInsertBufferIfNecessary();

                // 主动 flush 后删除还未触发的 timer。
                deleteRealtimeInsertFlushTimer(ctx);

                // 未知操作只记录日志，不写库。
                logger.warn("unknown operation type skipped, op={}, table={}, topic={}",
                        op, tableName, row.getKafkaTopic());

                // 保持旧语义：未知操作处理完也推进 offset。
                advanceOffset(row);
                break;
        }
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

        // 记录批量处理摘要。
        logger.info("start insert payload batch, scene={}, table={}, rowCount={}, fieldGroupCount={}",
                batchScene, tableName, rows.size(), groupedByFields.size());

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

    // 处理同一字段结构的一批 payload。
// 该方法只负责把 payload 转成 BatchInsertItem 并调用统一批量写入。
    private void processSameStructurePayloadBatch(List<KafkaPayload> sameStructurePayloads,
                                                  boolean needPadding,
                                                  String batchScene) throws Exception {
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

            // 构造统一 insert 批量 item。
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




    // 判断当前目标库类型是否启用实时新增批量写入。
    // 配置来源为 ServerInfo；未配置时 ServerInfo 默认启用 DORIS。
    private boolean isRealtimeInsertBatchEnabled() {
        // 目标库配置为空时不启用批量。
        if (this.targetDataSourceConfig == null) {
            return false;
        }

        // 通过 ServerInfo 判断当前目标库类型是否启用实时批量。
        return this.serverInfo != null
                && this.serverInfo.isRealtimeInsertBatchEnabled(this.targetDataSourceConfig.getDataSourceType());
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



    // processing-time timer 到期回调。
// 用于刷新未达到批量阈值的实时新增缓冲区。
// timer 回调运行在 Flink 算子线程内，可以安全写 side output。
    @Override
    public void onTimer(long timestamp,
                        OnTimerContext ctx,
                        Collector<OperationResult> out) throws Exception {
        // 过滤过期 timer。
        // 主动 flush 时会删除 timer，但边界情况下仍可能收到旧 timer 回调。
        if (timestamp != realtimeInsertFlushTimerTs) {
            return;
        }

        // 当前 timer 已经触发，清空 active timer 标记。
        realtimeInsertFlushTimerTs = -1L;

        // timer 到期时刷新实时新增缓冲区。
        flushRealtimeInsertBufferIfNecessary();

        // 输出 flush 过程中生成的同步日志。
        drainLogs(ctx);

        // 输出 flush 过程中生成的 offset commit event。
        emitOffsetCommitEvents(ctx);
    }

}