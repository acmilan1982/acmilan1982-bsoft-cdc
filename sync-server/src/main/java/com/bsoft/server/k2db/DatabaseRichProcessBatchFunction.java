package com.bsoft.server.k2db;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleAlterTableModify;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.DorisTableModel;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import com.bsoft.common.enums.TopicSourceKind;
import com.bsoft.common.metric.MonitorMetricEvent;
import com.bsoft.common.util.DateFormatUtils;
import com.bsoft.common.util.DateTimeConstants;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.server.api.SyncClientHTTPClient;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.offset.TopicOffsetCommitEvent;
import com.bsoft.server.sql.ddl.DDLStrategy;
import com.bsoft.server.sql.delete.DeleteStrategy;
import com.bsoft.server.sql.factory.*;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.select.SelectStrategy;
import com.bsoft.server.sql.update.UpdateStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

// 每一个主题，即每一个源表，对应一个 DatabaseRichProcessFunction 实例
public class DatabaseRichProcessBatchFunction extends ProcessFunction<KafkaPayload, OperationResult> {


    private static final Logger logger = LogManager.getLogger(DatabaseRichProcessBatchFunction.class);
//
//
//
//    // 共享池需要保存 cfg 做 release 的指纹
//    private transient HikariConfig hikariConfig;
//    // 目标库的数据源
//    private transient DataSource hikariDataSource;

    private ServerInfo serverInfo;

    private ServerTopicInfo serverTopicInfo;

    // 目标库配置
    private TargetDataSourceConfig targetDataSourceConfig;

    // 目标表表名
    private String tableName;

    // 表行删除策略
    private TableRowDeleteStrategy tableRowDeleteStrategy;

//    // 用于写入当前消息的偏移量
//    private transient GlobalZooKeeperManager offsetManager;

//    // 移除原有的 LogCollector logCollector 字段
//    private transient LogCollector logCollector;


    //---------------快照数据批处理相关----------------
    // 快照数据缓存，条件满足时候，批量处理
    private List<KafkaPayload> snapshotBuffer;


    // 批量处理大小，由配置文件决定，仅限于关系型数据库
    // doris等批量写入能力强的数据库，不受此限制
    private int snapshotBatchSize;

    // 批次大小管理器
    private BatchSizeManager batchSizeManager;

    // ==================== 字段补齐策略常量 ====================

    /** 字段补齐策略 - 宽表字段阈值 */
    private static final int WIDE_TABLE_COLUMN_THRESHOLD = 100;

    /** 字段补齐策略 - 分组数量阈值（激进策略） */
    private static final int PADDING_GROUP_THRESHOLD = 10;


    private List<ColumnInfo> columnInfoList;

    // 当前主题已处理的记录总数
    private long recordCount = 0L;


    private long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    // 获取查询策略
    private SelectStrategy selectStrategy;

    // 获取插入策略
    private InsertStrategy insertStrategy;

    // 获取删除策略
    private DeleteStrategy deleteStrategy;

    // 获取更新策略
    private UpdateStrategy updateStrategy;

    // 获取更新策略
    private DDLStrategy ddlStrategy;

    private SyncClientHTTPClient clientHTTPClient;


//    // 独立线程定时检查快照缓冲区
//    private transient ScheduledExecutorService snapshotFlushExecutor;
//    private transient ScheduledFuture<?> flushTask;
    // 快照阶段是否结束的标记
    private volatile boolean snapshotPhaseEnded = false;

    // 在类中添加常量
    private static final long TABLE_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(3); // 3分钟
    // 字段扩展的重试次数
    private static final int MAX_EXPANSION_RETRIES = 10; // 最多重试10次，共30分钟


    // 定义侧输出标签，用于输入日志
    private  OutputTag<String> syncLogEntryOutputTag;
    // 定义侧输出标签，用于输入日志
    private  OutputTag<String> metricEventOutputTag;

    // 日志缓冲队列
    private final Queue<SyncLogEntry> logQueue = new ConcurrentLinkedQueue<>();


    // topic offset 提交事件侧输出标签
    private final OutputTag<TopicOffsetCommitEvent> topicOffsetCommitOutputTag;

    private static final ObjectMapper mapper = new ObjectMapper();


    // ===== monitor offset 上报参数（建议后续改成配置）=====
    private static final long SNAPSHOT_REPORT_INTERVAL_MS = 10_000L; // 快照阶段：10s
    private static final int SNAPSHOT_REPORT_ROWS = 10_000;          // 快照阶段：1w条
    private static final long REALTIME_REPORT_INTERVAL_MS = 2_000L;  // 实时阶段：2s
    private static final int REALTIME_REPORT_ROWS = 500;             // 实时阶段：500条

    private enum OffsetPhase { SNAPSHOT, REALTIME }

    // 仅内存态（每个算子实例内）
    private long latestSnapshotOffset = -1L;
    private long latestRealtimeOffset = -1L;
    private long lastSnapshotReportTs = 0L;
    private long lastRealtimeReportTs = 0L;
    private int snapshotRowsSinceLastReport = 0;
    private int realtimeRowsSinceLastReport = 0;

    // doris建表模型
    private DorisTableModel tableModel;

    // PIPELINE_SQL_RESULT + Doris UNIQUE 表时，从目标表读取到的 UNIQUE KEY
    private LinkedHashSet<String> targetUniqueKeySet = new LinkedHashSet<>();


    // ===== offset commit 节流参数（先放当前类，后续稳定后再考虑外置）========================================

    // offset commit 最长上报间隔，达到这个时间即使条数不足也发一次
    private static final long OFFSET_COMMIT_REPORT_INTERVAL_MS = 10_000L;

    // offset commit 累计成功条数阈值，达到这个条数即使时间不足也发一次
    private static final int OFFSET_COMMIT_REPORT_ROWS = 200;

    // ===== offset commit 运行态字段 =====

    // 当前 topic 最近一次成功推进到的最大 offset
    private long latestOffsetCommitCandidate = -1L;

    // 上一次已经输出到 offset 侧输出流的最大 offset
    private long lastReportedOffsetCommitCandidate = -1L;
//
//    // 自上次发出 offset commit 事件后累计成功的条数
//    private int offsetCommitRowsSinceLastReport = 0;
//
//    // 上次发出 offset commit 事件的时间戳
//    private long lastOffsetCommitReportTs = System.currentTimeMillis();






    // 每个DatabaseRichProcessFunction，只写入指定目标库中的指定表(只会写入一个表)
    public DatabaseRichProcessBatchFunction(ServerInfo serverInfo,
                                            ServerTopicInfo serverTopicInfo,
                                            OutputTag<String> syncLogEntryOutputTag,
                                            OutputTag<String> metricEventOutputTag,
                                            OutputTag<TopicOffsetCommitEvent> topicOffsetCommitOutputTag) {

        this.serverInfo = serverInfo;
        this.serverTopicInfo = serverTopicInfo;
        // 获取目标表名
        this.tableName = serverTopicInfo.getTargetTableName();
        // 获取目标库配置
        this.targetDataSourceConfig = serverInfo.getTargetDataSourceConfigRegistry().findTargetDataSourceConfigByDataSourceId(serverTopicInfo.getTargetDataSourceId());
        // 获取表行删除策略
        this.tableRowDeleteStrategy = serverInfo.getTableRowDeleteStrategy();
        // 初始化快照数据缓存
        this.snapshotBuffer =  new ArrayList<>();
        // 全量快照每批次处理的记录数
        this.snapshotBatchSize = serverInfo.getSnapshotBatchSize();

        // 初始化批次大小管理器
        this.batchSizeManager = new BatchSizeManager(
                targetDataSourceConfig.getDataSourceType(),
                targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS ? -1 : snapshotBatchSize
        );

        this.syncLogEntryOutputTag = syncLogEntryOutputTag;
        this.metricEventOutputTag = metricEventOutputTag;


        // offset 提交事件侧输出
        this.topicOffsetCommitOutputTag = topicOffsetCommitOutputTag;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        // 1. 构建 HikariConfig
//        this.hikariConfig = buildHikariConfig(this.targetDataSourceConfig);
//
//        // 2. 从共享池获取（可能是复用）
//        this.hikariDataSource = SharedHikariPool.acquire(this.hikariConfig);


        // 1.获取查询策略对象
        this.selectStrategy = SelectStrategyFactory.getStrategy(targetDataSourceConfig);

        // 2.获取插入策略对象
        this.insertStrategy = InsertStrategyFactory.getStrategy(targetDataSourceConfig);

        // 3.获取删除策略对象
        this.deleteStrategy = DeleteStrategyFactory.getStrategy(targetDataSourceConfig,tableRowDeleteStrategy);

        // 4.获取更新策略对象
        this.updateStrategy = UpdateStrategyFactory.getStrategy(targetDataSourceConfig);

        // 5.获取DDL策略对象
        this.ddlStrategy = DDLStrategyFactory.getStrategy(targetDataSourceConfig);


        // 10.初始化全局offset管理器（单例模式，只会创建一个实例）
//        this.offsetManager = GlobalZooKeeperManager.getInstance(serverInfo);


        // 11.使用全局共享的 LogCollector
//`        this.logCollector = GlobalLogCollectorManager.getInstance(
//                this.serverInfo.getConfigDataSourceConfig(),
//                this.serverInfo.getRawMessageStorageStrategy()
//        );`

        // 12. 注册当前处理的表名到全局offset管理器
//        this.offsetManager.addSourceTableName(this.serverTopicInfo);



        // 15. 初始化 ClientHTTPClient 实例,用于向探针端请求表结构信息
        this.clientHTTPClient = new SyncClientHTTPClient(this.serverInfo);
        this.clientHTTPClient.init();

        // 16-1 可以自动建表
        // 16-2 多表合并场景下，确保目标表结构就绪(比如扩展字段长度)
        if(this.serverInfo.isAutoCreateTable() || this.serverInfo.isAutoExpandColumnLength()){
            ensureTableSchemaReady();
        }


        // 17. 初始化批次管理器的最后刷新时间
        this.batchSizeManager.setLastFlushTime(System.currentTimeMillis());

//        // 18. 启动独立线程定时检查快照缓冲区
//        this.snapshotFlushExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
//            Thread t = new Thread(r, "snapshot-flush-" + tableName);
//            t.setDaemon(true); // 设置为守护线程
//            return t;
//        });
//
//        // 每10秒检查一次
//        this.flushTask = snapshotFlushExecutor.scheduleAtFixedRate(
//                this::checkAndFlushSnapshot,
//                generateRandomInitialDelay(),   // 初始延迟：30-60秒随机
//                30,                             // 固定间隔：30秒
//                TimeUnit.SECONDS
//        );



        this.columnInfoList = this.ddlStrategy.getTableColumns(tableName);


        this.tableModel = this.targetDataSourceConfig.getTableModel(this.tableName);


        // PIPELINE_SQL_RESULT + Doris UNIQUE 表时，从目标表读取到的 UNIQUE KEY
        initPipelineSqlResultTargetUniqueKeys();
    }



    /**
     * 统一的“目标表就绪”流程：
     * 1) 检查表是否存在
     * 2) 不存在则按 sourceKind 请求建表
     * 3) 存在后仅 TABLE_CDC 场景可触发扩列
     */
    private void ensureTableSchemaReady() {
        int retryCount = 0;
        TopicSourceKind sourceKind = resolveSourceKind();

        logger.info("开始检查目标表是否就绪: table={}, sourceKind={}", tableName, sourceKind);

        while (true) {
            try {
                boolean exists = this.selectStrategy.checkTableExists(this.tableName);
                if (exists) {
                    logger.info("目标表已存在: table={}, sourceKind={}", tableName, sourceKind);

                    // 关键约束：PIPELINE_SQL_RESULT 下不做扩列
                    if (shouldHandleColumnExpansion(sourceKind)) {
                        handleColumnExpansion();
                    }
                    return;
                }

                retryCount++;
                logger.info("目标表不存在，准备请求建表: table={}, sourceKind={}, retry={}",
                        tableName, sourceKind, retryCount);

                // 向探针端发送建表语句请求
                requestCreateTableFromClient(sourceKind);

                logger.info("建表请求已发送，等待 {} 秒后重试: table={}, sourceKind={}",
                        TimeUnit.MILLISECONDS.toSeconds(TABLE_CHECK_INTERVAL_MS), tableName, sourceKind);

                TimeUnit.MILLISECONDS.sleep(TABLE_CHECK_INTERVAL_MS);

            } catch (Exception e) {
                logger.warn("目标表就绪检查失败，将继续重试: table={}, sourceKind={}, retry={}, err={}",
                        tableName, sourceKind, retryCount, e.getMessage());

                try {
                    TimeUnit.MILLISECONDS.sleep(TABLE_CHECK_INTERVAL_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.warn("目标表就绪等待被中断: table={}, sourceKind={}", tableName, sourceKind);
                    return;
                }
            }
        }
    }

    private TopicSourceKind resolveSourceKind() {
        return this.serverTopicInfo.getSourceKind() == null
                ? TopicSourceKind.TABLE_CDC
                : this.serverTopicInfo.getSourceKind();
    }

    private boolean shouldHandleColumnExpansion(TopicSourceKind sourceKind) {
        return sourceKind != TopicSourceKind.PIPELINE_SQL_RESULT
                && this.serverInfo.isAutoExpandColumnLength();
    }
    /**
     * 处理字段扩展逻辑
     * 发送扩展请求并重试，达到最大重试次数后结束（不影响任务启动）
     */
    private void handleColumnExpansion() {

        for (int i = 0; i < MAX_EXPANSION_RETRIES; i++) {
            try {
                logger.info("目标表表 {}  尝试向探针端发送字段扩展请求（第 {}/{} 次）...", tableName,i + 1, MAX_EXPANSION_RETRIES);
                // 向探针端请求表结构信息
                requestTableSchemaFromClient();
                logger.info("目标表表 {}   字段扩展请求已发送，等待 {} 秒后继续...", tableName,TimeUnit.MILLISECONDS.toSeconds(TABLE_CHECK_INTERVAL_MS));
                TimeUnit.MILLISECONDS.sleep(TABLE_CHECK_INTERVAL_MS);
                // 发送成功即可，不验证结果
                logger.info("✅ 字段扩展处理完成");
                return;

            } catch (Exception e) {
                e.printStackTrace();
                // 任何异常都静默处理，继续重试
                logger.debug("字段扩展请求失败（第 {}/{} 次），将继续重试", i + 1, MAX_EXPANSION_RETRIES);
            }
        }

        // 达到最大重试次数，字段扩展结束（不影响任务）
        logger.info("⚠️ 字段扩展请求达到最大重试次数，跳过扩展（不影响任务启动）");
    }


    private void requestCreateTableFromClient(TopicSourceKind sourceKind) throws Exception {
        if (sourceKind == TopicSourceKind.TABLE_CDC) {
            requestTableSchemaFromClient();
        } else {
            requestSqlSchemaFromClient();
        }
    }


    // 向探针端请求表结构信息，用于建表或扩展字段
    private void requestTableSchemaFromClient() throws Exception {

        TableDDLRequest tableDDLRequest = new TableDDLRequest();
        tableDDLRequest.setClientInfo(this.serverTopicInfo.getClientInfo());
        tableDDLRequest.setSchemaName(this.serverTopicInfo.getSchemaName());
        tableDDLRequest.setTableName(this.serverTopicInfo.getTableName());
        tableDDLRequest.setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId());

        // 复用已有的 ClientHTTPClient 实例
        this.clientHTTPClient.requestTableDdlExport(tableDDLRequest);

        logger.info("已向探针端发送表结构请求: clientId={}, schema={}, table={}",
                tableDDLRequest.getClientId(),
                tableDDLRequest.getSchemaName(),
                tableDDLRequest.getTableName());
    }


    // 向探针端请求 sql 语句执行结果信息，用于建表或扩展字段
    private void requestSqlSchemaFromClient() throws Exception {
        String topic = serverTopicInfo.getTopic();

        String clientId = requireNonBlank(serverTopicInfo.getClientId(), "missing clientId, topic=" + topic);
        String sql = requireNonBlank(serverTopicInfo.getSqlQuery(), "missing sqlQuery, topic=" + topic);
        String schema = requireNonBlank(serverTopicInfo.getSqlSchemaName(), "missing sqlSchemaName, topic=" + topic);
        String sqlNodeId = requireNonBlank(serverTopicInfo.getSqlNodeId(), "missing sqlNodeId, topic=" + topic);


        if (this.targetDataSourceConfig == null) {
            throw new IllegalStateException("missing targetDataSourceConfig, topic=" + topic);
        }
        String targetTableName = requireNonBlank(this.tableName, "missing targetTableName, topic=" + topic);

        OracleSqlColumnsQueryRequest request = new OracleSqlColumnsQueryRequest();
        request.setClientId(clientId);
        request.setSql(sql);
        request.setSchema(schema);
        request.setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId());
        request.setTargetTableName(targetTableName);
        request.setSqlNodeId(sqlNodeId);

        logger.info("发送 SQL 建表请求: topic={}, clientId={}, schema={}, targetTable={}",
                topic, clientId, schema, targetTableName);

        this.clientHTTPClient.requestSqlColumnsExport(request);
    }

    private String requireNonBlank(String value, String message) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value.trim();
    }


//    /**
//     * 定时检查并刷新快照缓冲区
//     * 执行顺序按照发生频率排序：最高频的检查放在最前面
//     */
//    private void checkAndFlushSnapshot() {
//        try {
//            // 1.没有超过最大时间间隔，不做任何处理（最高频场景，无需加锁的快速检查）
//            // 快照阶段可能持续几个小时，定时任务会执行几百次，大部分情况下时间未到直接返回
//            if(!isFlushTimeoutExceeded()){
//                return;
//            }
//
//
//            // 2.超时了，刷新快照缓冲区（中频场景，只在超时时执行）
//            // 只在这里加锁，减少锁竞争
//            synchronized (this) {
//                if (!snapshotBuffer.isEmpty()) {
//                    // 刷新快照缓冲区并更新时间戳
//                    flushSnapshotBuffer();
//                }
//            }
//
//            // 3.快照完成后，停止刷新线程（最低频场景，只在快照结束后发生一次）
//            if (snapshotPhaseEnded) {
//                logger.info("✅ 快照阶段已结束,停止刷新线程: 表={}", tableName);
//                shutdownFlushExecutor();
//            }
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.error("❌ 定时刷新快照失败: 表={}", tableName, e);
//        }
//    }

//    // 停止线程，关闭线程池
//    private void shutdownFlushExecutor() {
//        // 1. 先取消定时任务
//        if (flushTask != null && !flushTask.isCancelled()) {
//            flushTask.cancel(false); // 不中断正在执行的任务
//        }
//
//        // 2. 再关闭线程池
//        if (snapshotFlushExecutor != null && !snapshotFlushExecutor.isShutdown()) {
//            snapshotFlushExecutor.shutdown();
//            try {
//                if (!snapshotFlushExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
//                    snapshotFlushExecutor.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                snapshotFlushExecutor.shutdownNow();
//                Thread.currentThread().interrupt();
//            }
//        }
//    }

//    /**
//     * 检查目标表是否存在
//     * @return true-表存在, false-表不存在
//     */
//    private boolean checkTableExists() throws Exception {
//
//        // 构建SQL
//        String sql = this.selectStrategy.buildCheckTableExistsSql();
//
//        try (Connection conn = hikariDataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            this.selectStrategy.setParameters(ps, this.tableName);
//            try (ResultSet rs = ps.executeQuery()) {
//                if (rs.next()) {
//                    int count = rs.getInt(1);
//                    boolean exists = count > 0;
//                    logger.info("🔍 表 {} 存在检查结果: {}", tableName, exists);
//                    return exists;
//                }
//            }
//
//        } catch (Exception e) {
//            logger.error("❌ 检查表是否存在失败: {}", tableName, e);
//            throw e;
//        }
//
//        return false;
//    }



    @Override
    public void processElement(KafkaPayload row, Context ctx, Collector<OperationResult> out) throws Exception {

        // ✅ 记录方法开始时间
        long methodStartTime = System.currentTimeMillis();

        long startTime = System.currentTimeMillis();
        String op = row.getOp();


        // 当前消息的操作结果
        OperationResult result = new OperationResult();
        result.setServerId(serverInfo.getServerId());
        result.setDataFromSourceId(row.getBusinessDataSourceId());
        result.setDateToSourceId(targetDataSourceConfig.getDataSourceId());
        result.setTableName(tableName);
        result.setOperation(op);
        result.setOriginalPayload(row);

        try {
            // 检查是否是快照数据且快照阶段未结束
            if (row.isSnapshot()) {
                synchronized(this) {
                    handleSnapshotData(row);
                }
            } else {
                //  遇到第一条非快照数据,标记快照阶段结束
                if (!snapshotPhaseEnded) {
                    snapshotPhaseEnded = true;
                    logger.info("🏁 快照阶段结束标记已设置: 表={}", tableName);
                }

                synchronized(this) {
                    // 如果遇到非快照数据，先处理缓存中的快照数据
                    if (!snapshotBuffer.isEmpty()) {
                        // 刷新快照缓冲区并更新时间戳
                        flushSnapshotBuffer();
                        logger.info("📦 快照阶段结束，共处理 {} 条快照数据", snapshotBuffer.size());
                    }
                }

                /*
                     唯一键的规约化处理:
                     1.如果满足 PIPELINE_SQL_RESULT & DorisTableModel.UNIQUE,那么使用 目标表探测得到的 主键
                     2.单表CDC，固定使用：CDC_ROWID_COLUMN_LOWER
                 */
                normalizePrimaryKeysForWrite(row);
                // 处理实时数据
                processRealTimeData(row, out);
            }

            result.setSuccess(true);
        } catch (Exception e) {
            logger.error(" kafka消息处理失败: {}", e.getMessage());
            e.printStackTrace();
            result.setSuccess(false);
            result.setErrorMessage(e.getMessage());
        } finally {
            long executionTime = System.currentTimeMillis() - startTime;
            result.setExecutionTimeMs(executionTime);


//            // 异步更新offset，快照阶段结束后，才更新offset
//            if (!snapshotPhaseEnded) {
//                Map<String, String> metric = new HashMap<>();
//                metric.put(MetricKeys.TOPIC, this.serverTopicInfo.getTopic());
//                metric.put(MetricKeys.TARGET_DATA_SOURCE_ID, this.targetDataSourceConfig.getDataSourceId());
//                metric.put(MetricKeys.TARGET_TABLE_NAME, this.tableName);
//                metric.put(MetricKeys.OFFSET, row.getOffset().toString());
//
//                // 异步更新offset
//                offsetManager.updateOffset(metric);
//            }

            // monitorOffset 近似上报
//            if (result.isSuccess() && row.getOffset() != null) {
//                OffsetPhase phase = snapshotPhaseEnded ? OffsetPhase.REALTIME : OffsetPhase.SNAPSHOT;
//                MonitorMetricEvent monitorMetricEvent = buildMonitorOffsetEventIfDue(row.getOffset(), phase);
//                if(monitorMetricEvent != null){
//                    ctx.output(metricEventOutputTag, mapper.writeValueAsString(monitorMetricEvent));
//                }
//            }

            // 日志输出到侧输出流
            SyncLogEntry logEntry;
            while ((logEntry = logQueue.poll()) != null) {
                ctx.output(syncLogEntryOutputTag, mapper.writeValueAsString(logEntry));
            }


            // topic offset commit 事件按节流策略输出到侧输出流
            TopicOffsetCommitEvent offsetCommitEvent = buildOffsetCommitEventIfDue();
            if (offsetCommitEvent != null) {
                ctx.output(topicOffsetCommitOutputTag, offsetCommitEvent);
            }

            // 输出结果供下游统计使用
            out.collect(result);


            // ✅ 累计执行时间
            totalDeserializeTime += (System.currentTimeMillis() - methodStartTime);
            ++recordCount;

            if(recordCount % 1000 == 0){

//                logger.info("累计processElement耗时: {}s, 平均单条耗时: {}s",
//
//                        totalDeserializeTime / 1000.0,
//                        (totalDeserializeTime / recordCount) / 1000.0);


            }

        }
    }


    // 缓存快照数据，条件满足时候，处理快照数据
    private synchronized void handleSnapshotData(KafkaPayload row) throws Exception {
        // 1. 先缓存当前快照数据
        snapshotBuffer.add(row);

        // 使用批次管理器获取当前批次大小，可能会动态调整
        int currentBatchSize = batchSizeManager.getCurrentBatchSize();

        // 2.如果 缓存数量 达到批次大小 或者 超过最大时间，马上进行批量处理，否则继续缓存
        if ((snapshotBuffer.size() >= currentBatchSize  || isFlushTimeoutExceeded()) || row.lastRecordInTable()) {

            // 刷新快照缓冲区并更新时间戳
            flushSnapshotBuffer();
        }

    }

    /**
     * 判断距离上次刷新是否超过最大时间间隔
     * @return true-超过刷新间隔，需要强制刷新; false-未超过
     */
    public boolean isFlushTimeoutExceeded() {
        // 当前时间
        long currentTime = System.currentTimeMillis();
        return  (currentTime - this.batchSizeManager.getLastFlushTime()) >= this.batchSizeManager.getFlushIntervalMs();
    }

    /**
     * 刷新快照缓冲区并更新时间戳
     * 该方法会：
     * 1. 将缓冲区数据批量写入目标库
     * 2. 清空缓冲区
     * 3. 更新最后刷新时间
     */
    private void flushSnapshotBuffer() throws Exception {
        processBatchSnapshot();
        snapshotBuffer.clear();
        batchSizeManager.setLastFlushTime(System.currentTimeMillis());
    }


    // 处理快照数据
    private void processBatchSnapshot() throws Exception {
        if (snapshotBuffer.isEmpty()) {
            return;
        }

        // 按字段结构分组批量处理
        Map<String, List<KafkaPayload>> groupedByFields = groupByFieldStructure(snapshotBuffer);

        logger.info("开始批量处理 {} 条快照数据，快照数据按字段结构分组: {} 个不同结构", snapshotBuffer.size(),groupedByFields.size());

        // 判断是否需要字段补齐
        boolean needPadding = targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS && shouldPadFields(groupedByFields.size());

        if(needPadding){
            processSameStructureBatch(this.snapshotBuffer,true);

        }else{
            // 每一组字段结构，单独批量
            for (Map.Entry<String, List<KafkaPayload>> entry : groupedByFields.entrySet()) {
                // 每组分别进行批量插入
                processSameStructureBatch(entry.getValue(),false);
            }
        }


    }


    /**
     * 判断是否需要字段补齐（激进策略-适用于宽表场景）
     *
     * 触发条件（需同时满足）：
     * 1. 字段数 >= 50（宽表阈值）
     * 2. 分组数 >= 3（激进策略）
     *
     * @param groupCount 字段结构分组数量
     * @return true-需要补齐, false-按原分组写入
     */
    private boolean shouldPadFields(int groupCount) {

        int columnCount = this.columnInfoList.size();
        // 字段数量检查，分组数检查
         if (columnCount < WIDE_TABLE_COLUMN_THRESHOLD || groupCount < PADDING_GROUP_THRESHOLD) {
            return false;
        }

        // 两个条件都满足,触发补齐
        logger.info("🔧 触发字段补齐: 字段数={} >= 阈值{}, 分组数={} >= 阈值{}",
                columnCount, WIDE_TABLE_COLUMN_THRESHOLD,
                groupCount, PADDING_GROUP_THRESHOLD);
        return true;
    }


    // 按字段结构分组，因为不同的字段结构需要不同的SQL
    private Map<String, List<KafkaPayload>> groupByFieldStructure(List<KafkaPayload> payloads) {
        Map<String, List<KafkaPayload>> grouped = new LinkedHashMap<>();

        for (KafkaPayload payload : payloads) {
            Map<String, Object> afterData = payload.getAfterData();
            if (afterData == null || afterData.isEmpty()) {
                logger.warn("⚠️ 跳过空的afterData记录: offset={}", payload.getOffset());
                continue;
            }

            // 生成字段结构签名（按字段名排序确保一致性）
            String fieldSignature = generateFieldSignature(afterData);

            // 相同字段的，分在同一组
            grouped.computeIfAbsent(fieldSignature, k -> new ArrayList<>()).add(payload);
        }

        return grouped;
    }

    // 生成字段结构签名
    private String generateFieldSignature(Map<String, Object> afterData) {
        return afterData.keySet().stream()
                .sorted() // 排序确保一致性
                .collect(Collectors.joining(","));
    }


    // 处理相同字段结构的批量数据
    private void processSameStructureBatch(List<KafkaPayload> sameStructurePayloads,boolean needPadding) throws Exception {
        if (sameStructurePayloads.isEmpty()) {
            return;
        }

        // 预处理所有数据，为每条数据添加CDC字段
        List<BatchInsertItem> batchItems = new ArrayList<>();

        for (KafkaPayload payload : sameStructurePayloads) {
            Map<String, Object> afterData = new HashMap<>(payload.getAfterData());

            // 添加CDC字段，自定义的时间相关字段，目前都是"yyyy-MM-dd HH:mm:ss" 的字符串，等入库前再转换
            afterData.put("cdc_op", payload.getOp());
            afterData.put("cdc_dml_Time", payload.getDmlTime());
            afterData.put("cdc_kafka_enqueue_time", payload.getKafkaEnqueueTime());
            afterData.put("cdc_insert_time", DateFormatUtils.getCurrentTimeString());

            batchItems.add(new BatchInsertItem(payload, afterData));
        }

        // 执行批量插入
        invokeBatchInsert(batchItems,needPadding);
    }


    private void invokeBatchInsert(List<BatchInsertItem> batchItems,boolean needPadding) throws Exception {
        if (batchItems.isEmpty()) {
            return;
        }

        // 调用策略对象执行批量插入
        Map<Long, String> statusMap = this.insertStrategy.executeBatchInsert(this.tableName, batchItems,this.batchSizeManager,this.columnInfoList,needPadding);

        // 生成日志记录
        for (BatchInsertItem item : batchItems) {
            KafkaPayload payload = item.getPayload();
            Long offset = payload.getOffset();
            String status = statusMap.getOrDefault(offset, "UNKNOWN_ERROR");

            boolean isSuccess = "SUCCESS".equals(status);
            String logDetail = getLogDetail(isSuccess, status);

            // 构建日志对象并加入队列
            logQueue.offer(buildSyncLogEntry(payload, isSuccess, logDetail));

        }

        // 当前阶段默认整批处理结束后即可推进 offset 候选进度
        advanceLatestOffsetCommitCandidateByBatch(batchItems);

    }



    private  String getLogDetail(boolean isSuccess, String status) {
        String logDetail;

        if (isSuccess) {
            logDetail = "批量插入成功";
        } else if (status.startsWith("BINDING_FAILED:")) {
            logDetail = "参数绑定失败: " + status.substring("BINDING_FAILED:".length()).trim();
        } else if (status.startsWith("BATCH_EXECUTE_FAILED:")) {
            logDetail = "批量执行失败: " + status.substring("BATCH_EXECUTE_FAILED:".length()).trim();
        } else if ("EXECUTE_FAILED".equals(status)) {
            logDetail = "数据库执行失败";
        } else if (status.startsWith("CONNECTION_FAILED:")) {
            logDetail = "数据库连接失败: " + status.substring("CONNECTION_FAILED:".length()).trim();
        } else {
            logDetail = "未知错误: " + status;
        }
        return logDetail;
    }

//
//    // Stream Load批量插入
//    private void invokeDorisStreamLoad(List<BatchInsertItem> batchItems) throws Exception {
//        // 批处理开始时间
//        long startTime = System.currentTimeMillis();
//
//        // 解析Doris连接信息
//        DorisConnectionInfo dorisInfo = new DorisConnectionInfo(this.targetDataSourceConfig.getDataSourceHost(),
//                                                               8030,  // 该端口目前写死
//                                                                this.targetDataSourceConfig.getDataSourceServiceName());
//
//
//
//        // Stream Load执行结果
//        // 初始化默认失败结果，避免空指针
//        StreamLoadResult streamLoadResult = new StreamLoadResult(false, 0, 0, "执行前异常");
//
//        try {
//             // 执行 Stream Load
//            streamLoadResult = executeStreamLoad(dorisInfo, batchItems);
//
//            // 根据执行成功与否输出日志
//            if (streamLoadResult.isSuccess()) {
//                int loadedRows = streamLoadResult.getNumberLoadedRows();
//                int filteredRows = streamLoadResult.getNumberFilteredRows();
//                logger.info("Doris Stream Load 成功: 本组记录供 {} 条，实际成功 {} 条, 过滤 {} 条,", batchItems.size(), loadedRows, filteredRows);
//
//            } else {
//                logger.info("Doris Stream Load 失败，错误信息: {}",streamLoadResult.getResponseJson());
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            // 更新失败结果
//            streamLoadResult = new StreamLoadResult(
//                    false,
//                    0,
//                    0,
//                    "执行异常: " + LogCollector.getSafeStackTraceHead(e, 40, 4000));
//
//        }finally {
//
//            // 统一计算执行时间
//            long executionTime = System.currentTimeMillis() - startTime;
//            // 输出执行耗时
//            logger.info("⏱️ Stream Load 执行耗时: {}秒", executionTime / 1000.0);
    ////            logger.info("⏱️ Stream Load 响应内容: {}", streamLoadResult.getResponseJson());
//            // 根据执行性能和数据大小综合调整批次大小
//            batchSizeManager.adjustBatchSizeByPerformance(executionTime, batchItems);
//
//            boolean isSuccess = streamLoadResult.isSuccess();
//            // 为每个KafkaPayload生成对应的SyncLogEntry
//            for (BatchInsertItem item : batchItems) {
//                KafkaPayload payload = item.getPayload();
//
//                // 为每条数据生成独立的日志记录
//                SyncLogEntry logEntry = SyncLogEntry.builder()
//                        .setInstructionType("batch_" + payload.getOp())
//                        .setSourceTime(payload.getDmlTime())
//                        .setTargetTime(LocalDateTime.now())
//                        .setLogDetail(streamLoadResult.getResponseJson())
//                        .setSourceDataSourceId(payload.getBusinessDataSourceId())
//                        .setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId())
//                        .setSourceTableName(payload.getTableName())
//                        .setTargetTableName(payload.getTableName())
//                        .setResultCode(isSuccess ? 0 : 1)
//                        .setOffset(payload.getOffset())
//                        .setKafkaEnqueueTime(payload.getKafkaEnqueueTime())
//                        .setRawMessage(payload.getRawMessage())
//                        .setSourceSchemaName(payload.getSchemaName())
//                        .build();
//
//                logCollector.add(logEntry);
//            }
//
//        }
//    }



    private void processRealTimeData(KafkaPayload row, Collector<OperationResult> out) throws Exception {
        String op = row.getOp();

        switch (op) {
            case "r":
            case "c":
                invokeInsert(row);
                break;
            case "u":
                invokeUpdate(row);
                break;
            case "d":
                invokeDelete(row);
                break;
            case "ddl":
                invokeDDL(row);
                break;
            default:
                logger.warn("⚠️ Unknown operation type: {} skipped", op);
        }

        advanceLatestOffsetCommitCandidate(row.getOffset());
    }

    public void invokeInsert(KafkaPayload row)  {

        boolean success = true;
        String logDetail = "插入成功";
        try {
            this.insertStrategy.executeSingleInsert(this.tableName, row,this.columnInfoList);
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);
        } finally {
            log(row, success, logDetail);
        }

    }



    // invokeUpdate 方法（协调层）
    public void invokeUpdate(KafkaPayload row) {
        boolean success = true;
        String logDetail = "更新成功";

        try {
            // Doris DUPLICATE KEY 表不执行 UPDATE/DELETE 实际写库。
            // 这类表仅保留插入语义，更新和删除在当前链路中记日志后直接跳过。
            if (shouldSkipMutationForDorisDuplicate()) {
                logDetail = "Doris DUPLICATE 表跳过 UPDATE，未执行实际写库";
                logger.info("跳过 UPDATE: targetTable={},", tableName);
                return;
            }

            validatePrimaryKeyValuesPresentForMutation(row);
            // 调用策略执行更新
            this.updateStrategy.executeUpdate(tableName, row);
        } catch (Exception e) {
            success = false;
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);
        } finally {
            log(row, success, logDetail);
        }
    }

    public void invokeDelete(KafkaPayload row) throws Exception {
        boolean success = true;
        String logDetail = "";

        try{

            // Doris DUPLICATE KEY 表不执行 UPDATE/DELETE 实际写库。
            // 这类表仅保留插入语义，更新和删除在当前链路中记日志后直接跳过。
            if (shouldSkipMutationForDorisDuplicate()) {
                logDetail = "Doris DUPLICATE 表跳过 DELETE，未执行实际写库";
                logger.info("跳过 DELETE: targetTable={},", tableName);
                return;
            }

            // 检查消息中是否包含 指定的主键
            validatePrimaryKeyValuesPresentForMutation(row);

            // 如果满足 PIPELINE_SQL_RESULT & 目标库是doris,那么强迫走物理删除
            if (shouldForcePhysicalDeleteForPipelineSqlResult()) {
                this.deleteStrategy.executePhysicalDelete(tableName, row);
            } else {
                this.deleteStrategy.executeDelete(tableName, row);
            }

        } catch (Exception e) {
            e.printStackTrace();
            success = false;
            logDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 2000);
        } finally {
            log(row, success, logDetail);
        }

    }

    /**
     * Doris DUPLICATE KEY 表不执行 UPDATE/DELETE 实际写库。
     * 这类表仅保留插入语义，更新和删除在当前链路中记日志后直接跳过。
     */
    private boolean shouldSkipMutationForDorisDuplicate() {
        return this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS
                && this.tableModel == DorisTableModel.DUPLICATE;
    }

    public void invokeDDL(KafkaPayload row) throws Exception {
        // 获取DDL执行策略


        // 原始DDL语句
        String ddlStatement = row.getDdlStatement();

        // 解析具体的字段变更信息
        ColumnChangeInfo columnChange = parseOracleColumnDDL(ddlStatement);

        boolean success = true;
        String logDetail = "DDL执行成功";

        try {
            if (columnChange == null) {
                logger.warn("⚠️ 无法解析DDL语句: {}", ddlStatement);
                success = false;
                logDetail = "DDL解析失败：无法识别的DDL语句格式";
                return;
            }

            // 记录DDL操作信息
            logger.info("🔧 开始执行DDL操作: 表={}, 操作={}, 字段={}",
                    columnChange.getTableName(),
                    columnChange.getOperationType(),
                    columnChange.getColumnName());

            // 根据操作类型调用相应的策略方法
            switch (columnChange.getOperationType().toUpperCase()) {
                case "ADD":
                    this.ddlStrategy.handleAddColumn(columnChange);
                    logDetail = String.format("新增字段成功: %s %s",
                            columnChange.getColumnName(),
                            columnChange.getColumnType());
                    break;

                case "MODIFY":
                    this.ddlStrategy.handleModifyColumn(columnChange);
                    logDetail = String.format("修改字段成功: %s %s",
                            columnChange.getColumnName(),
                            columnChange.getColumnType());
                    break;

                case "DROP":
                    this.ddlStrategy.handleDropColumn(columnChange);
                    logDetail = String.format("删除字段成功: %s", columnChange.getColumnName());
                    break;

                case "RENAME":
                    this.ddlStrategy.handleRenameColumn(columnChange);
                    logDetail = String.format("重命名字段成功: %s -> %s",
                            columnChange.getOldColumnName(),
                            columnChange.getColumnName());
                    break;

                default:
                    logger.warn("⚠️ 不支持的DDL操作类型: {}", columnChange.getOperationType());
                    success = false;
                    logDetail = "不支持的DDL操作类型: " + columnChange.getOperationType();
            }

            if (success) {
                logger.info("✅ DDL操作执行成功: {}", logDetail);
            }

        } catch (Exception e) {
            e.printStackTrace();
            success = false;
//            logDetail = LogCollector.getSafeStackTraceHead(e, 40, 4000);
            logger.error("❌ DDL操作执行失败: {}", ddlStatement, e);
//            throw e;
        } finally {
//            log(row, success, logDetail);
        }
    }




    /**
     * 使用Druid 1.2.20解析Oracle表字段DDL语句
     * @param ddlStatement Oracle DDL语句
     * @return 字段变更信息
     */
    public  ColumnChangeInfo parseOracleColumnDDL(String ddlStatement) {
        if (ddlStatement == null || ddlStatement.trim().isEmpty()) {
            return null;
        }

        try {
            // 使用Druid解析Oracle SQL
            OracleStatementParser parser = new OracleStatementParser(ddlStatement);
            SQLStatement statement = parser.parseStatement();

            if (statement instanceof SQLAlterTableStatement) {
                SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;


                // 获取所有ALTER操作项
                List<SQLAlterTableItem> items = alterTable.getItems();

                if (items != null && !items.isEmpty()) {
                    SQLAlterTableItem item = items.get(0); // 处理第一个操作项

                    if (item instanceof SQLAlterTableAddColumn) {
                        // ADD COLUMN操作
                        SQLAlterTableAddColumn addItem = (SQLAlterTableAddColumn) item;
                        if (!addItem.getColumns().isEmpty()) {
                            SQLColumnDefinition column = addItem.getColumns().get(0);

                            String columnName = column.getColumnName();
                            String columnType = column.getDataType().toString();
                            return new ColumnChangeInfo(this.tableName,columnName, columnType, "ADD");
                        }

                    } else if (item instanceof SQLAlterTableDropColumnItem) {
                        // DROP COLUMN操作
                        SQLAlterTableDropColumnItem dropItem = (SQLAlterTableDropColumnItem) item;
                        if (!dropItem.getColumns().isEmpty()) {
                            String columnName = dropItem.getColumns().get(0).getSimpleName();
                            return new ColumnChangeInfo(this.tableName,columnName, null, "DROP");
                        }

                    } else if (item instanceof OracleAlterTableModify) {
                        // MODIFY COLUMN操作
                        OracleAlterTableModify oracleAlterTableModify = (OracleAlterTableModify) item;
                        String columnName = oracleAlterTableModify.getColumns().get(0).getColumnName();
                        String columnType = oracleAlterTableModify.getColumns().get(0).getDataType() != null ?
                                oracleAlterTableModify.getColumns().get(0).getDataType().toString() : null;
                        return new ColumnChangeInfo(this.tableName,columnName, columnType, "MODIFY");

                    } else if (item instanceof SQLAlterTableRenameColumn) {
                        // RENAME COLUMN操作
                        SQLAlterTableRenameColumn renameItem = (SQLAlterTableRenameColumn) item;
                        String oldColumnName = renameItem.getColumn().getSimpleName();
                        String newColumnName = renameItem.getTo().getSimpleName();
                        return new ColumnChangeInfo(this.tableName,newColumnName, null, "RENAME", oldColumnName);
                    }

                    logger.debug("🔍 未识别的ALTER操作类型: {}", item.getClass().getSimpleName());
                }

                logger.info("🔍 使用Druid解析DDL成功: {}", ddlStatement);
            }

        } catch (Exception e) {
            logger.error("❌ Druid解析Oracle DDL失败: {}", ddlStatement, e);
        }

        return null;
    }

    public static void main(String[] args) {
        // 测试各种DDL语句
        String[] testDDLs = {
                "ALTER TABLE test_table ADD column_name VARCHAR2(100)",
                "ALTER TABLE test_table MODIFY column_name NUMBER(10,2)",
                "ALTER TABLE test_table DROP COLUMN column_name",
                "ALTER TABLE test_table RENAME COLUMN old_name TO new_name"
        };

        for (String ddl : testDDLs) {
//            System.out.println("DDL: " + ddl);
////            ColumnChangeInfo result = parseOracleColumnDDL(ddl);
//            System.out.println("Result: " + result);
//            System.out.println("---");
        }
    }
    private void log(KafkaPayload payload, boolean success, String logDetail) {
        SyncLogEntry syncLogEntry = buildSyncLogEntry(payload, success, logDetail);

        long methodStartTime = System.currentTimeMillis();
        // 使用共享的日志收集器
        logQueue.offer(syncLogEntry);
//        logger.info("⏱️ logCollector.add(syncLogEntry);---------->方法已耗时={}ms",
//                System.currentTimeMillis() - methodStartTime);
    }

    private SyncLogEntry buildSyncLogEntry(KafkaPayload payload, boolean success, String logDetail) {
        SyncLogEntry syncLogEntry = SyncLogEntry.builder()
                .setInstructionType(payload.getOp())              // 操作类型
                .setSourceTime(payload.getDmlTime())              // dml事务提交时间
                .setTargetTime(LocalDateTime.now().format(DateTimeConstants.FMT_DATETIME))               // 当前时间，近似落盘到目标库的时间
                .setLogDetail(logDetail)                          // 日志详情
                .setSourceDataSourceId(payload.getBusinessDataSourceId())               // 源库的 数据源 id
                .setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId())   //目标库  数据源 id
                .setSourceTableName(payload.getTableName())      // 源表名
                .setTargetTableName(this.tableName)              // 目标库表名
                .setResultCode(success ? 0 : 1)                  // 是否执行成功
                .setOffset(payload.getOffset())                  // Kafka offset
                .setKafkaEnqueueTime(payload.getKafkaEnqueueTime())  // 数据进入Kafka的时间（数据进入链路）
                .setRawMessage( shouldStoreRawMessage(success) ? payload.getRawMessage() : null)
                .setSourceSchemaName(payload.getSchemaName())    // 源-模式名
                .build();
        return syncLogEntry;
    }

    /**
     * 判断是否需要存储原始消息
     * @param success 操作是否成功
     * @return true-需要存储, false-不存储
     */
    private boolean shouldStoreRawMessage(boolean success) {
        // 失败时总是存储，或策略配置为非NONE时存储
        return !success || this.serverInfo.getRawMessageStorageStrategy() != RawMessageStorageStrategy.NONE;
    }

    @Override
    public void close() throws Exception {
        // 1. 清理本地引用
//        this.logCollector = null;

        // 2. 停止定时任务
//        shutdownFlushExecutor();

    }



    /** 根据目标库配置构造一个保守的 HikariConfig（不在 open 之外共享/序列化） */
    private HikariConfig buildHikariConfig(TargetDataSourceConfig t) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(t.getUrl());
        c.setUsername(t.getDataSourceUserName());
        c.setPassword(t.getDataSourcePassword());

        DatabaseType tp = t.getDataSourceType();
        if (tp == DatabaseType.MYSQL) {
            c.setDriverClassName("com.mysql.cj.jdbc.Driver");
            c.setMaximumPoolSize(100);
            c.setMinimumIdle(0);
            c.setInitializationFailTimeout(-1); // 初始化拿不到连接也不失败
            c.setConnectionTimeout(30000);
            c.setValidationTimeout(5000);
            c.setIdleTimeout(600000);
            c.setMaxLifetime(1800000);
            c.setConnectionTestQuery("SELECT 1");
            c.addDataSourceProperty("useUnicode", "true");
            c.addDataSourceProperty("characterEncoding", "utf8");
            c.addDataSourceProperty("useSSL", "false");
            c.addDataSourceProperty("allowMultiQueries", "true");
        } else if (tp == DatabaseType.ORACLE) {
            c.setDriverClassName("oracle.jdbc.OracleDriver");
            c.setMaximumPoolSize(100);          // 单 TM 1~2 条连接足够
            c.setMinimumIdle(0);
            c.setInitializationFailTimeout(-1);
            c.setConnectionTimeout(60000);
            c.setValidationTimeout(5000);
            c.setIdleTimeout(600000);
            c.setMaxLifetime(1800000);
            c.setConnectionTestQuery("SELECT 1 FROM DUAL");
            // Oracle 网络超时
            c.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "60000");
            c.addDataSourceProperty("oracle.jdbc.ReadTimeout", "60000");
        }

        c.setPoolName(tp + "-" + t.getDataSourceUserName());
        return c;
    }


    /**
     * 生成随机的初始延迟时间（30-60秒）
     * 避免多个实例同时触发定时检查
     * @return 随机延迟秒数
     */
    private int generateRandomInitialDelay() {
        Random random = new Random();
        return 30 + random.nextInt(30); // 45 + [0,15] = [45,60]
    }


    private MonitorMetricEvent buildMonitorOffsetEventIfDue(long rowOffset, OffsetPhase phase) {
        // monitorOffset 为近似值：快照/实时都允许按节流策略上报
        long now = System.currentTimeMillis();

        if (phase == OffsetPhase.SNAPSHOT) {
            latestSnapshotOffset = Math.max(latestSnapshotOffset, rowOffset);
            snapshotRowsSinceLastReport++;

            boolean hitRows = snapshotRowsSinceLastReport >= SNAPSHOT_REPORT_ROWS;
            boolean hitTime = (now - lastSnapshotReportTs) >= SNAPSHOT_REPORT_INTERVAL_MS;
            if (hitRows || hitTime) {
                lastSnapshotReportTs = now;
                snapshotRowsSinceLastReport = 0;
                return buildOffsetMetricEvent(latestSnapshotOffset);
            }
            return null;
        }

        // REALTIME：每条推进内存 latest，按时间/条数节流上报
        latestRealtimeOffset = Math.max(latestRealtimeOffset, rowOffset);
        realtimeRowsSinceLastReport++;

        boolean hitRows = realtimeRowsSinceLastReport >= REALTIME_REPORT_ROWS;
        boolean hitTime = (now - lastRealtimeReportTs) >= REALTIME_REPORT_INTERVAL_MS;
        if (hitRows || hitTime) {
            lastRealtimeReportTs = now;
            realtimeRowsSinceLastReport = 0;
            return buildOffsetMetricEvent(latestRealtimeOffset);
        }
        return null;
    }



    private MonitorMetricEvent buildOffsetMetricEvent(long offset) {
        MonitorMetricEvent event = MonitorMetricEvent.offsetEvent(
                this.serverInfo.getServerId(),
                this.serverTopicInfo.getTopic(),
                this.targetDataSourceConfig.getDataSourceId(),
                this.tableName,
                offset
        );
        event.validate();
        return event;
    }





    private TopicOffsetCommitEvent buildOffsetCommitEventIfDue() {
        // 还没有任何已持久化成功的 offset 时，不输出事件
        if (latestOffsetCommitCandidate < 0L) {
            return null;
        }

        // 如果候选进度没有比上次已输出进度更大，则不重复输出
        if (latestOffsetCommitCandidate <= lastReportedOffsetCommitCandidate) {
            return null;
        }
//
//        // 计算当前时间
//        long now = System.currentTimeMillis();
//
//        // 时间到阈值则触发一次
//        boolean hitTime = (now - lastOffsetCommitReportTs) >= OFFSET_COMMIT_REPORT_INTERVAL_MS;
//
//        // 成功条数到阈值则触发一次
//        boolean hitRows = offsetCommitRowsSinceLastReport >= OFFSET_COMMIT_REPORT_ROWS;
//
//        // 任一条件满足都触发
//        if (hitTime || hitRows) {
//            // 更新最近一次发出时间
//            lastOffsetCommitReportTs = now;
//
//            // 清空本轮计数，开始下一轮累计
//            offsetCommitRowsSinceLastReport = 0;
//
//            // 记录本次已输出到侧输出流的最大 offset
//            lastReportedOffsetCommitCandidate = latestOffsetCommitCandidate;
//
//            // 保存的是 nextOffset，因此需要 +1
//            long nextOffset = latestOffsetCommitCandidate + 1L;
//
//            // 构造 offset commit 事件
//            return TopicOffsetCommitEvent.of(
//                    this.serverInfo.getServerId(),
//                    this.serverTopicInfo.getTopic(),
//                    nextOffset
//            );
//        }

        // 记录本次已输出到侧输出流的最大 offset
        lastReportedOffsetCommitCandidate = latestOffsetCommitCandidate;

        // 保存的是 nextOffset，因此需要 +1
        long nextOffset = latestOffsetCommitCandidate + 1L;

        // 构造 offset commit 事件
        return TopicOffsetCommitEvent.of(
                this.serverInfo.getServerId(),
                this.serverTopicInfo.getTopic(),
                nextOffset
        );


//
//
//        // 未达到触发条件则不发事件
//        return null;
    }
    // PIPELINE_SQL_RESULT + Doris UNIQUE 表时，从目标表读取到的 UNIQUE KEY
    private void initPipelineSqlResultTargetUniqueKeys() throws Exception {
        TopicSourceKind sourceKind = resolveSourceKind();

        if (sourceKind != TopicSourceKind.PIPELINE_SQL_RESULT) {
            return;
        }

        if (this.targetDataSourceConfig == null) {
            throw new IllegalStateException(
                    "PIPELINE_SQL_RESULT missing targetDataSourceConfig, table=" + this.tableName
            );
        }

        if (this.targetDataSourceConfig.getDataSourceType() != DatabaseType.DORIS) {
            throw new IllegalStateException(
                    "PIPELINE_SQL_RESULT currently requires Doris target, table=" + this.tableName
                            + ", dbType=" + this.targetDataSourceConfig.getDataSourceType()
            );
        }

        if (this.tableModel != DorisTableModel.UNIQUE) {
            throw new IllegalStateException(
                    "PIPELINE_SQL_RESULT requires Doris UNIQUE table model, table=" + this.tableName
                            + ", tableModel=" + this.tableModel
            );
        }

        LinkedHashSet<String> resolvedKeys = this.selectStrategy.getTargetUniqueKeys(this.tableName);
        if (resolvedKeys == null || resolvedKeys.isEmpty()) {
            throw new IllegalStateException(
                    "PIPELINE_SQL_RESULT target table has no readable UNIQUE KEY, table=" + this.tableName
            );
        }

        this.targetUniqueKeySet = resolvedKeys;
        validateTargetUniqueKeysExistInTargetColumns();

        logger.info(
                "Initialized PIPELINE_SQL_RESULT target UNIQUE KEY, table={}, keys={}",
                this.tableName,
                this.targetUniqueKeySet
        );
    }

    private void validateTargetUniqueKeysExistInTargetColumns() {
        if (this.columnInfoList == null || this.columnInfoList.isEmpty()) {
            throw new IllegalStateException("columnInfoList is empty, table=" + this.tableName);
        }

        Set<String> targetColumns = new LinkedHashSet<>();
        for (ColumnInfo columnInfo : this.columnInfoList) {
            if (columnInfo == null || columnInfo.getColumnName() == null) {
                continue;
            }
            targetColumns.add(columnInfo.getColumnName().trim().toLowerCase(Locale.ROOT));
        }

        LinkedHashSet<String> missingKeys = new LinkedHashSet<>();
        for (String key : this.targetUniqueKeySet) {
            if (!targetColumns.contains(key)) {
                missingKeys.add(key);
            }
        }

        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Resolved Doris UNIQUE KEY contains columns missing from target schema, table=" + this.tableName
                            + ", missingKeys=" + missingKeys
                            + ", targetColumns=" + targetColumns
            );
        }
    }

    public LinkedHashSet<String> getTargetUniqueKeySet() {
        return new LinkedHashSet<>(this.targetUniqueKeySet);
    }


    // 唯一键的规约化处理,

    /**
          唯一键的规约化处理:
          1.如果满足 PIPELINE_SQL_RESULT & DorisTableModel.UNIQUE,那么使用 目标表探测得到的 主键
          2.单表CDC，固定使用：CDC_ROWID_COLUMN_LOWER

     */
    private void normalizePrimaryKeysForWrite(KafkaPayload row) {
        LinkedHashSet<String> effectiveKeys = resolveEffectivePrimaryKeys();
        row.setPrimaryKeySet(new LinkedHashSet<>(effectiveKeys));
    }

    // 提取有效的主键
    private LinkedHashSet<String> resolveEffectivePrimaryKeys() {
        // 如果满足 PIPELINE_SQL_RESULT & DorisTableModel.UNIQUE,那么使用从 表探测得到的 主键
        if (shouldUseTargetUniqueKeyForPipelineSqlResult()) {
            if (this.targetUniqueKeySet == null || this.targetUniqueKeySet.isEmpty()) {
                throw new IllegalStateException(
                        "PIPELINE_SQL_RESULT target UNIQUE KEY is empty, table=" + this.tableName
                );
            }
            return new LinkedHashSet<>(this.targetUniqueKeySet);
        }

        // 单表CDC，固定使用：CDC_ROWID_COLUMN_LOWER
        LinkedHashSet<String> fallbackKeys = new LinkedHashSet<>();
        fallbackKeys.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
        return fallbackKeys;
    }

    // 检查消息中是否包含 指定的主键
    private void validatePrimaryKeyValuesPresentForMutation(KafkaPayload row) {
        Set<String> effectiveKeys = row.getPrimaryKeySet();
        if (effectiveKeys == null || effectiveKeys.isEmpty()) {
            throw new IllegalStateException("effectiveKeys is empty, table=" + this.tableName);
        }

        String op = row.getOp();
        Map<String, Object> data = "d".equalsIgnoreCase(op) ? row.getBeforeData() : row.getAfterData();

        if (data == null) {
            throw new IllegalStateException(
                    "payload data is null for op=" + op + ", table=" + this.tableName
            );
        }

        LinkedHashSet<String> missingKeys = new LinkedHashSet<>();
        for (String key : effectiveKeys) {
            Object value = findValueIgnoreCase(data, key);
            if (value == null) {
                missingKeys.add(key);
            }
        }

        if (!missingKeys.isEmpty()) {
            throw new IllegalStateException(
                    "missing primary key values for mutation, table=" + this.tableName
                            + ", op=" + op
                            + ", keys=" + effectiveKeys
                            + ", missingKeys=" + missingKeys
                            + ", payloadFields=" + data.keySet()
            );
        }
    }

    private Object findValueIgnoreCase(Map<String, Object> data, String key) {
        if (data == null || key == null) {
            return null;
        }

        Object direct = data.get(key);
        if (direct != null) {
            return direct;
        }

        String keyLower = key.toLowerCase(Locale.ROOT);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getKey() != null
                    && entry.getKey().toLowerCase(Locale.ROOT).equals(keyLower)) {
                return entry.getValue();
            }
        }
        return null;
    }

    // 如果满足 PIPELINE_SQL_RESULT & DorisTableModel.UNIQUE,那么使用从 表探测得到的 主键
    private boolean shouldUseTargetUniqueKeyForPipelineSqlResult() {
        return resolveSourceKind() == TopicSourceKind.PIPELINE_SQL_RESULT
                && this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS
                && this.tableModel == DorisTableModel.UNIQUE;
    }

    // 如果满足 PIPELINE_SQL_RESULT & 目标库是doris,那么强迫走物理删除
    private boolean shouldForcePhysicalDeleteForPipelineSqlResult() {
        return resolveSourceKind() == TopicSourceKind.PIPELINE_SQL_RESULT
                && this.targetDataSourceConfig != null
                && this.targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS;
    }










    private synchronized void advanceLatestOffsetCommitCandidate(Long rowOffset) {
        // 空 offset 不参与推进
        if (rowOffset == null) {
            return;
        }

        // 推进当前 topic 已确认完成持久化的最大 offset
        latestOffsetCommitCandidate = Math.max(latestOffsetCommitCandidate, rowOffset);

//        // 成功条数加一，用于后续节流判断
//        offsetCommitRowsSinceLastReport++;
    }

    private synchronized void advanceLatestOffsetCommitCandidateByBatch(List<BatchInsertItem> batchItems) {
        // 批量列表为空则直接返回
        if (batchItems == null || batchItems.isEmpty()) {
            return;
        }

        long maxOffset = -1L;
        int successCount = 0;

        // 当前阶段默认整批处理完成后即可推进 offset
        for (BatchInsertItem item : batchItems) {
            KafkaPayload payload = item.getPayload();
            if (payload == null || payload.getOffset() == null) {
                continue;
            }

            maxOffset = Math.max(maxOffset, payload.getOffset());
            successCount++;
        }

        // 没有有效 offset 时不推进
        if (successCount == 0) {
            return;
        }

        // 批量推进当前 topic 已确认完成持久化的最大 offset
        latestOffsetCommitCandidate = Math.max(latestOffsetCommitCandidate, maxOffset);

//        // 成功条数累加，用于后续节流判断
//        offsetCommitRowsSinceLastReport += successCount;
    }
}
