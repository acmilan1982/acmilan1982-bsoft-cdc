package com.bsoft.server.sql.insert;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.*;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.dataSource.HikariMonitor;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.adapter.OracleAdapter;

import com.bsoft.server.sql.doris.DorisCustomColumnMappingSupport;
import com.bsoft.server.sql.update.AbstractUpdateStrategy;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

// 插入策略基类，执行单条及同结构批量插入，并传递日期字段单位。
public abstract class AbstractInsertStrategy implements InsertStrategy {


    private static final Logger logger = LogManager.getLogger(AbstractInsertStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected  DatabaseAdapter adapter;


    public AbstractInsertStrategy() {
    }

    protected AbstractInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }

    // 执行 Oracle、MySQL 批量新增，按输入位置返回处理状态。
// 上游保证字段结构和日期单位一致，整批使用一个 SQL，不在本方法内拆批。
    @Override
    public List<String> executeBatchInsertOrdered(
            String tableName,
            List<BatchInsertItem> batchItems,
            BatchSizeManager batchSizeManager,
            List<ColumnInfo> allColumns,
            boolean needPadding) throws Exception {
        // 校验输入，空批次返回空结果。
        Objects.requireNonNull(batchItems, "新增批次不能为 null");
        if (batchItems.isEmpty()) {
            return new ArrayList<>();
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("新增的目标表名不能为空");
        }

        // 检查策略初始化状态。
        if (dataSourceHolder == null
                || dataSourceHolder.getTargetDataSourceConfig() == null
                || adapter == null) {
            throw new IllegalStateException("JDBC 新增策略尚未初始化");
        }

        // 防止 Doris 尚未重写新入口时误走 JDBC。
        DatabaseType databaseType = dataSourceHolder.getTargetDataSourceConfig().getDataSourceType();
        if (databaseType != DatabaseType.ORACLE && databaseType != DatabaseType.MYSQL) {
            throw new UnsupportedOperationException(
                    "当前 JDBC 批量新增入口不支持: " + databaseType);
        }

        // 确保每个输入位置都有有效消息，不能通过删除列表元素跳过。
        for (BatchInsertItem item : batchItems) {
            if (item == null || item.getPayload() == null || item.getData() == null) {
                throw new IllegalArgumentException("新增批次包含空 item、payload 或 data");
            }
        }

        // 沿用同结构批次约定，字段顺序只确定一次。
        BatchInsertItem firstItem = batchItems.get(0);
        List<String> fieldList = new ArrayList<>(firstItem.getData().keySet());
        Collections.sort(fieldList);

        // 复制日期元数据，补充服务端 CDC 时间字段。
        Set<String> dateFieldSet = new HashSet<>(Objects.requireNonNull(
                firstItem.getPayload().getDateFieldSet(),
                "dateFieldSet 不能为 null"));
        appendCdcTimeFields(dateFieldSet);

        // 整批沿用首条记录的明确日期单位，不猜测单位。
        Map<String, String> dateFieldUnits = new LinkedHashMap<>(
                Objects.requireNonNull(
                        firstItem.getPayload().getDateFieldUnits(),
                        "dateFieldUnits 不能为 null"));
        Set<String> primaryKeys = new LinkedHashSet<>(
                Objects.requireNonNull(
                        firstItem.getPayload().getPrimaryKeySet(),
                        "primaryKeySet 不能为 null"));

        // SQL 只构建一次，与整批参数绑定共用字段顺序和日期元数据。
        String sql = buildInsertSql(
                tableName,
                fieldList,
                primaryKeys,
                dateFieldSet,
                dateFieldUnits);

        // 每个输入位置预留一个结果，禁止使用 offset 作为结果标识。
        List<String> statuses = new ArrayList<>(
                Collections.nCopies(batchItems.size(), "NOT_EXECUTED"));

        // 保存已加入 JDBC 批次的原始位置，用于准确回填结果。
        List<Integer> boundIndexes = new ArrayList<>(batchItems.size());


        // 记录批次开始时间，用于统计准备、获取连接、绑定、执行及提交的总耗时。
        long startNanos = System.nanoTime();


        // 整批使用一个连接和一个事务。
        try (Connection connection = dataSourceHolder.getDataSource().getConnection()) {
            // 禁止自动提交，执行结果确认后再统一提交。
            connection.setAutoCommit(false);

            try {
                // 所有成功绑定的记录共用同一个 PreparedStatement。
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (int itemIndex = 0; itemIndex < batchItems.size(); itemIndex++) {
                        BatchInsertItem item = batchItems.get(itemIndex);

                        // 清理上一条参数，清理失败时终止整个批次。
                        statement.clearParameters();

                        // 单条绑定失败保留原始结果位置，不影响其他位置的关联。
                        try {
                            setParameters(
                                    statement,
                                    item.getData(),
                                    fieldList,
                                    dateFieldSet,
                                    dateFieldUnits);
                        } catch (Exception bindingException) {
                            // 记录失败摘要，原始消息由 writer 按日志规则保存。
                            statuses.set(
                                    itemIndex,
                                    "BINDING_FAILED: "
                                            + ExceptionDetailUtil.buildRootCauseDetail(
                                            bindingException, 2000));
                            continue;
                        }

                        // addBatch 失败不按普通绑定失败跳过，避免批次状态不确定。
                        statement.addBatch();
                        boundIndexes.add(itemIndex);
                    }

                    // 至少一条绑定成功时才执行数据库批次。
                    if (!boundIndexes.isEmpty()) {
                        int[] results = statement.executeBatch();

                        // 返回数量必须与实际加入 JDBC 批次的命令数量一致。
                        if (results == null || results.length != boundIndexes.size()) {
                            throw new SQLException(
                                    "批量新增返回结果数量与绑定成功数量不一致");
                        }

                        // 当前是普通 INSERT，零行或负数失败状态不能视为成功。
                        for (int resultIndex = 0; resultIndex < results.length; resultIndex++) {
                            int affectedRows = results[resultIndex];
                            if (affectedRows != Statement.SUCCESS_NO_INFO
                                    && affectedRows <= 0) {
                                throw new SQLException(
                                        "批量新增命令失败，输入索引="
                                                + boundIndexes.get(resultIndex)
                                                + "，返回值=" + affectedRows);
                            }
                        }
                    }
                }

                // Statement 正常关闭后提交，提交前不标记 SUCCESS。
                connection.commit();

                // 只回填实际加入 JDBC 批次的输入位置。
                for (Integer itemIndex : boundIndexes) {
                    statuses.set(itemIndex, "SUCCESS");
                }



                // 提交成功后统计结果，绑定失败的记录不计入成功数量。
                int successCount = boundIndexes.size();
                String batchResult = successCount == batchItems.size()
                        ? "SUCCESS"
                        : (successCount > 0 ? "PARTIAL_FAILURE" : "FAILED");

                // 总耗时以秒为单位，保留三位小数。
                String elapsedSeconds = String.format(
                        Locale.ROOT,
                        "%.3f",
                        (System.nanoTime() - startNanos) / 1_000_000_000.0);

                // 仅在正常提交后打印汇总，不重复记录异常原因。
                logger.info(
                        "批量插入完成，table={}，输入条数={}，成功条数={}，总耗时={}秒，结果={}",
                        tableName,
                        batchItems.size(),
                        successCount,
                        elapsedSeconds,
                        batchResult);
            } catch (Exception exception) {
                // 执行或提交异常时尝试回滚，禁止返回可能误导的成功状态。
                try {
                    connection.rollback();
                } catch (Exception rollbackException) {
                    // 保留最初异常，并附加回滚异常。
                    exception.addSuppressed(rollbackException);
                }
                throw exception;
            }
        }

        // 结果长度和输入长度一致，绑定失败的位置也保留。
        return statuses;
    }
    // oracle,mysql走当前方法
    // 上游已保证当前批次 结构和日期单位一致，整批使用同一个插入 SQL。
    @Override
    public Map<Long, String> executeBatchInsert(String tableName,                   // 目标表名
                                                List<BatchInsertItem> batchItems,
                                                BatchSizeManager batchSizeManager,
                                                List<ColumnInfo> allColumns,        // 目标表的列信息
                                                boolean needPadding) throws Exception {

        if (batchItems.isEmpty()) {
            return Collections.emptyMap();
        }
        // 记录开始时间
        long methodStartTime = System.currentTimeMillis();

        // 使用第一条数据的字段结构
        BatchInsertItem firstItem = batchItems.get(0);
        List<String> fieldList = new ArrayList<>(firstItem.getData().keySet());
        Collections.sort(fieldList);



        // 复制首条记录的日期字段集合，补充 CDC 字段时不修改原消息。
        Set<String> dateFieldSet = new HashSet<>(firstItem.getPayload().getDateFieldSet());
        appendCdcTimeFields(dateFieldSet);

        // 上游保证同批次日期单位一致，整批共用首条记录的单位映射。
        Map<String, String> dateFieldUnits = firstItem.getPayload().getDateFieldUnits();


        Set<String> primaryKeys = firstItem.getPayload().getPrimaryKeySet();



        // 构建批量插入SQL
        String sql = buildInsertSql(tableName,
                                    fieldList,
                                    primaryKeys,
                                    dateFieldSet,
                                    dateFieldUnits
        );

        // 记录每条数据的处理状态，value为状态码或错误信息，整个生命周期中，value可能会发生多次变化
        Map<Long, String> statusMap = new HashMap<>(); // offset -> status/error

        // 参数绑定成功的计数
        int successfullyBoundCount = 0;

        long startTime1 = System.currentTimeMillis();



        long connectionStartTime = System.currentTimeMillis();
//
//        //  增加超时前的状态快照
//        if (this.dataSourceHolder.getDataSource() instanceof HikariDataSource) {
//            HikariDataSource ds = (HikariDataSource) this.dataSourceHolder.getDataSource();
//            logger.info("⏱ 准备获取连接: 池名={}, 活动={}, 空闲={}, 总数={}, 等待线程={}, 方法已耗时={}ms",
//                    ds.getPoolName(),
//                    ds.getHikariPoolMXBean().getActiveConnections(),
//                    ds.getHikariPoolMXBean().getIdleConnections(),
//                    ds.getHikariPoolMXBean().getTotalConnections(),
//                    ds.getHikariPoolMXBean().getThreadsAwaitingConnection(),
//                    System.currentTimeMillis() - methodStartTime);
//        }

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection()) {

//            long connectionAcquireTime = System.currentTimeMillis() - connectionStartTime;
//
//            //  详细记录连接获取信息
//            logger.info(" 成功获取连接: 耗时={}ms, 表={}, 批次={}, 线程={}",
//                    connectionAcquireTime, tableName, batchItems.size(), Thread.currentThread().getName());

            //  测试连接有效性
            long pingStartTime = System.currentTimeMillis();
            if (conn.isValid(5)) {  // 5秒超时
                long pingTime = System.currentTimeMillis() - pingStartTime;
                logger.debug("🏓 连接有效性测试: 耗时={}ms", pingTime);

                if (pingTime > 1000) {
                    logger.warn("⚠️ 连接有效性测试耗时过长: {}ms (可能是网络延迟或数据库响应慢)", pingTime);
                }
            } else {
                logger.error("❌ 连接无效!");
            }

            double executionTimeSeconds1 = (System.currentTimeMillis() - startTime1) / 1000.0;

            conn.setAutoCommit(false);


            try (PreparedStatement ps = conn.prepareStatement(sql)) {

                long startTime2 = System.currentTimeMillis();
                // step1:参数绑定阶段
                for (BatchInsertItem item : batchItems) {
                    // 当前消息在topic上的偏移量
                    Long offset = item.getPayload().getOffset();
                    try {
                        // 使用与本批次 SQL 一致的日期元数据绑定当前记录。
                        setParameters(
                                ps,
                                item.getData(),
                                fieldList,
                                dateFieldSet,
                                dateFieldUnits
                        );
                        ps.addBatch();
                        successfullyBoundCount++;
                        statusMap.put(offset, "BOUND_SUCCESS"); // 绑定成功
                    } catch (Exception e) {
                        logger.warn("参数绑定失败，跳过: offset={}, error={}", offset, e.getMessage());
                        statusMap.put(offset, "BINDING_FAILED: " + e.getMessage()); // 绑定失败
                    }

                }
                double executionTimeSeconds2 = (System.currentTimeMillis() - startTime2) / 1000.0;

//                long globalCommitGateStartTime = System.currentTimeMillis();
//                GlobalCommitGate.COMMIT.acquire();
//                double globalCommitGateExecutionTimeSeconds = (System.currentTimeMillis() - globalCommitGateStartTime) / 1000.0;


                long startTime3 = System.currentTimeMillis();
                // step2:数据库执行阶段
                if (successfullyBoundCount > 0) {
                    try {
                        int[] results = ps.executeBatch();

                        // 更新成功绑定数据的执行状态
                        int resultIndex = 0;
                        for (BatchInsertItem item : batchItems) {
                            Long offset = item.getPayload().getOffset();
                            if ("BOUND_SUCCESS".equals(statusMap.get(offset))) {
                                if (resultIndex < results.length) {
                                    if (results[resultIndex] == PreparedStatement.EXECUTE_FAILED) {
                                        statusMap.put(offset, "EXECUTE_FAILED");
                                    } else {
                                        statusMap.put(offset, "SUCCESS");
                                    }
                                    resultIndex++;
                                }
                            }
                        }


                        conn.commit();

                        // 过滤掉失败的结果再求和
                        int actualInserted = Arrays.stream(results)
                                .filter(result -> result != PreparedStatement.EXECUTE_FAILED)
                                .map(result -> result == PreparedStatement.SUCCESS_NO_INFO ? 1 : Math.max(0, result))
                                .sum();

                        long failedCount = Arrays.stream(results)
                                .filter(result -> result == PreparedStatement.EXECUTE_FAILED)
                                .count();



                        double executionTimeSeconds3 = (System.currentTimeMillis() - startTime3) / 1000.0;
                        logger.info("Oracle: 批量插入完成: 本组记录供 {} 条，实际成功 {} 条, 执行失败 {} 条, 绑定失败 {} 条,获取链接耗时{} S，绑定参数耗时 {} S， 执行ps.executeBatch()耗时 {} S",
                                batchItems.size(),actualInserted, failedCount, batchItems.size() - successfullyBoundCount,
                                executionTimeSeconds1,
                                executionTimeSeconds2,
                                executionTimeSeconds3);

                    } catch (Exception e) {
                        e.printStackTrace();
                        conn.rollback();
                        // 批量执行失败，更新所有绑定成功的数据状态
                        for (BatchInsertItem item : batchItems) {
                            Long offset = item.getPayload().getOffset();
                            if ("BOUND_SUCCESS".equals(statusMap.get(offset))) {
                                statusMap.put(offset, "BATCH_EXECUTE_FAILED: " + e.getMessage());
                            }
                        }
                        throw e;
                    }
                }

            } catch (Exception e) {
                throw e;
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("❌ 批量插入失败: {}", e.getMessage());


            long connectionFailTime = System.currentTimeMillis() - connectionStartTime;

            long totalWaitTime = System.currentTimeMillis() - connectionStartTime;

            // ✅ 详细记录超时信息
            if (e instanceof java.sql.SQLTransientConnectionException) {
                logger.error("🚨 连接超时异常详情:");
                logger.error("  - 总等待时长: {}ms", totalWaitTime);
                logger.error("  - 方法已执行: {}ms", System.currentTimeMillis() - methodStartTime);
                logger.error("  - 批次信息: 表={}, 大小={}, 首条offset={}",
                        tableName, batchItems.size(), batchItems.get(0).getPayload().getOffset());

                // ✅ 记录连接池状态
                if (this.dataSourceHolder.getDataSource() instanceof HikariDataSource) {
                    HikariMonitor.logPoolStatsDirectly(
                            (HikariDataSource) this.dataSourceHolder.getDataSource(),
                            "连接超时时");
                }

                // ✅ 记录 JVM 线程信息
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                logger.error("  - JVM线程: 总数={}, 守护={}, 峰值={}",
                        threadMXBean.getThreadCount(),
                        threadMXBean.getDaemonThreadCount(),
                        threadMXBean.getPeakThreadCount());
            }

            // 未处理的数据标记为整体失败
            for (BatchInsertItem item : batchItems) {
                Long offset = item.getPayload().getOffset();
                if (!statusMap.containsKey(offset)) {
                    statusMap.put(offset, "CONNECTION_FAILED: " + e.getMessage());
                }
            }
        }

        return statusMap;

    }


    // 执行单条插入，根据当前消息的日期单位构建 SQL 并绑定参数。
    @Override
    public void executeSingleInsert(String tableName, KafkaPayload payload,List<ColumnInfo> allColumns) throws Exception {

        // 获取数据并添加CDC字段
        Map<String, Object> afterData = payload.getAfterData();

        // 增加 op,dml_Time，kafka_enqueue_time
        String op = payload.getOp();
        afterData.put("cdc_op", op.equals("c") ? "i" : op);
        afterData.put("cdc_dml_Time", payload.getDmlTime());
        afterData.put("cdc_kafka_enqueue_time", payload.getKafkaEnqueueTime());
        afterData.put("cdc_insert_time", LocalDateTime.now());
        afterData.put("cdc_update_time", LocalDateTime.now());



        /*
              预处理：默认空函数
              doris的处理： 1.处理 Doris custom_column_mapping
                           2.Doris 且当前表建表属性未声明 cdc_rowid 时，插入前移除该字段
         */
        preprocessInsertData(tableName, afterData);

        // 消息中的数据，过滤掉不存在字段，因为表字段与消息字段可能不一致，表可能是不是程序自动创建的
        Map<String, Object> insertData = prepareSingleInsertData(tableName, afterData, allColumns);
        List<String> fieldList = new ArrayList<>(insertData.keySet());





         // 复制日期字段集合，补充 CDC 字段时不修改原消息。
        Set<String> dateFieldSet = new HashSet<>(payload.getDateFieldSet());
        appendCdcTimeFields(dateFieldSet);

         // 获取当前消息的日期字段单位。
        Map<String, String> dateFieldUnits = payload.getDateFieldUnits();

        // 获取主键
        Set<String> primaryKeys = payload.getPrimaryKeySet();

       // 根据当前消息的单位选择 TO_DATE 或 TO_TIMESTAMP。
        String sql = buildInsertSql(
                tableName,
                fieldList,
                primaryKeys,
                dateFieldSet,
                dateFieldUnits
        );

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
//            logger.info("afterData: {}", afterData);
            // 使用生成 SQL 时的同一份日期元数据绑定参数。
            setParameters(
                    ps,
                    insertData,
                    fieldList,
                    dateFieldSet,
                    dateFieldUnits
            );


            // 执行插入
            ps.executeUpdate();

        } catch (SQLException e) {
            e.getMessage();
            logger.error("单条插入失败: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean supportsBatch() {
        return true;
    }


     protected void appendCdcTimeFields(Set<String> dateFieldSet) {
        dateFieldSet.add("cdc_dml_Time");
        dateFieldSet.add("cdc_kafka_enqueue_time");
        dateFieldSet.add("cdc_insert_time");
        dateFieldSet.add("cdc_update_time");
    }


    public DatabaseAdapter getAdapter() {
        return adapter;
    }

    public void setAdapter(DatabaseAdapter adapter) {
        this.adapter = adapter;
    }

    public DataSourceHolder getDataSourceHolder() {
        return dataSourceHolder;
    }

    public void setDataSourceHolder(DataSourceHolder dataSourceHolder) {
        this.dataSourceHolder = dataSourceHolder;
    }

    public static void main(String[] args) {
        String strValue ="1751932801000";
        // 如果是纯数字，按时间戳处理
        if (strValue.matches("\\d+")) {
            long timestamp = Long.parseLong(strValue);

            System.out.println(new Date(timestamp));
        }
        // 支持常见日期格式
        java.time.format.DateTimeFormatter[] formatters = new java.time.format.DateTimeFormatter[] {
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
        };
        for (java.time.format.DateTimeFormatter formatter : formatters) {
            try {
                LocalDateTime ldt;
                if (formatter.toString().contains("HH")) {
                    ldt = LocalDateTime.parse(strValue, formatter);
                } else {
                    ldt = java.time.LocalDate.parse(strValue, formatter).atStartOfDay();
                }

                System.out.println(Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant()));
            } catch (Exception ignore) {}
        }
    }












    /**
     * 仅对 Doris 目标库生效：
     * 1. 先取当前表的表级建表属性
     * 2. 没有则取库级建表属性
     * 3. 判断 key_columns / distributed_hash_columns 中是否包含 cdc_rowid
     */
    protected boolean shouldKeepCdcRowIdForCurrentTable(String tableName) {
        TargetDataSourceConfig targetConfig = this.dataSourceHolder.getTargetDataSourceConfig();
        if (targetConfig == null) {
            return true;
        }

        if (targetConfig.getDataSourceType() != DatabaseType.DORIS) {
            return true;
        }

        DdlBizAttr ddlBizAttr = targetConfig.getTableLevelDdlBizAttr(tableName);
        if (ddlBizAttr == null) {
            ddlBizAttr = targetConfig.getSystemLevelDdlBizAttr();
        }

        if (!(ddlBizAttr instanceof DorisBizAttr)) {
            return true;
        }

        DorisBizAttr dorisBizAttr = (DorisBizAttr) ddlBizAttr;
        return containsCdcRowId(dorisBizAttr.getKeyColumns())
                || containsCdcRowId(dorisBizAttr.getDistributedHashColumns());
    }

    private boolean containsCdcRowId(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return false;
        }

        for (String column : columns) {
            if (column == null) {
                continue;
            }
            String normalized = column.trim();
            if (normalized.equalsIgnoreCase(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER)
                    || normalized.equalsIgnoreCase(CdcColumnConstants.CDC_ROWID_COLUMN_UPPER)) {
                return true;
            }
        }
        return false;
    }




    protected void preprocessInsertData(String tableName, Map<String, Object> data) {
        // default no-op
    }




    // 过滤不存在字段
    protected Map<String, Object> prepareSingleInsertData(String tableName,
                                                          Map<String, Object> afterData,  // 消息中的参数
                                                          List<ColumnInfo> allColumns) {  // 实际表字段
        if (afterData == null || afterData.isEmpty()) {
            return Collections.emptyMap();
        }

        if (allColumns == null || allColumns.isEmpty()) {
            return new LinkedHashMap<>(afterData);
        }

        // 实际表字段集合
        Set<String> targetColumns = allColumns.stream()
                .filter(Objects::nonNull)
                .map(ColumnInfo::getColumnName)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        Map<String, Object> filteredData = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : afterData.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.trim().isEmpty()) {
                continue;
            }

            // 只保留实际表有包含的字段，没有的字段过滤掉
            if (targetColumns.contains(key.toLowerCase(Locale.ROOT))) {
                filteredData.put(key, entry.getValue());
            }
        }

        return filteredData;
    }


    /**
         processedData的归一处理：
         1. 如果key在表中不存在对应的字段，该entry过滤掉不要
         2. key与表中字段的实际大小写保持一致


     */
    protected Map<String, Object> normalizeDataByTargetColumns(Map<String, Object> processedData,
                                                             List<ColumnInfo> allColumns) {
        if (processedData == null || processedData.isEmpty()) {
            return Collections.emptyMap();
        }

        if (allColumns == null || allColumns.isEmpty()) {
            return processedData;
        }

        // key: 小写列名, value: 目标表真实列名（保留实际大小写）
        Map<String, String> targetColumnCaseMap = buildTargetColumnCaseMap(allColumns);

        Map<String, Object> normalizedData = new LinkedHashMap<>();

        // 遍历cdc的参数，即源表字段
        for (Map.Entry<String, Object> entry : processedData.entrySet()) {
            // 源表字段名
            String sourceKey = entry.getKey();
            // 源表字段值
            Object value = entry.getValue();

            if (sourceKey == null || sourceKey.trim().isEmpty()) {
                continue;
            }
            
            // 表中的实际字段名
            String actualColumnName = targetColumnCaseMap.get(sourceKey.toLowerCase(Locale.ROOT));
            if (actualColumnName == null) {
                // 目标表不存在该字段，直接丢弃
                continue;
            }

            Object existingValue = normalizedData.get(actualColumnName);
            normalizedData.put(actualColumnName, choosePreferredValue(existingValue, value));
        }

        return normalizedData;
    }




    /**
         表中实际字段，构建成map，
         key:   小写字段名
         value: 原始字段名(与表中字段实际的大小写一致)
     */
    protected Map<String, String> buildTargetColumnCaseMap(List<ColumnInfo> allColumns) {
        Map<String, String> targetColumnCaseMap = new LinkedHashMap<>();
        if (allColumns == null || allColumns.isEmpty()) {
            return targetColumnCaseMap;
        }

        for (ColumnInfo columnInfo : allColumns) {
            if (columnInfo == null || columnInfo.getColumnName() == null) {
                continue;
            }

            String actualColumnName = columnInfo.getColumnName().trim();
            if (actualColumnName.isEmpty()) {
                continue;
            }

            targetColumnCaseMap.put(actualColumnName.toLowerCase(Locale.ROOT), actualColumnName);
        }

        return targetColumnCaseMap;
    }
    protected Object choosePreferredValue(Object existingValue, Object newValue) {
        if (existingValue == null) {
            return newValue;
        }
        if (newValue == null) {
            return existingValue;
        }
        return newValue;
    }
}