package com.bsoft.server.sql.insert;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.dataSource.HikariMonitor;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.adapter.OracleAdapter;

import com.bsoft.server.sql.update.AbstractUpdateStrategy;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Oracle数据库插入策略实现
 * 修改策略：只插入新数据，如果主键重复则忽略（不插入不更新）
 */
public abstract class AbstractInsertStrategy  implements  InsertStrategy{


    private static final Logger logger = LogManager.getLogger(AbstractInsertStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected  DatabaseAdapter adapter;


    public AbstractInsertStrategy() {
    }

    protected AbstractInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }



    @Override
    public Map<Long, String> executeBatchInsert(String tableName,
                                                List<BatchInsertItem> batchItems,
                                                BatchSizeManager batchSizeManager,
                                                List<ColumnInfo> allColumns,
                                                boolean needPadding) throws Exception {

        if (batchItems.isEmpty()) {
            return Collections.emptyMap();
        }
        // ✅ 记录开始时间
        long methodStartTime = System.currentTimeMillis();

        // 使用第一条数据的字段结构
        BatchInsertItem firstItem = batchItems.get(0);
        List<String> fieldList = new ArrayList<>(firstItem.getData().keySet());
        Collections.sort(fieldList);

        Set<String> dateFieldSet = firstItem.getPayload().getDateFieldSet();
        appendCdcTimeFields(dateFieldSet);

        Set<String> primaryKeys = firstItem.getPayload().getPrimaryKeySet();



        // 生成sql语句
        String sql = buildInsertSql(tableName, fieldList, primaryKeys, dateFieldSet);

        // 记录每条数据的处理状态，value为状态码或错误信息，整个生命周期中，value可能会发生多次变化
        Map<Long, String> statusMap = new HashMap<>(); // offset -> status/error

        // 参数绑定成功的计数
        int successfullyBoundCount = 0;

        long startTime1 = System.currentTimeMillis();







        long connectionStartTime = System.currentTimeMillis();

        // ✅ 增加超时前的状态快照
        if (this.dataSourceHolder.getDataSource() instanceof HikariDataSource) {
            HikariDataSource ds = (HikariDataSource) this.dataSourceHolder.getDataSource();
            logger.info("⏱️ 准备获取连接: 池名={}, 活动={}, 空闲={}, 总数={}, 等待线程={}, 方法已耗时={}ms",
                    ds.getPoolName(),
                    ds.getHikariPoolMXBean().getActiveConnections(),
                    ds.getHikariPoolMXBean().getIdleConnections(),
                    ds.getHikariPoolMXBean().getTotalConnections(),
                    ds.getHikariPoolMXBean().getThreadsAwaitingConnection(),
                    System.currentTimeMillis() - methodStartTime);
        }
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection()) {

            long connectionAcquireTime = System.currentTimeMillis() - connectionStartTime;

            // ✅ 详细记录连接获取信息
            logger.info("✅ 成功获取连接: 耗时={}ms, 表={}, 批次={}, 线程={}",
                    connectionAcquireTime, tableName, batchItems.size(), Thread.currentThread().getName());

            // ✅ 测试连接有效性
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
                    Long offset = item.getPayload().getOffset();
                    try {
                        // 进行实际的参数绑定
                        setParameters(ps, item.getData(), fieldList, dateFieldSet);
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

    int i;
    @Override
    public void executeSingleInsert(String tableName, KafkaPayload payload) throws Exception {
        i++;
        logger.error("executeSingleInsert: {}", i);
        // 获取数据并添加CDC字段
        Map<String, Object> afterData = payload.getAfterData();

        // 增加 op,dml_Time，kafka_enqueue_time
        afterData.put("cdc_op", payload.getOp());
        afterData.put("cdc_dml_Time", payload.getDmlTime());
        afterData.put("cdc_kafka_enqueue_time", payload.getKafkaEnqueueTime());
        afterData.put("cdc_insert_time", LocalDateTime.now());

        // 获取字段列表并排序
        List<String> fieldList = new ArrayList<>(afterData.keySet());


        // 获取日期字段集合
        Set<String> dateFieldSet = payload.getDateFieldSet();
        appendCdcTimeFields(dateFieldSet);

        // 获取主键
        Set<String> primaryKeys = payload.getPrimaryKeySet();

        // 生成SQL
        String sql = buildInsertSql(tableName, fieldList, primaryKeys, dateFieldSet);

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            logger.info("afterData: {}", afterData);
            // 设置参数
            setParameters(ps, afterData, fieldList, dateFieldSet);


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
}