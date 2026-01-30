package com.bsoft.server.sql.insert;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.sql.adapter.OracleAdapter;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class OracleInsertStrategy extends AbstractInsertStrategy {

    private static final Logger logger = LogManager.getLogger(OracleInsertStrategy.class);

    public OracleInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        // 使用工厂创建适配器
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);

    }


    @Override
    public String buildInsertSql(String tableName, List<String> fieldList, Set<String> primaryKeys, Set<String> dateFieldSet) {
        // 1. 字段名统一转成大写
        List<String> upperCaseFieldList = convertFieldsToUpperCase(fieldList);

        // 2. 构建列名部分
        String columns = String.join(",", upperCaseFieldList);

        // 3. 构建占位符（date 字段 → TO_DATE）
        String placeholders = fieldList.stream()
                .map(f -> {
                    if (dateFieldSet.contains(f)) {
                        return "TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss')";
                    }
                    return "?";
                })
                .collect(Collectors.joining(","));

        // 4. 表名也统一大写
        String upperTable = tableName.toUpperCase();

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                upperTable,
                columns,
                placeholders
        );
    }




    @Override
    public void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keyList, Set<String> dateFieldSet)
            throws SQLException {

        int index = 1;
        // 按照keys的顺序设置参数值
        for (String key : keyList) {
            Object value = data.get(key);
            this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
        }
    }





    /**
     * 将字段列表中的所有元素转换为大写
     * @param filedList 原始字段列表
     * @return 转换为大写的字段列表
     */
    private List<String> convertFieldsToUpperCase(List<String> filedList) {
        return filedList.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toList());
    }


//    /**
//     * 构建MERGE语句，只执行INSERT操作，不执行UPDATE
//     * 当主键匹配时什么都不做
//     */
//    private String buildInsertOnlyMergeSql(String tableName, List<String> filedList, Set<String> primaryKeys, Set<String> dateFieldSet) {
//        StringBuilder sql = new StringBuilder();
//        sql.append("MERGE INTO ").append(tableName).append(" target ");
//        sql.append("USING (SELECT ");
//
//        // 构建SELECT子句 - 为每个字段创建别名，日期字段使用TO_DATE
//        for (int i = 0; i < filedList.size(); i++) {
//            if (i > 0) sql.append(", ");
//
//            String fieldName = filedList.get(i);
//            if (dateFieldSet.contains(fieldName)) {
//                sql.append("TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS') AS ").append(fieldName);
//            } else {
//                sql.append("? AS ").append(fieldName);
//            }
//        }
//        sql.append(" FROM DUAL) source ");
//
//        // 构建ON子句（基于主键）
//        sql.append("ON (");
//        String onCondition = primaryKeys.stream()
//                .map(pk -> "target." + pk + " = source." + pk)
//                .collect(Collectors.joining(" AND "));
//        sql.append(onCondition).append(") ");
//
//        // 只有当不匹配时才插入，匹配时什么都不做
//        sql.append("WHEN NOT MATCHED THEN INSERT (");
//        sql.append(String.join(",", filedList));
//        sql.append(") VALUES (");
//        String sourceValues = filedList.stream()
//                .map(k -> "source." + k)
//                .collect(Collectors.joining(","));
//        sql.append(sourceValues).append(")");
//
//        return sql.toString();
//    }



//
//    @Override
//    public Map<Long, String> executeBatchInsert(String tableName,
//                                                List<BatchInsertItem> batchItems) throws Exception {
//
//        if (batchItems.isEmpty()) {
//            return Collections.emptyMap();
//        }
//
//
//        long startTime = System.currentTimeMillis();
//        // 使用第一条数据的字段结构
//        BatchInsertItem firstItem = batchItems.get(0);
//        List<String> fieldList = new ArrayList<>(firstItem.getData().keySet());
//        Collections.sort(fieldList);
//
//        Set<String> dateFieldSet = firstItem.getPayload().getDateFieldSet();
//        appendCdcTimeFields(dateFieldSet);
//
//        Set<String> primaryKeys = firstItem.getPayload().getPrimaryKeySet();
//
//
//
//        // 生成sql语句
//        String sql = buildInsertSql(tableName, fieldList, primaryKeys, dateFieldSet);
//
//        // 记录每条数据的处理状态，value为状态码或错误信息，整个生命周期中，value可能会发生多次变化
//        Map<Long, String> statusMap = new HashMap<>(); // offset -> status/error
//
//        // 参数绑定成功的计数
//        int successfullyBoundCount = 0;
//
//        long startTime1 = System.currentTimeMillis();
//        try (Connection conn = getDataSource().getConnection()) {
//
//            double executionTimeSeconds1 = (System.currentTimeMillis() - startTime1) / 1000.0;
//            conn.setAutoCommit(false);
//
//
//            try (PreparedStatement ps = conn.prepareStatement(sql)) {
//
//                long startTime2 = System.currentTimeMillis();
//                // step1:参数绑定阶段
//                for (BatchInsertItem item : batchItems) {
//                    Long offset = item.getPayload().getOffset();
//                    try {
//                        // 进行实际的参数绑定
//                        setParameters(ps, item.getData(), fieldList, dateFieldSet);
//                        ps.addBatch();
//                        successfullyBoundCount++;
//                        statusMap.put(offset, "BOUND_SUCCESS"); // 绑定成功
//                    } catch (Exception e) {
//                        logger.warn("参数绑定失败，跳过: offset={}, error={}", offset, e.getMessage());
//                        statusMap.put(offset, "BINDING_FAILED: " + e.getMessage()); // 绑定失败
//                    }
//
//                }
//                double executionTimeSeconds2 = (System.currentTimeMillis() - startTime2) / 1000.0;
//
////                long globalCommitGateStartTime = System.currentTimeMillis();
////                GlobalCommitGate.COMMIT.acquire();
////                double globalCommitGateExecutionTimeSeconds = (System.currentTimeMillis() - globalCommitGateStartTime) / 1000.0;
//
//
//                long startTime3 = System.currentTimeMillis();
//                // step2:数据库执行阶段
//                if (successfullyBoundCount > 0) {
//                    try {
//                        int[] results = ps.executeBatch();
//
//                        // 更新成功绑定数据的执行状态
//                        int resultIndex = 0;
//                        for (BatchInsertItem item : batchItems) {
//                            Long offset = item.getPayload().getOffset();
//                            if ("BOUND_SUCCESS".equals(statusMap.get(offset))) {
//                                if (resultIndex < results.length) {
//                                    if (results[resultIndex] == PreparedStatement.EXECUTE_FAILED) {
//                                        statusMap.put(offset, "EXECUTE_FAILED");
//                                    } else {
//                                        statusMap.put(offset, "SUCCESS");
//                                    }
//                                    resultIndex++;
//                                }
//                            }
//                        }
//
//
//                        conn.commit();
//
//                        // 过滤掉失败的结果再求和
//                        int actualInserted = Arrays.stream(results)
//                                .filter(result -> result != PreparedStatement.EXECUTE_FAILED)
//                                .map(result -> result == PreparedStatement.SUCCESS_NO_INFO ? 1 : Math.max(0, result))
//                                .sum();
//
//                        long failedCount = Arrays.stream(results)
//                                .filter(result -> result == PreparedStatement.EXECUTE_FAILED)
//                                .count();
//
//
//
//                        double executionTimeSeconds = (System.currentTimeMillis() - startTime3) / 1000.0;
//                        logger.info("Oracle: 批量插入完成: 本组记录供 {} 条，实际成功 {} 条, 执行失败 {} 条, 绑定失败 {} 条,获取链接耗时{} S，绑定参数耗时 {} S， 耗时 {} S",
//                                batchItems.size(),actualInserted, failedCount, batchItems.size() - successfullyBoundCount,executionTimeSeconds1,executionTimeSeconds2,executionTimeSeconds);
//
//                    } catch (Exception e) {
//                        conn.rollback();
//                        // 批量执行失败，更新所有绑定成功的数据状态
//                        for (BatchInsertItem item : batchItems) {
//                            Long offset = item.getPayload().getOffset();
//                            if ("BOUND_SUCCESS".equals(statusMap.get(offset))) {
//                                statusMap.put(offset, "BATCH_EXECUTE_FAILED: " + e.getMessage());
//                            }
//                        }
//                        throw e;
//                    }
//                }
//
//            } catch (Exception e) {
//                conn.rollback();
//                throw e;
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.error("❌ 批量插入失败: {}", e.getMessage());
//
//            // 未处理的数据标记为整体失败
//            for (BatchInsertItem item : batchItems) {
//                Long offset = item.getPayload().getOffset();
//                if (!statusMap.containsKey(offset)) {
//                    statusMap.put(offset, "CONNECTION_FAILED: " + e.getMessage());
//                }
//            }
//        }
//
//        return statusMap;
//
//    }
//
//    @Override
//    public void executeSingleInsert(String tableName, KafkaPayload payload) throws Exception {
//
//        // 获取数据并添加CDC字段
//        Map<String, Object> afterData = payload.getAfterData();
//
//        // 增加 op,dml_Time，kafka_enqueue_time
//        afterData.put("cdc_op", payload.getOp());
//        afterData.put("cdc_dml_Time", payload.getDmlTime());
//        afterData.put("cdc_kafka_enqueue_time", payload.getKafkaEnqueueTime());
//        afterData.put("cdc_insert_time", LocalDateTime.now());
//
//        // 获取字段列表并排序
//        List<String> fieldList = new ArrayList<>(afterData.keySet());
//
//
//        // 获取日期字段集合
//        Set<String> dateFieldSet = payload.getDateFieldSet();
//        appendCdcTimeFields(dateFieldSet);
//
//        // 获取主键
//        Set<String> primaryKeys = payload.getPrimaryKeySet();
//
//        // 生成SQL
//        String sql = buildInsertSql(tableName, fieldList, primaryKeys, dateFieldSet);
//
//        try (Connection conn = getDataSource().getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            setParameters(ps, afterData, fieldList, dateFieldSet);
//
//            // 执行插入
//            ps.executeUpdate();
//
//        } catch (SQLException e) {
//            logger.error("单条插入失败: {}", e.getMessage());
//            throw e;
//        }
//    }
//
//    @Override
//    public boolean supportsBatch() {
//        return true;
//    }
//
//    public static void main(String[] args) {
//        String strValue ="1751932801000";
//        // 如果是纯数字，按时间戳处理
//        if (strValue.matches("\\d+")) {
//            long timestamp = Long.parseLong(strValue);
//
//            System.out.println(new java.util.Date(timestamp));
//        }
//        // 支持常见日期格式
//        java.time.format.DateTimeFormatter[] formatters = new java.time.format.DateTimeFormatter[] {
//                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
//                java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
//        };
//        for (java.time.format.DateTimeFormatter formatter : formatters) {
//            try {
//                java.time.LocalDateTime ldt;
//                if (formatter.toString().contains("HH")) {
//                    ldt = java.time.LocalDateTime.parse(strValue, formatter);
//                } else {
//                    ldt = java.time.LocalDate.parse(strValue, formatter).atStartOfDay();
//                }
//
//                System.out.println(java.util.Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant()));
//            } catch (Exception ignore) {}
//        }
//    }
}