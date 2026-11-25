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

// Oracle 插入策略，根据日期字段单位选择 SQL 日期表达式并绑定参数
public class OracleInsertStrategy extends AbstractInsertStrategy {

    private static final Logger logger = LogManager.getLogger(OracleInsertStrategy.class);


    // 初始化 Oracle 插入策略，创建数据源持有对象和数据库适配器。
    public OracleInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        // 初始化基类的数据源配置。
        super(targetDataSourceConfig);

        // 根据目标库配置创建适配器。
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    // 保留接口要求的入口，但不为源表日期字段补充或猜测单位。
    @Override
    public String buildInsertSql(String tableName,
                                 List<String> fieldList,
                                 Set<String> primaryKeys,
                                 Set<String> dateFieldSet) {
        // 空映射仅适用于没有源日期字段的 SQL，源日期字段缺少单位时明确报错。
        return buildInsertSql(
                tableName,
                fieldList,
                primaryKeys,
                dateFieldSet,
                Collections.emptyMap()
        );
    }
    // 根据明确的字段单位构建 Oracle 插入 SQL。
    @Override
    public String buildInsertSql(String tableName,
                                 List<String> fieldList,
                                 Set<String> primaryKeys,
                                 Set<String> dateFieldSet,             // 源表的时间字段，小写
                                 Map<String, String> dateFieldUnits) { // 源表时间字段的单位，小写
        // 日期元数据必须使用非 null 集合，没有日期字段时允许为空。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 所有 列名 均转 大写
        String columns = String.join(
                ",",
                convertFieldsToUpperCase(fieldList)
        );

        // 按字段顺序构建表达式，保持 SQL 和参数绑定顺序一致。
        List<String> placeholders = new ArrayList<>(fieldList.size());
        for (String fieldName : fieldList) {
            // 使用小写名称查询日期单位及识别 CDC 字段。
            String normalizedFieldName = fieldName.toLowerCase(Locale.ROOT);

            // 判断是否为日期字段，判断与 OracleAdapter 保持一致。
            boolean dateField = dateFieldSet.contains(fieldName)
                    || dateFieldSet.contains(normalizedFieldName);

            // 非日期字段使用普通占位符。
            if (!dateField) {
                placeholders.add("?");
                continue;
            }

            // 服务端生成的 CDC 时间统一精确到秒，不依赖源表单位。
            if (isCdcTimeField(normalizedFieldName)) {
                placeholders.add("TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')");
                continue;
            }

            // 源表 时间字段 必须提供明确单位。
            String unit = dateFieldUnits.get(normalizedFieldName);
            placeholders.add(buildDatePlaceholder(fieldName, unit));
        }

        // 保留表名转大写的处理，并避免受 JVM 默认区域设置影响。
        String upperTable = tableName.toUpperCase(Locale.ROOT);

        // 返回单批次使用的插入 SQL。
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                upperTable,
                columns,
                String.join(",", placeholders)
        );
    }

    // 根据源日期字段单位构建 SQL 表达式，缺少单位时明确报错。
    private String buildDatePlaceholder(String fieldName, String unit) {
        // 不再将缺失单位默认解释为毫秒或 DATE。
        if (unit == null) {
            throw new IllegalArgumentException(
                    "日期字段缺少时间单位，字段: " + fieldName
            );
        }

        // 表达式中的格式必须与 OracleAdapter 输出一致。
        switch (unit) {
            case "ms":
                // 毫秒单位按约定保留到秒。
                return "TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')";
            case "us":
                // 微秒单位保留 6 位小数秒。
                return "TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF6')";
            case "ns":
                // 纳秒单位保留 9 位小数秒。
                return "TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9')";
            default:
                // 非法单位附带字段名报错，便于定位消息问题。
                throw new IllegalArgumentException(
                        "日期字段单位无效，字段: " + fieldName
                                + "，单位: " + unit
                                + "，仅支持 ms、us、ns"
                );
        }
    }

    // 识别服务端生成的 CDC 时间字段，参数使用小写字段名。
    private boolean isCdcTimeField(String fieldName) {
        // 保持与 OracleAdapter 相同的 CDC 时间字段范围。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // CDC 时间统一使用秒级日期表达式。
                return true;
            default:
                // 其他日期字段必须使用消息中的单位。
                return false;
        }
    }


    // 保留接口要求的入口，不为非空源日期值猜测单位。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keyList,
                              Set<String> dateFieldSet) throws SQLException {
        // 源日期值缺少单位时，由 OracleAdapter 明确报错。
        setParameters(
                ps,
                data,
                keyList,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 按 SQL 字段顺序绑定参数，将当前消息的单位信息传给适配器。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keyList,
                              Set<String> dateFieldSet,
                              Map<String, String> dateFieldUnits) throws SQLException {
        // JDBC 参数索引从 1 开始。
        int parameterIndex = 1;

        // 按生成 SQL 时的字段顺序逐个绑定参数。
        for (String fieldName : keyList) {
            // 获取当前字段值，未提供的字段按 null 处理。
            Object value = data.get(fieldName);

            // 由 OracleAdapter 根据字段单位生成对应的日期参数字符串。
            this.adapter.setParameterValue(
                    ps,
                    parameterIndex,
                    value,
                    fieldName,
                    dateFieldSet,
                    dateFieldUnits
            );

            // 移动到下一个参数位置。
            parameterIndex++;
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