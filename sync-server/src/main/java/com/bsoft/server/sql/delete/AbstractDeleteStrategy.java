package com.bsoft.server.sql.delete;

import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName: AbstractDeleteStrategy
 * Package:   com.bsoft.server.sql.delete
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/30 - 15:36
 */
public abstract class AbstractDeleteStrategy implements DeleteStrategy {

    private static final Logger logger = LogManager.getLogger(AbstractDeleteStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected TableRowDeleteStrategy tableRowDeleteStrategy;

    // 建议放在类中，复用执行状态字符串，避免魔法字面量
    private static final String STATUS_BOUND_SUCCESS = "BOUND_SUCCESS";
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_EXECUTE_FAILED = "EXECUTE_FAILED";
    private static final String STATUS_BINDING_FAILED_PREFIX = "BINDING_FAILED:";
    private static final String STATUS_BATCH_EXECUTE_FAILED_PREFIX = "BATCH_EXECUTE_FAILED:";
    private static final String STATUS_CONNECTION_FAILED_PREFIX = "CONNECTION_FAILED:";


    public AbstractDeleteStrategy() {
    }

    protected AbstractDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig, TableRowDeleteStrategy tableRowDeleteStrategy) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig);
        this.tableRowDeleteStrategy = tableRowDeleteStrategy;
    }


    /**
     * 校验主键集合是否为空
     *
     * @param tableName     表名
     * @param primaryKeySet 主键集合
     * @throws IllegalArgumentException 当主键集合为空时抛出
     */
    protected void validatePrimaryKeySet(String tableName, Set<String> primaryKeySet) {
        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("Primary key set is empty, unsafe to delete from table: " + tableName);
        }
    }

    /**
     * 构建 WHERE 子句（按字典序排列主键，避免 SQL 缓存失效）
     *
     * @param primaryKeySet 主键集合
     * @return WHERE 子句（如：pk1 = ? AND pk2 = ?）
     */
    protected String buildWhereClause(Set<String> primaryKeySet) {
        List<String> primaryKeyList = new ArrayList<>(primaryKeySet);
        primaryKeyList.sort(String::compareTo);

        return primaryKeyList.stream()
                .map(pk -> pk + " = ?")
                .collect(Collectors.joining(" and "));
    }


//    @Override
//    public String buildDeleteSql(String tableName, Set<String> primaryKeySet) {
//        // 没有主键，无法安全删除
//        if (primaryKeySet.isEmpty()) {
//            throw new IllegalArgumentException("Primary key set is empty, unsafe to delete from table: " + tableName);
//        }
//
//        // 4. WHERE：固定顺序（字典序），避免 Set 迭代顺序漂移
//        List<String> primaryKeyList = new ArrayList<>(primaryKeySet);
//        primaryKeyList.sort(String::compareTo);
//        // 物理删除
//
//        String whereClause = primaryKeyList.stream()
//                .map(pk -> pk + " = ?")
//                .collect(Collectors.joining(" AND "));
//        return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
//
//    }

    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> beforeData,
                              Set<String> primaryKeySet) throws SQLException {
        // 仅绑定 WHERE 子句的主键参数（单条删除）
        int index = 1;

        if (tableRowDeleteStrategy == TableRowDeleteStrategy.DELETE_FLAG) {
            Object pkValue = beforeData.get(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
            String newRowId = System.currentTimeMillis() + "_" + String.valueOf(pkValue);
            ps.setObject(index++, newRowId);
        }

        List<String> primaryKeyList = new ArrayList<>(primaryKeySet);
        primaryKeyList.sort(String::compareTo);

        for (String pk : primaryKeyList) {
            Object value = beforeData.get(pk);
            ps.setObject(index++, value);
        }
    }
//
//    @Override
//    public void executeDelete(String tableName, KafkaPayload payload) throws Exception {
//        Map<String, Object> beforeData = payload.getBeforeData();
//        Set<String> primaryKeySet = payload.getPrimaryKeySet();
//
//        // 用源表的主键删除，可能存在以下问题:1.源表没有主键 2.多表合并，主键冲突，因此统一用ROWID来删除
//        primaryKeySet.clear();
//        primaryKeySet.add(CdcSqlConstants.CDC_ROWID_FIELD);
//
//
//        // 检查beforeData是否包含所有主键值
//        for (String pk : primaryKeySet) {
//            if (!beforeData.containsKey(pk.toLowerCase()) || beforeData.get(pk.toLowerCase()) == null) {
//                logger.info("⚠️ Primary key value missing in beforeData: " + pk + " for table: " + tableName);
//                return;
//            }
//        }
//
//        // 构建SQL
//        String sql = buildDeleteSql(tableName, primaryKeySet);
//
//        boolean success = true;
//        String logDetail = "";
//
//        try (Connection conn = dataSourceHolder.getDataSource().getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            setParameters(ps, beforeData, primaryKeySet);
//
//            // 执行操作
//            int affectedRows = ps.executeUpdate();
//
//            if (affectedRows == 0) {
//                logger.warn("⚠️ No rows affected for table: {}, primaryKeys: {}",
//                        tableName, primaryKeySet);
//            } else {
//                logger.debug("✅ Deleted {} rows in table: {}", affectedRows, tableName);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw  e;  // 抛出异常前，try-with-resources 自动关闭连接
//        }
//    }


    @Override
    public void executeDelete(String tableName, KafkaPayload payload) throws Exception {
        // 兼容旧调用：单条也走批量逻辑，保持行为一致
        List<KafkaPayload> payloads = new ArrayList<>(1);
        payloads.add(payload);
        Map<Long, String> statusMap = executeBatchDelete(tableName, payloads);

        // 如果批量里该条不是 SUCCESS，则抛异常让上层记录失败日志
        String status = statusMap.getOrDefault(payload.getOffset(), STATUS_EXECUTE_FAILED);
        if (!STATUS_SUCCESS.equals(status)) {
            throw new RuntimeException("single delete failed: " + status);
        }
    }

    @Override
    public Map<Long, String> executeBatchDelete(String tableName, List<KafkaPayload> payloads) throws Exception {
        Map<Long, String> statusMap = new HashMap<>();
        if (payloads == null || payloads.isEmpty()) {
            return statusMap;
        }

        // 批量删除统一使用 CDC_ROWID 作为主键字段
        Set<String> primaryKeySet = new LinkedHashSet<>();
        primaryKeySet.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);

        // 先构建 SQL，由具体的子类实现
        String sql = buildDeleteSql(tableName, primaryKeySet);

        long connectionStartTime = System.currentTimeMillis();
        int boundCount = 0;

        try (Connection conn = dataSourceHolder.getDataSource().getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                // 1) 逐条绑定 + addBatch
                for (KafkaPayload payload : payloads) {
                    Long offset = payload.getOffset();
                    Map<String, Object> beforeData = payload.getBeforeData();

                    // 缺少 CDC_ROWID 就直接标记失败并跳过
                    Object rowIdValue = beforeData != null
                            ? beforeData.get(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER)
                            : null;

                    if (rowIdValue == null) {
                        statusMap.put(offset, STATUS_BINDING_FAILED_PREFIX + " missing cdc_rowid");
                        continue;
                    }

                    try {
                        // 仅绑定 WHERE 子句主键参数
                        setParameters(ps, beforeData, primaryKeySet);
                        ps.addBatch();
                        boundCount++;
                        statusMap.put(offset, STATUS_BOUND_SUCCESS);
                    } catch (Exception e) {
                        statusMap.put(offset, STATUS_BINDING_FAILED_PREFIX + " " + e.getMessage());
                    }
                }

                // 2) 执行批量
                if (boundCount > 0) {
                    try {
                        int[] results = ps.executeBatch();

                        // 将执行结果回填到已绑定成功的记录
                        int resultIndex = 0;
                        for (KafkaPayload payload : payloads) {
                            Long offset = payload.getOffset();
                            if (STATUS_BOUND_SUCCESS.equals(statusMap.get(offset))) {
                                if (resultIndex < results.length) {
                                    if (results[resultIndex] == PreparedStatement.EXECUTE_FAILED) {
                                        statusMap.put(offset, STATUS_EXECUTE_FAILED);
                                    } else {
                                        statusMap.put(offset, STATUS_SUCCESS);
                                    }
                                    resultIndex++;
                                }
                            }
                        }

                        conn.commit();
                    } catch (Exception e) {
                        conn.rollback();
                        // 批量执行失败，标记所有绑定成功的数据
                        for (KafkaPayload payload : payloads) {
                            Long offset = payload.getOffset();
                            if (STATUS_BOUND_SUCCESS.equals(statusMap.get(offset))) {
                                statusMap.put(offset, STATUS_BATCH_EXECUTE_FAILED_PREFIX + " " + e.getMessage());
                            }
                        }
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            // 连接或执行阶段异常：标记所有尚未赋值的记录
            for (KafkaPayload payload : payloads) {
                Long offset = payload.getOffset();
                if (!statusMap.containsKey(offset)) {
                    statusMap.put(offset, STATUS_CONNECTION_FAILED_PREFIX + " " + e.getMessage());
                }
            }
            throw e;
        }

        return statusMap;
    }
}