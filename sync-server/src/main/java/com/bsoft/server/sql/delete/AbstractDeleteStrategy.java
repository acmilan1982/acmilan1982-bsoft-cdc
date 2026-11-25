package com.bsoft.server.sql.delete;

import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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





    // 使用 JDBC 事务执行 Oracle、MySQL 批量删除。
// 按输入顺序绑定和执行，整批提交成功后正常返回。
// 任一校验、绑定或执行失败均抛出异常，由上层记录整批失败。
    @Override
    public void executeBatchDelete(String tableName,
                                   List<KafkaPayload> payloads,
                                   boolean forcePhysicalDelete) throws Exception {
        // 校验必要参数，空批次无需访问数据库。
        Objects.requireNonNull(payloads, "删除批次不能为 null");
        if (payloads.isEmpty()) {
            return;
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("删除的目标表名不能为空");
        }

        // 检查策略初始化状态。
        if (dataSourceHolder == null
                || dataSourceHolder.getTargetDataSourceConfig() == null) {
            throw new IllegalStateException("删除策略尚未初始化");
        }

        // 本实现仅供 Oracle、MySQL 使用，Doris 必须重写 HTTP 批量入口。
        DatabaseType databaseType = dataSourceHolder
                .getTargetDataSourceConfig().getDataSourceType();
        if (databaseType != DatabaseType.ORACLE
                && databaseType != DatabaseType.MYSQL) {
            throw new UnsupportedOperationException(
                    "当前 JDBC 批量删除入口不支持: " + databaseType);
        }

        // 非强制物理删除时，必须使用明确的删除配置。
        if (!forcePhysicalDelete
                && tableRowDeleteStrategy != TableRowDeleteStrategy.DELETE
                && tableRowDeleteStrategy != TableRowDeleteStrategy.DELETE_FLAG) {
            throw new IllegalStateException(
                    "不支持的删除策略: " + tableRowDeleteStrategy);
        }

        // 固定使用 cdc_rowid 定位记录，不依赖源表业务主键。
        Set<String> primaryKeys = new LinkedHashSet<>();
        primaryKeys.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);

        // 在获取连接前校验整批主键，不读取或转换源日期字段。
        List<Map<String, Object>> keyRows = new ArrayList<>(payloads.size());
        for (KafkaPayload payload : payloads) {
            // 仅允许删除消息进入该入口。
            if (payload == null || !"d".equals(payload.getOp())) {
                throw new IllegalArgumentException("删除批次包含空消息或非删除消息");
            }

            // 删除使用 before 数据。
            Map<String, Object> beforeData = payload.getBeforeData();
            if (beforeData == null) {
                throw new IllegalArgumentException("删除消息缺少 before 数据");
            }

            // 忽略字段名大小写查找主键，并拒绝大小写重复的主键字段。
            Object rowId = null;
            boolean rowIdFound = false;
            for (Map.Entry<String, Object> entry : beforeData.entrySet()) {
                if (CdcColumnConstants.CDC_ROWID_COLUMN_LOWER
                        .equalsIgnoreCase(entry.getKey())) {
                    if (rowIdFound) {
                        throw new IllegalArgumentException(
                                "删除消息包含重复的 cdc_rowid 字段");
                    }
                    rowIdFound = true;
                    rowId = entry.getValue();
                }
            }

            // 主键必须为非空字符串，保留原值，不裁剪或转换大小写。
            if (!(rowId instanceof String)
                    || ((String) rowId).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "删除消息必须提供非空字符串 cdc_rowid");
            }

            // 创建规范化的参数数据，不修改原始 before 数据。
            Map<String, Object> keyData = new LinkedHashMap<>();
            keyData.put(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER, rowId);
            keyRows.add(keyData);
        }

        // 强制物理删除时直接构建 DELETE，不修改策略实例的配置。
        // 其余情况复用子类 SQL，保留 Oracle、MySQL 原有逻辑删除规则。
        String sql = forcePhysicalDelete
                ? String.format(
                "delete from %s where %s",
                tableName, buildWhereClause(primaryKeys))
                : buildDeleteSql(tableName, primaryKeys);

        // 使用独立连接执行整批事务。
        try (Connection connection = dataSourceHolder.getDataSource().getConnection()) {
            // 禁用自动提交，所有批次命令成功后再提交。
            connection.setAutoCommit(false);

            try {
                // 整批使用同一个 PreparedStatement。
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    // 按消息到达顺序加入批次，不按主键分组或去重。
                    for (Map<String, Object> keyData : keyRows) {
                        // 清除上一条参数，避免残留绑定。
                        statement.clearParameters();

                        // 强制物理删除只有一个 WHERE 参数。
                        if (forcePhysicalDelete) {
                            statement.setObject(
                                    1,
                                    keyData.get(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER));
                        } else {
                            // 复用原绑定逻辑，包括逻辑删除生成的新 cdc_rowid。
                            setParameters(statement, keyData, primaryKeys);
                        }

                        // 当前命令加入 JDBC 批次。
                        statement.addBatch();
                    }

                    // 一次执行当前批次，并检查每条命令的结果。
                    int[] results = statement.executeBatch();
                    if (results == null || results.length != keyRows.size()) {
                        throw new SQLException("批量删除返回结果数量与提交数量不一致");
                    }

                    // 允许影响行数为零，以及驱动返回成功但未提供行数。
                    for (int resultIndex = 0; resultIndex < results.length; resultIndex++) {
                        int affectedRows = results[resultIndex];
                        if (affectedRows < 0 && affectedRows != Statement.SUCCESS_NO_INFO) {
                            throw new SQLException(
                                    "批量删除命令执行失败，批次索引="
                                            + resultIndex + "，返回值=" + affectedRows);
                        }
                    }
                }

                // Statement 正常关闭后提交，提交成功才允许正常返回。
                connection.commit();
            } catch (Exception exception) {
                // 绑定、执行或提交异常时尝试回滚，并保留原始异常。
                try {
                    connection.rollback();
                } catch (Exception rollbackException) {
                    // 回滚失败作为附加异常保留，不覆盖最初的失败原因。
                    exception.addSuppressed(rollbackException);
                }
                throw exception;
            }
        }
    }






}