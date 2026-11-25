package com.bsoft.server.sql.update;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 定义数据库更新策略，支持明确日期单位、目标字段元数据及批量更新入口。
public interface UpdateStrategy {

    // 保留现有 SQL 构建入口，供 Oracle、MySQL 等 JDBC 更新策略使用。
    String buildUpdateSql(String tableName,
                          List<String> keys,
                          Set<String> primaryKeySet,
                          Set<String> dateFieldSet);

    // 根据明确日期单位构建更新 SQL，具体策略按需要重写。
    default String buildUpdateSql(String tableName,
                                  List<String> keys,
                                  Set<String> primaryKeySet,
                                  Set<String> dateFieldSet,
                                  Map<String, String> dateFieldUnits) {
        // 默认沿用原 SQL 构建入口，日期表达式由具体策略处理。
        return buildUpdateSql(
                tableName,
                keys,
                primaryKeySet,
                dateFieldSet
        );
    }

    // 保留现有 JDBC 参数绑定入口。
    void setParameters(PreparedStatement statement,
                       Map<String, Object> data,
                       List<String> keys,
                       Set<String> primaryKeySet,
                       Set<String> dateFieldSet) throws SQLException;

    // 绑定 JDBC 参数，并向具体策略传递明确日期单位。
    default void setParameters(PreparedStatement statement,
                               Map<String, Object> data,
                               List<String> keys,
                               Set<String> primaryKeySet,
                               Set<String> dateFieldSet,
                               Map<String, String> dateFieldUnits) throws SQLException {
        // 默认沿用原参数绑定入口，已接入日期单位的策略重写本方法。
        setParameters(
                statement,
                data,
                keys,
                primaryKeySet,
                dateFieldSet
        );
    }

    // 保留现有单条更新入口，更新失败必须抛出异常。
    void executeUpdate(String tableName,
                       KafkaPayload payload) throws Exception;

    // 根据目标字段元数据执行单条更新，Doris 用于过滤、补齐和归一化字段。
    default void executeUpdate(String tableName,
                               KafkaPayload payload,
                               List<ColumnInfo> allColumns) throws Exception {
        // 不依赖目标字段元数据的策略继续使用现有单条更新实现。
        executeUpdate(tableName, payload);
    }

    // 执行一批更新，调用方必须保持同一行身份的事件顺序。
    // 正常返回表示本次调用确认成功，失败或结果无法确认时必须抛出异常。
    default void executeBatchUpdate(String tableName,
                                    List<KafkaPayload> payloads,
                                    List<ColumnInfo> allColumns) throws Exception {
        // 默认不模拟批量操作，只有显式实现批量更新的策略才能使用该入口。
        throw new UnsupportedOperationException(
                "当前更新策略尚未实现批量更新: " + getClass().getName()
        );
    }

    // 表示当前策略是否实现了 executeBatchUpdate，供调用方选择执行入口。
    default boolean supportsBatch() {
        // 默认不支持批量更新，Doris 实现批量入口后显式返回 true。
        return false;
    }
}