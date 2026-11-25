package com.bsoft.server.sql.insert;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 定义数据库插入策略，支持携带日期字段单位并兼容原有实现。
public interface InsertStrategy {

    // 按原有规则构建插入 SQL，供现有实现和无单位调用使用。
    String buildInsertSql(String tableName,
                          List<String> keys,
                          Set<String> primaryKeys,
                          Set<String> dateFieldSet);

    // 根据日期字段单位构建插入 SQL，具体数据库可重写此方法。
    default String buildInsertSql(String tableName,
                                  List<String> keys,
                                  Set<String> primaryKeys,
                                  Set<String> dateFieldSet,
                                  Map<String, String> dateFieldUnits) {
        // 默认委托原方法，保持尚未支持单位映射的数据库行为不变。
        return buildInsertSql(tableName, keys, primaryKeys, dateFieldSet);
    }

    // 按原有规则绑定插入参数，供现有实现和无单位调用使用。
    void setParameters(PreparedStatement ps,
                       Map<String, Object> data,
                       List<String> keys,
                       Set<String> dateFieldSet) throws SQLException;

    // 根据当前消息的日期字段单位绑定插入参数，具体数据库可重写此方法。
    default void setParameters(PreparedStatement ps,
                               Map<String, Object> data,
                               List<String> keys,
                               Set<String> dateFieldSet,
                               Map<String, String> dateFieldUnits) throws SQLException {
        // 默认委托原方法，兼容现有参数绑定实现。
        setParameters(ps, data, keys, dateFieldSet);
    }

    // 返回当前策略是否支持批量插入。
    default boolean supportsBatch() {
        // 保持默认支持批量插入的现有约定。
        return true;
    }

    // 执行批量插入，并返回各条记录的处理状态。
    Map<Long, String> executeBatchInsert(String tableName,
                                         List<BatchInsertItem> batchItems,
                                         BatchSizeManager batchSizeManager,
                                         List<ColumnInfo> allColumns,
                                         boolean needPadding) throws Exception;

    // 根据单条消息执行插入。
    void executeSingleInsert(String tableName,
                             KafkaPayload payload,
                             List<ColumnInfo> allColumns) throws Exception;



    // 执行批量新增，按输入位置返回每条消息的处理状态。
// 返回列表长度必须等于 batchItems 长度，第 i 项对应输入的第 i 条消息。
// 即使消息来自不同 topic 或具有相同 offset，也必须独立记录结果。
// 实现不得修改输入顺序；绑定失败的记录也必须保留对应结果位置。
// 批次执行失败或提交结果无法确认时抛出异常，由调用方记录整批失败。
    default List<String> executeBatchInsertOrdered(
            String tableName,
            List<BatchInsertItem> batchItems,
            BatchSizeManager batchSizeManager,
            List<ColumnInfo> allColumns,
            boolean needPadding) throws Exception {
        // 必须由具体策略实现，不能从可能已发生覆盖的旧 offset Map 转换结果。
        throw new UnsupportedOperationException(
                "当前新增策略尚未实现按输入顺序返回结果的批量入口: "
                        + getClass().getName());
    }
}