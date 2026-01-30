package com.bsoft.server.sql.insert;

/**
 * ClassName: InsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:23
 */

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据库插入策略接口
 * 用于处理不同数据库的插入操作差异
 */
public interface InsertStrategy {

    /**
     * 构建插入SQL语句
     * @param tableName 表名
     * @param keys 字段列表
     * @param primaryKeys 主键字段集合
     * @return 插入SQL语句
     */
    String buildInsertSql(String tableName, List<String> keys, Set<String> primaryKeys,Set<String> dateFieldSet);

    /**
     * 设置PreparedStatement参数
     * @param ps PreparedStatement对象
     * @param data 数据映射
     * @param keys 字段列表
     * @throws SQLException SQL异常
     */
    void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keys,Set<String> dateFieldSet) throws SQLException;


    /**
     * 是否支持批量操作
     * @return true支持，false不支持
     */
    default boolean supportsBatch() {
        return true;
    }

    // 批量插入-快照阶段
    Map<Long, String> executeBatchInsert(String tableName,
                                         List<BatchInsertItem> batchItems,
                                         BatchSizeManager batchSizeManager,
                                         List<ColumnInfo> allColumns,
                                         boolean needPadding) throws Exception;
    // 单条插入-增量阶段
    void executeSingleInsert(String tableName, KafkaPayload payload) throws Exception;



}
