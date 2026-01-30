package com.bsoft.server.sql.update;

/**
 * ClassName: UpdateStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:10
 */


import com.bsoft.common.bean.KafkaPayload;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据库更新策略接口
 * 用于处理不同数据库的更新操作差异
 */
public interface UpdateStrategy {

    /**
     * 构建更新SQL语句
     * @param tableName 表名
     * @param keys 所有字段列表（包含主键）
     * @param primaryKeySet 主键字段集合
     * @return 更新SQL语句
     */
    String buildUpdateSql(String tableName, List<String> keys, Set<String> primaryKeySet, Set<String> dateFieldSet);

    /**
     * 设置PreparedStatement参数
     * @param ps PreparedStatement对象
     * @param data 数据映射
     * @param keys 字段列表
     * @param primaryKeySet 主键字段集合
     * @throws SQLException SQL异常
     */
    void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keys, Set<String> primaryKeySet, Set<String> dateFieldSet) throws SQLException;


    /**
     * 执行单条更新操作
     * @param tableName 表名
     * @param payload Kafka消息负载
     * @throws Exception 执行异常
     */
    void executeUpdate(String tableName, KafkaPayload payload) throws Exception;


    /**
     * 是否支持批量操作
     * @return true支持，false不支持
     */
    default boolean supportsBatch() {
        return true;
    }
}