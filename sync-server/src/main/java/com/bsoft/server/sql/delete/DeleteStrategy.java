package com.bsoft.server.sql.delete;

/**
 * ClassName: DeleteStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:23
 */



import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.enums.TableRowDeleteStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * 数据库删除策略接口
 * 用于处理不同删除方式的操作差异
 */
public interface DeleteStrategy {

    /**
     * 构建删除SQL语句
     * @param tableName 表名
     * @param primaryKeys 主键字段集合
     * @return 删除SQL语句
     */
    String buildDeleteSql(String tableName, Set<String> primaryKeys);

    /**
     * 设置PreparedStatement参数
     * @param ps PreparedStatement对象
     * @param beforeData 删除前数据（包含主键值）
     * @param primaryKeys 主键字段集合
     * @throws SQLException SQL异常
     */
    void setParameters(PreparedStatement ps, Map<String, Object> beforeData, Set<String> primaryKeys) throws SQLException;

    /**
     * 执行单条删除操作
     * @param tableName 表名
     * @param payload Kafka消息负载
     * @throws Exception 执行异常
     */
    void executeDelete(String tableName, KafkaPayload payload) throws Exception;

}