package com.bsoft.server.sql.delete;

/**
 * ClassName: MySQLDeleteStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:24
 */


import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.TableRowDeleteStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL数据库删除策略实现
 */
public class MySQLDeleteStrategy extends AbstractDeleteStrategy {

    public MySQLDeleteStrategy() {
    }

    public MySQLDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }
//
//    @Override
//    public String buildDeleteSql(String tableName, Set<String> primaryKeys, TableRowDeleteStrategy deleteStrategy) {
//        // 没有主键，无法安全删除
//        if (primaryKeys.isEmpty()) {
//            throw new IllegalArgumentException("Primary key set is empty, unsafe to delete from table: " + tableName);
//        }
//
//        switch (deleteStrategy) {
//            case DELETE:
//                // 物理删除
//                String whereClause = primaryKeys.stream()
//                        .map(pk -> pk + " = ?")
//                        .collect(Collectors.joining(" AND "));
//                return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
//
//            case DELETE_FLAG:
//                // 逻辑删除：更新删除标记
//                String whereClauseUpdate = primaryKeys.stream()
//                        .map(pk -> pk + " = ?")
//                        .collect(Collectors.joining(" AND "));
//                return String.format("UPDATE %s SET cdc_op = 'd' ,cdc_delete_flag = 1  ,cdc_delete_time = NOW() WHERE %s", tableName, whereClauseUpdate);
//
//            default:
//                throw new IllegalArgumentException("不支持的删除策略: " + deleteStrategy);
//        }
//    }
//
//    @Override
//    public void setParameters(PreparedStatement ps, Map<String, Object> beforeData, Set<String> primaryKeys,
//                              TableRowDeleteStrategy deleteStrategy) throws SQLException {
//        int index = 1;
//
//        // 设置WHERE子句的主键参数
//        for (String pk : primaryKeys) {
//            Object value = beforeData.get(pk);
//            ps.setObject(index++, value);
//        }
//    }
}