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

    public MySQLDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig,TableRowDeleteStrategy tableRowDeleteStrategy) {
        super(targetDataSourceConfig,tableRowDeleteStrategy);
    }

    @Override
    public String buildDeleteSql(String tableName, Set<String> primaryKeySet) {
        // 没有主键，无法安全删除
        validatePrimaryKeySet(tableName,primaryKeySet);
        // WHERE：固定顺序（字典序），避免 Set 迭代顺序漂移
        String whereClause = buildWhereClause(primaryKeySet);
        switch (tableRowDeleteStrategy) {
            case DELETE:
                // 物理删除
                return String.format("delete from %s where %s", tableName, whereClause);
            case DELETE_FLAG:
                // 逻辑删除：更新删除标记
                return String.format("update %s set cdc_op = 'd' ,cdc_delete_flag = 1  ,cdc_delete_time = NOW() where %s", tableName, whereClause);

            default:
                throw new IllegalArgumentException("不支持的删除策略: " + tableRowDeleteStrategy);
        }
    }
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