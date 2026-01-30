package com.bsoft.server.sql.update;

/**
 * ClassName: MySQLUpdateStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:10
 */


import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.adapter.MySQLAdapter;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MySQL数据库更新策略实现
 */
public class MySQLUpdateStrategy extends AbstractUpdateStrategy  {

    public MySQLUpdateStrategy() {
    }

    public MySQLUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        // 使用工厂创建适配器
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> keys, Set<String> primaryKeySet, Set<String> dateFieldSet) {
        // 统一参数校验
        validateUpdateParameters(tableName, keys, primaryKeySet);

        // 过滤掉主键字段，只更新非主键字段
        List<String> updateKeys = keys.stream()
                .filter(key -> !primaryKeySet.contains(key))
                .collect(Collectors.toList());

        // 没有可更新字段
        if (updateKeys.isEmpty()) {
            throw new IllegalArgumentException("No update columns available for table: " + tableName);
        }

        // 构建UPDATE语句
        String setClause = updateKeys.stream()
                .map(key -> key + " = ?")
                .collect(Collectors.joining(", "));

        String whereClause = primaryKeySet.stream()
                .map(pk -> pk + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
    }

    @Override
    public void setParameters(PreparedStatement ps, Map<String, Object> data,
                              List<String> keys, Set<String> primaryKeys, Set<String> dateFieldSet)
            throws SQLException {
        int index = 1;

        // 设置SET子句的参数（非主键字段）
        for (String key : keys) {
            if (!primaryKeys.contains(key)) {
                Object value = data.get(key);
                // 使用父类的通用方法处理时间戳转换
                this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
            }
        }

        // 设置WHERE子句的参数（主键字段）- 主键通常不是时间戳，直接设置
        for (String pk : primaryKeys) {
            ps.setObject(index++, data.get(pk));
        }
    }
}