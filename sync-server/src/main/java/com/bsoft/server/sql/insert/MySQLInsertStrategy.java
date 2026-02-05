package com.bsoft.server.sql.insert;

/**
 * ClassName: MySQLInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:23
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
 * MySQL数据库插入策略实现
 */
public class MySQLInsertStrategy extends AbstractInsertStrategy  {

    public MySQLInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }
    @Override
    public String buildInsertSql(String tableName, List<String> keys, Set<String> primaryKeys, Set<String> dateFieldSet) {
        String columns = String.join(",", keys).toLowerCase();
        String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(","));

        // MySQL使用INSERT IGNORE来处理重复键冲突
        return String.format("INSERT  INTO %s (%s) VALUES (%s)",
                tableName, columns, placeholders);
    }




    @Override
    public void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keys, Set<String> dateFieldSet)
            throws SQLException {
        int index = 1;
        for (String key : keys) {
            Object value = data.get(key);
            // 使用父类的通用方法处理时间戳转换
            this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
        }
    }

    @Override
    public boolean supportsBatch() {
        return true;
    }
}