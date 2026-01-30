package com.bsoft.server.sql.update;

/**
 * ClassName: OracleUpdateStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:11
 */


import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import com.bsoft.server.sql.insert.OracleInsertStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Oracle数据库更新策略实现
 */
public class OracleUpdateStrategy extends AbstractUpdateStrategy  {

    private static final Logger logger = LogManager.getLogger(OracleUpdateStrategy.class);


    public OracleUpdateStrategy() {
    }

    public OracleUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        // 使用工厂创建适配器
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> keys,  Set<String> primaryKeySet, Set<String> dateFieldSet) {
        // 1.统一参数校验
        validateUpdateParameters(tableName, keys, primaryKeySet);

        // 2.可更新列：非主键
        List<String> updateKeys = keys.stream()
                .filter(k -> !primaryKeySet.contains(k))
                .collect(Collectors.toList());

        if (updateKeys.isEmpty()) {
            throw new IllegalArgumentException("No update columns available for table: " + tableName);
        }

        // 构建UPDATE语句
        // 3. SET：date 字段用 TO_DATE
        String setClause = updateKeys.stream()
                .map(k -> {
                    String col = k.toUpperCase();
                    if (dateFieldSet != null && dateFieldSet.contains(k)) {
                        return col + " = TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss')";
                    }
                    return col + " = ?";
                })
                .collect(Collectors.joining(", "));

        // 4. WHERE：固定顺序（字典序），避免 Set 迭代顺序漂移
        List<String> pkList = new ArrayList<>(primaryKeySet);
        pkList.sort(String::compareTo);

        String whereClause = pkList.stream()
                .map(pk -> {
                    String col = pk.toUpperCase();
                    // 理论上主键不该是 date，这里仍做通用支持
                    if (dateFieldSet != null && dateFieldSet.contains(pk)) {
                        return col + " = TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss')";
                    }
                    return col + " = ?";
                })
                .collect(Collectors.joining(" AND "));

        return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
    }

    @Override
    public void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keys,
                              Set<String> primaryKeySet, Set<String> dateFieldSet) throws SQLException {


        int index = 1;

        // 1) SET 子句参数（顺序必须与 buildUpdateSql 中 updateKeys 顺序一致）
        for (String key : keys) {
            if (!primaryKeySet.contains(key)) {
                Object value = data.get(key);
                // 复用同一套日期处理：epoch -> UTC 文本；字符串 -> 原样；最终 ps.setString(...) 适配 TO_DATE
                this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
            }
        }

        // 2) WHERE 子句参数：必须与 buildUpdateSql 中 pkList 的顺序一致（建议内部排序）
        List<String> pkList = new ArrayList<>(primaryKeySet);
        pkList.sort(String::compareTo);

        for (String pk : pkList) {
            Object value = data.get(pk);

            // - 若 pk 是 date 字段：会走 setString（配合 SQL 中 TO_DATE）
            // - 若 pk 不是 date 字段：会走 setObject/setString
            this.adapter.setParameterValue(ps, index++, value, pk, dateFieldSet);

        }
    }

}