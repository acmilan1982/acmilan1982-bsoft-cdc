package com.bsoft.server.sql.update;

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.adapter.DorisAdapter;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Doris 数据库更新策略实现（单条更新；WHERE 使用传入的 primaryKeys）
 *
 * 约定：
 * - keys：本次写入涉及的列（包含主键列与待更新列）
 * - primaryKeys：用于定位行的主键列集合（若无主键，你会把 ROWID 放进来）
 *
 * 生成：
 *   UPDATE `table` SET `col1`=?,`col2`=? ... WHERE `pk1`=? AND `pk2`=? ...
 *
 * 绑定顺序：
 *   1) 先绑定所有“非主键列”的值（按 keys 的顺序过滤）
 *   2) 再绑定所有“主键列”的值（按 primaryKeys 的迭代顺序；建议使用 LinkedHashSet 以保持稳定）
 */
public class DorisUpdateStrategy extends AbstractUpdateStrategy  {

    public DorisUpdateStrategy() {
    }

    public DorisUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        // 使用工厂创建适配器
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> keys, Set<String> primaryKeySet, Set<String> dateFieldSet) {
        // 统一参数校验
        validateUpdateParameters(tableName, keys, primaryKeySet);

        // 过滤出可更新列（非主键）
        List<String> updateKeys = keys.stream()
                .filter(k -> !primaryKeySet.contains(k))
                .collect(Collectors.toList());

        if (updateKeys.isEmpty()) {
            throw new IllegalArgumentException("No updatable columns (all provided keys are primary keys) for table: " + tableName);
        }

        String setClause = updateKeys.stream()
                .map(k -> quote(k) + " = ?")
                .collect(Collectors.joining(", "));

        // WHERE 按 primaryKeys 的迭代顺序生成（建议外部传入 LinkedHashSet 以稳定顺序）
        List<String> pkList = new ArrayList<>(primaryKeySet);
        String whereClause = pkList.stream()
                .map(pk -> quote(pk) + " = ?")
                .collect(Collectors.joining(" AND "));

        return String.format("UPDATE %s SET %s WHERE %s", quote(tableName), setClause, whereClause);
    }

    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeys,
                              Set<String> dateFieldSet) throws SQLException {


        int index = 1;

        // 1) 绑定非主键列（SET 部分）
        for (String key : keys) {
            if (!primaryKeys.contains(key)) {
                Object value = data.get(key);
                this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
            }
        }

        // 2) 绑定主键列（WHERE 部分）
        for (String pk : primaryKeys) {
            ps.setObject(index++, data.get(pk));
        }
    }

    /** 反引号转义（Doris/MySQL 协议） */
    private String quote(String ident) {
        return "`" + ident.replace("`", "") + "`";
    }
}
