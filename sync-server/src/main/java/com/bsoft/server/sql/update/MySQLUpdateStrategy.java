package com.bsoft.server.sql.update;

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// MySQL 更新策略，按明确单位转换日期，写入精确到秒的日期值。
public class MySQLUpdateStrategy extends AbstractUpdateStrategy {

    // 保留无参构造入口，执行前需完成数据源和适配器初始化。
    public MySQLUpdateStrategy() {
        // 调用基类无参构造方法。
        super();
    }

    // 根据目标库配置初始化更新策略。
    public MySQLUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        // 初始化数据源持有对象。
        super(targetDataSourceConfig);

        // 创建 MySQL 参数适配器。
        this.adapter =
                DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    // 构建普通参数化 UPDATE，日期转换在参数绑定阶段完成。
    @Override
    public String buildUpdateSql(String tableName,
                                 List<String> keys,
                                 Set<String> primaryKeySet,
                                 Set<String> dateFieldSet) {
        // 校验表名、字段列表及定位字段，避免生成无条件更新。
        validateUpdateParameters(tableName, keys, primaryKeySet);

        // 按原字段顺序构建 SET，排除用于定位记录的字段。
        List<String> assignments = new ArrayList<>();
        for (String fieldName : keys) {
            if (!primaryKeySet.contains(fieldName)) {
                // 日期字段和普通字段均使用普通占位符。
                assignments.add(fieldName + " = ?");
            }
        }

        // 没有可更新字段时明确报错。
        if (assignments.isEmpty()) {
            throw new IllegalArgumentException(
                    "No update columns available for table: " + tableName
            );
        }

        // 对定位字段排序，确保 SQL 和参数绑定采用相同顺序。
        List<String> primaryKeys = new ArrayList<>(primaryKeySet);
        primaryKeys.sort(String::compareTo);

        // 构建 WHERE，实际更新链路使用 cdc_rowid 定位。
        List<String> conditions = new ArrayList<>();
        for (String fieldName : primaryKeys) {
            // 定位条件使用参数化表达式。
            conditions.add(fieldName + " = ?");
        }

        // 返回更新 SQL，不依赖 MySQL 日期转换函数。
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableName,
                String.join(", ", assignments),
                String.join(" AND ", conditions)
        );
    }

    // 保留原参数绑定入口，不为源日期字段猜测单位。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet) throws SQLException {
        // 缺少源日期单位时，由适配器明确报错。
        setParameters(
                ps,
                data,
                keys,
                primaryKeySet,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 先绑定 SET 参数，再绑定 WHERE 参数，并传递日期单位映射。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet,
                              Map<String, String> dateFieldUnits) throws SQLException {
        // 校验数据及日期元数据，没有日期字段时允许为空集合。
        Objects.requireNonNull(data, "data 不能为 null");
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 校验参数顺序所依赖的字段列表和定位字段集合。
        Objects.requireNonNull(keys, "keys 不能为 null");
        Objects.requireNonNull(primaryKeySet, "primaryKeySet 不能为 null");

        // JDBC 参数索引从 1 开始。
        int parameterIndex = 1;

        // 按 SQL 中 SET 表达式的字段顺序绑定。
        for (String fieldName : keys) {
            if (primaryKeySet.contains(fieldName)) {
                continue;
            }

            // 日期按明确单位解释，最终统一输出到秒。
            this.adapter.setParameterValue(
                    ps,
                    parameterIndex,
                    data.get(fieldName),
                    fieldName,
                    dateFieldSet,
                    dateFieldUnits
            );

            // 移动到下一个参数位置。
            parameterIndex++;
        }

        // WHERE 使用与 SQL 构建相同的定位字段排序规则。
        List<String> primaryKeys = new ArrayList<>(primaryKeySet);
        primaryKeys.sort(String::compareTo);

        // 按排序后的字段顺序绑定定位条件。
        for (String fieldName : primaryKeys) {
            // 普通定位字段原样绑定，日期定位字段也使用明确单位。
            this.adapter.setParameterValue(
                    ps,
                    parameterIndex,
                    data.get(fieldName),
                    fieldName,
                    dateFieldSet,
                    dateFieldUnits
            );

            // 移动到下一个参数位置。
            parameterIndex++;
        }
    }
}