package com.bsoft.server.sql.insert;

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// MySQL 插入策略，传递日期字段单位，由适配器统一转换为秒级日期。
public class MySQLInsertStrategy extends AbstractInsertStrategy {

    // 根据目标库配置初始化插入策略。
    public MySQLInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        // 初始化数据源持有对象。
        super(targetDataSourceConfig);

        // 创建 MySQL 参数适配器。
        this.adapter =
                DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    // 构建普通参数化 INSERT，日期转换在参数绑定阶段完成。
    @Override
    public String buildInsertSql(String tableName,
                                 List<String> keys,
                                 Set<String> primaryKeys,
                                 Set<String> dateFieldSet) {
        // 保持现有插入列名转小写的处理。
        String columns = String.join(",", keys).toLowerCase(Locale.ROOT);

        // 每个字段使用一个普通占位符，与绑定顺序保持一致。
        List<String> placeholders = new ArrayList<>(keys.size());
        for (String fieldName : keys) {
            // 日期字段和普通字段均通过参数绑定写入。
            placeholders.add("?");
        }

        // 保持原有 INSERT 行为，不增加重复键忽略或更新逻辑。
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                columns,
                String.join(",", placeholders)
        );
    }

    // 保留原参数绑定入口，不为源日期字段猜测单位。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> dateFieldSet) throws SQLException {
        // 缺少源日期单位时，由适配器明确报错。
        setParameters(
                ps,
                data,
                keys,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 按 SQL 字段顺序绑定参数，并向适配器传递日期单位。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> dateFieldSet,
                              Map<String, String> dateFieldUnits) throws SQLException {
        // 校验数据和日期元数据，允许日期集合及单位映射为空。
        Objects.requireNonNull(data, "data 不能为 null");
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 插入字段列表不能为空。
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("columns(keys) is empty");
        }

        // JDBC 参数索引从 1 开始。
        int parameterIndex = 1;

        // 按构建 SQL 时的字段顺序绑定。
        for (String fieldName : keys) {
            // 日期数值按明确单位解释，输出统一精确到秒。
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

    // 声明支持基类提供的 JDBC 批量插入。
    @Override
    public boolean supportsBatch() {
        // 保持现有批量能力不变。
        return true;
    }
}