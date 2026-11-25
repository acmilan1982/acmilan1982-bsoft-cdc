package com.bsoft.server.sql.update;

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

// Oracle 更新策略，根据明确的日期单位构建 SQL，并复用 OracleAdapter 绑定参数。
public class OracleUpdateStrategy extends AbstractUpdateStrategy {

    // 保留现有无参构造入口，执行前需完成数据源和适配器初始化。
    public OracleUpdateStrategy() {
        // 调用基类无参构造方法。
        super();
    }

    // 根据目标库配置初始化 Oracle 更新策略。
    public OracleUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        // 初始化基类的数据源持有对象。
        super(targetDataSourceConfig);

        // 创建与目标库对应的适配器。
        this.adapter =
                DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
    }

    // 保留原接口入口，不为源日期字段猜测单位。
    @Override
    public String buildUpdateSql(String tableName,
                                 List<String> keys,
                                 Set<String> primaryKeySet,
                                 Set<String> dateFieldSet) {
        // 源日期字段缺少单位时，由新入口明确报错。
        return buildUpdateSql(
                tableName,
                keys,
                primaryKeySet,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 根据日期字段单位构建 UPDATE，保持 SQL 与参数绑定顺序一致。
    @Override
    public String buildUpdateSql(String tableName,
                                 List<String> keys,
                                 Set<String> primaryKeySet,
                                 Set<String> dateFieldSet,
                                 Map<String, String> dateFieldUnits) {
        // 保留更新操作的必要参数校验。
        validateUpdateParameters(tableName, keys, primaryKeySet);

        // 日期元数据必须为非 null 集合，没有日期字段时允许为空。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 按原字段顺序构建 SET 表达式，排除用于定位记录的字段。
        List<String> assignments = new ArrayList<>();
        for (String fieldName : keys) {
            if (!primaryKeySet.contains(fieldName)) {
                assignments.add(buildFieldExpression(
                        fieldName,
                        dateFieldSet,
                        dateFieldUnits
                ));
            }
        }

        // 没有可更新字段时明确报错。
        if (assignments.isEmpty()) {
            throw new IllegalArgumentException(
                    "No update columns available for table: " + tableName
            );
        }

        // WHERE 字段按固定顺序排列，与参数绑定顺序保持一致。
        List<String> primaryKeys = new ArrayList<>(primaryKeySet);
        primaryKeys.sort(String::compareTo);

        // 构建定位条件，实际更新链路使用 cdc_rowid。
        List<String> conditions = new ArrayList<>();
        for (String fieldName : primaryKeys) {
            conditions.add(buildFieldExpression(
                    fieldName,
                    dateFieldSet,
                    dateFieldUnits
            ));
        }

        // 保持现有表名使用方式，列名由表达式方法统一转为大写。
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableName,
                String.join(", ", assignments),
                String.join(" AND ", conditions)
        );
    }

    // 保留原参数绑定入口，不为源日期值猜测单位。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet) throws SQLException {
        // 源日期值缺少单位时，由 OracleAdapter 明确报错。
        setParameters(
                ps,
                data,
                keys,
                primaryKeySet,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 先绑定 SET 参数，再绑定 WHERE 参数，并传递同一份日期单位映射。
    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet,
                              Map<String, String> dateFieldUnits) throws SQLException {
        // 校验日期元数据，禁止通过 null 隐式回退。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // JDBC 参数索引从 1 开始。
        int parameterIndex = 1;

        // SET 参数按 keys 中非定位字段的顺序绑定。
        for (String fieldName : keys) {
            if (primaryKeySet.contains(fieldName)) {
                continue;
            }

            // 复用插入时已验证的日期转换和参数绑定逻辑。
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

        // WHERE 参数使用与 SQL 构建相同的排序规则。
        List<String> primaryKeys = new ArrayList<>(primaryKeySet);
        primaryKeys.sort(String::compareTo);

        // 定位字段也通过相同适配器绑定，保证表达式和参数类型一致。
        for (String fieldName : primaryKeys) {
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

    // 构建字段等值表达式，同时用于 SET 赋值和 WHERE 定位条件。
    private String buildFieldExpression(String fieldName,
                                        Set<String> dateFieldSet,
                                        Map<String, String> dateFieldUnits) {
        // 使用原字段名判断集合归属，小写名称用于单位查询。
        String normalizedFieldName = fieldName.toLowerCase(Locale.ROOT);
        String columnName = fieldName.toUpperCase(Locale.ROOT);

        // 日期判断与 OracleAdapter 保持一致。
        boolean dateField = dateFieldSet.contains(fieldName)
                || dateFieldSet.contains(normalizedFieldName);

        // 普通字段使用直接绑定的参数。
        if (!dateField) {
            return columnName + " = ?";
        }

        // CDC 时间由服务端生成，统一按秒级日期处理。
        if (isCdcTimeField(normalizedFieldName)) {
            return columnName
                    + " = TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')";
        }

        // 源日期字段必须提供明确单位。
        String unit = dateFieldUnits.get(normalizedFieldName);
        if (unit == null) {
            throw new IllegalArgumentException(
                    "日期字段缺少时间单位，字段: " + fieldName
            );
        }

        // SQL 格式与 OracleAdapter 输出的日期字符串保持一致。
        switch (unit) {
            case "ms":
                // 毫秒单位按当前约定舍弃小数秒。
                return columnName
                        + " = TO_DATE(?, 'YYYY-MM-DD HH24:MI:SS')";
            case "us":
                // 微秒单位保留 6 位小数秒。
                return columnName
                        + " = TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF6')";
            case "ns":
                // 纳秒单位保留 9 位小数秒。
                return columnName
                        + " = TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF9')";
            default:
                // 非法单位附带字段名报错，禁止猜测。
                throw new IllegalArgumentException(
                        "日期字段单位无效，字段: " + fieldName
                                + "，单位: " + unit
                                + "，仅支持 ms、us、ns"
                );
        }
    }

    // 识别服务端生成的 CDC 时间字段，范围与 OracleAdapter 保持一致。
    private boolean isCdcTimeField(String fieldName) {
        // 仅识别明确约定的时间字段。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // CDC 时间不依赖源表的日期单位。
                return true;
            default:
                // 其他日期字段必须提供单位。
                return false;
        }
    }
}