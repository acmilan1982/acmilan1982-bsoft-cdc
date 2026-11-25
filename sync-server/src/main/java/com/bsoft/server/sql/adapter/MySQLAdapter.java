package com.bsoft.server.sql.adapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// MySQL 适配器，按明确单位解释日期数值，并统一输出到秒。
public class MySQLAdapter extends AbstractDatabaseAdapter {

    // 使用秒级格式，不向 MySQL 传递小数秒。
    private static final DateTimeFormatter MYSQL_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);

    // 校验单位后统一选择秒级输出格式，数值换算仍使用原单位。
    @Override
    protected DateTimeFormatter getDateTimeFormatterForUnit(String unit) {
        // 复用基类校验，拒绝缺失单位或非法单位。
        super.getDateTimeFormatterForUnit(unit);

        // 毫秒、微秒、纳秒均输出到秒，小数部分直接舍弃。
        return MYSQL_DATETIME_FORMATTER;
    }

    // 保留现有接口入口，不为源日期字段猜测单位。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet) throws SQLException {
        // 空单位映射只适用于普通字段、空值及服务端 CDC 时间。
        setParameterValue(
                ps,
                index,
                value,
                fieldName,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 根据日期字段单位绑定参数，源日期和 CDC 时间分别处理。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet,
                                  Map<String, String> dateFieldUnits) throws SQLException {
        // 日期元数据必须非 null，没有源日期字段时允许为空集合。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 使用小写字段名查询单位并识别 CDC 时间。
        String normalizedFieldName =
                Objects.requireNonNull(fieldName, "fieldName 不能为 null")
                        .toLowerCase(Locale.ROOT);

        // 同时支持原字段名和小写字段名，兼容 cdc_dml_Time 的现有形式。
        boolean dateField = dateFieldSet.contains(fieldName)
                || dateFieldSet.contains(normalizedFieldName);

        // 非日期字段保持原有对象绑定方式。
        if (!dateField) {
            ps.setObject(index, value);
            return;
        }

        // 空日期绑定为 SQL NULL，不执行单位换算。
        if (value == null) {
            ps.setString(index, null);
            return;
        }

        // CDC 时间由服务端生成，不依赖源字段单位。
        if (isCdcTimeField(normalizedFieldName)) {
            // CDC 时间应为日期对象或日期字符串，拒绝数字时间戳。
            if (value instanceof Number
                    || (value instanceof String
                    && ((String) value).trim().matches("[+-]?[0-9]+"))) {
                throw new IllegalArgumentException(
                        "CDC 时间字段必须使用日期对象或日期字符串，字段: "
                                + fieldName
                );
            }

            // ms 仅用于进入合法转换入口，日期对象及日期字符串不进行数值缩放。
            ps.setString(index, fastParseDateForKafka(value, "ms"));
            return;
        }

        // 非空源日期字段必须提供明确单位，不允许根据数字位数猜测。
        String unit = dateFieldUnits.get(normalizedFieldName);
        if (!"ms".equals(unit)
                && !"us".equals(unit)
                && !"ns".equals(unit)) {
            throw new IllegalArgumentException(
                    "日期字段缺少有效时间单位，字段: " + fieldName
                            + "，单位: " + unit
                            + "，仅支持 ms、us、ns"
            );
        }

        // 基类按明确单位换算，当前适配器保证输出秒级字符串。
        try {
            ps.setString(index, fastParseDateForKafka(value, unit));
        } catch (IllegalArgumentException exception) {
            // 转换失败时附带字段上下文并向外抛出，不尝试其他单位。
            throw new IllegalArgumentException(
                    "MySQL 日期字段转换失败，字段: " + fieldName
                            + "，单位: " + unit,
                    exception
            );
        }
    }

    // 识别服务端生成的 CDC 时间字段，参数使用小写字段名。
    private boolean isCdcTimeField(String fieldName) {
        // 仅识别明确约定的时间字段。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // CDC 时间不要求源表单位元数据。
                return true;
            default:
                // 其他日期字段必须使用消息中的单位。
                return false;
        }
    }
}