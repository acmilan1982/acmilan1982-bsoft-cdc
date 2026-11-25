package com.bsoft.server.sql.adapter;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;
import java.util.Set;
import java.time.format.ResolverStyle;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;


// Doris 数据库适配器，按明确单位解析日期数值，并统一输出到秒。
public class DorisAdapter extends AbstractDatabaseAdapter {

    // doris4版本， 全线支持 DateTimeV2 类型
    protected boolean shouldEnableDateTimeV2;

    // 保留现有接口入口，不为源表日期字段猜测时间单位。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet) throws SQLException {
        // 空映射只适用于普通字段、空值及服务端生成的 CDC 时间。
        setParameterValue(
                ps,
                index,
                value,
                fieldName,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 根据当前消息的单位绑定日期参数，所有日期均输出到秒。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet,
                                  Map<String, String> dateFieldUnits) throws SQLException {
        // 元数据使用非 null 集合，没有日期字段时允许为空。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 小写字段名用于查询单位及识别 CDC 时间。
        String normalizedFieldName =
                Objects.requireNonNull(fieldName, "fieldName 不能为 null")
                        .toLowerCase(Locale.ROOT);

        // 日期字段转换成标准字符串后绑定。
        if (isDateField(dateFieldSet, fieldName)) {
            // 空日期直接绑定为 SQL NULL。
            if (value == null) {
                ps.setString(index, null);
                return;
            }

            // CDC 时间由服务端生成，不依赖源表单位。
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

                // ms 仅用于选择合法的转换入口，日期对象及日期字符串不进行数值缩放。
                ps.setString(index, fastParseDateForKafka(value, "ms"));
                return;
            }

            // 非空源日期字段必须提供有效单位。
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

            // 基类按单位解析数值，当前类的格式选择方法保证输出到秒。
            try {
                ps.setString(index, fastParseDateForKafka(value, unit));
            } catch (IllegalArgumentException exception) {
                // 补充字段名并向外抛出，禁止转换失败后继续执行插入。
                throw new IllegalArgumentException(
                        "日期字段转换失败，字段: " + fieldName
                                + "，单位: " + unit,
                        exception
                );
            }
            return;
        }

        // 非日期字段保持原有对象绑定方式，包含 null 值。
        ps.setObject(index, value);
    }
    // Doris 日期统一输出到秒，不保留小数秒。
    private static final DateTimeFormatter DORIS_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);

    // 校验单位后统一返回秒级格式，时间数值仍按原单位解析。
    @Override
    protected DateTimeFormatter getDateTimeFormatterForUnit(String unit) {
        // 复用基类的单位校验，拒绝缺失单位及 ms、us、ns 之外的单位。
        super.getDateTimeFormatterForUnit(unit);

        // 仅改变输出精度，不改变毫秒、微秒、纳秒的解释方式。
        return DORIS_DATETIME_FORMATTER;
    }


    // 判断日期字段，同时支持原字段名及小写字段名。
    private boolean isDateField(Set<String> dateFieldSet, String fieldName) {
        // 空参数不属于日期字段。
        if (dateFieldSet == null || fieldName == null) {
            return false;
        }

        // 支持当前集合中 cdc_dml_Time 等保留大小写的字段名。
        return dateFieldSet.contains(fieldName)
                || dateFieldSet.contains(fieldName.toLowerCase(Locale.ROOT));
    }

    // 识别服务端生成的 CDC 时间字段，参数使用小写字段名。
    private boolean isCdcTimeField(String fieldName) {
        // 仅识别明确约定的 CDC 时间字段。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // CDC 时间不需要源表的单位映射。
                return true;
            default:
                // 其他日期字段必须使用消息中的单位。
                return false;
        }
    }


    /**
     * 将时间戳转换为 Doris 支持的格式
     */
    private String formatTimestampForDoris(Object value) {
        if (value == null) return null;

        try {
            long timestamp;
            if (value instanceof Number) {
                timestamp = ((Number) value).longValue();
            } else if (value instanceof String) {
                timestamp = Long.parseLong((String) value);
            } else {
                return value.toString();
            }

            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(timestamp),
                    ZoneId.systemDefault()
            );
            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (Exception e) {
            e.printStackTrace();
            return value.toString();
        }
    }

    protected Date convertToDateString(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Date) {
                return (Date) value;
            } else if (value instanceof java.time.LocalDateTime) {
                return Date.from(((java.time.LocalDateTime) value)
                        .atZone(java.time.ZoneId.systemDefault()).toInstant());
            } else if (value instanceof java.time.LocalDate) {
                return Date.from(((java.time.LocalDate) value)
                        .atStartOfDay(java.time.ZoneId.systemDefault()).toInstant());
            } else if (value instanceof String) {
                String strValue = ((String) value).trim();
                if (strValue.isEmpty()) {
                    return null;
                }
                // 如果是纯数字，按时间戳处理
                if (strValue.matches("^-?\\d+$")) {
                    long timestamp = Long.parseLong(strValue);
                    return new Date(timestamp);
                }
                // 支持常见日期格式
                java.time.format.DateTimeFormatter[] formatters = new java.time.format.DateTimeFormatter[] {
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
                };
                for (java.time.format.DateTimeFormatter formatter : formatters) {
                    try {
                        java.time.LocalDateTime ldt;
                        if (formatter.toString().contains("HH")) {
                            ldt = java.time.LocalDateTime.parse(strValue, formatter);
                        } else {
                            ldt = java.time.LocalDate.parse(strValue, formatter).atStartOfDay();
                        }
                        return Date.from(ldt.atZone(java.time.ZoneId.systemDefault()).toInstant());
                    } catch (Exception ignore) {}
                }
            }
        } catch (Exception e) {
            System.err.println("日期格式转换失败，字段值: " + value + ", 错误: " + e.getMessage());
            return null;
        }
        return null;
    }

}
