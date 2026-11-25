package com.bsoft.server.sql.adapter;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.TargetDataSourceConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

// Oracle 数据库适配器，负责参数绑定及日期字段的单位转换。
public class OracleAdapter extends AbstractDatabaseAdapter {
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter FMT_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FMT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public OracleAdapter() {
    }


    // 保留接口要求的无单位入口，不为源表日期字段猜测时间单位。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet) throws SQLException {
        // 无单位入口只能处理普通字段、空值及服务端生成的 CDC 时间字段。
        setParameterValue(
                ps,
                index,
                value,
                fieldName,
                dateFieldSet,
                Collections.emptyMap()
        );
    }
    // 根据日期字段单位绑定参数，源表日期与服务端 CDC 时间分别处理。
    @Override
    public void setParameterValue(PreparedStatement ps,
                                  int index,
                                  Object value,
                                  String fieldName,
                                  Set<String> dateFieldSet,
                                  Map<String, String> dateFieldUnits) throws SQLException {
        // 元数据必须使用非 null 集合，没有日期字段时允许为空。
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 保留原字段名用于数据定位，小写名称用于单位查找及 CDC 字段识别。
        String normalizedFieldName =
                Objects.requireNonNull(fieldName, "fieldName 不能为 null")
                        .toLowerCase(Locale.ROOT);

        // 兼容当前 CDC 字段集合中存在 cdc_dml_Time 的大小写形式。
        boolean dateField = dateFieldSet.contains(fieldName)
                || dateFieldSet.contains(normalizedFieldName);

        // 日期字段通过 SQL 日期函数解析，因此参数绑定为字符串。
        if (dateField) {
            // 空日期直接绑定为 SQL NULL，不进行数值转换。
            if (value == null) {
                ps.setString(index, null);
                return;
            }

            // CDC 时间字段不依赖源表单位，统一输出到秒。
            if (isCdcTimeField(normalizedFieldName)) {
                // 服务端生成的 CDC 时间不应是数字时间戳。
                if (value instanceof Number
                        || (value instanceof String
                        && ((String) value).trim().matches("[+-]?[0-9]+"))) {
                    throw new IllegalArgumentException(
                            "CDC 时间字段必须使用日期对象或日期字符串，字段: "
                                    + fieldName
                    );
                }

                // ms 在此只用于选择秒级输出格式，不对日期字符串或日期对象缩放。
                ps.setString(index, fastParseDateForKafka(value, "ms"));
                return;
            }

            // 源表日期字段必须提供明确单位，不再兼容缺少单位的消息。
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

            // 按明确单位解释时间值，并按 Oracle 插入约定保留对应精度。
            try {
                ps.setString(index, fastParseDateForKafka(value, unit));
            } catch (IllegalArgumentException exception) {
                // 补充字段名，便于从批量绑定错误中定位具体字段。
                throw new IllegalArgumentException(
                        "日期字段转换失败，字段: " + fieldName
                                + "，单位: " + unit,
                        exception
                );
            }
            return;
        }

        // 非日期字段的空值保持现有绑定方式。
        if (value == null) {
            ps.setObject(index, null);
            return;
        }

        // 字符串直接绑定。
        if (value instanceof String) {
            ps.setString(index, (String) value);
            return;
        }

        // 非日期字段中的 LocalDateTime 保持现有字符串绑定方式。
        if (value instanceof LocalDateTime) {
            ps.setString(index, ((LocalDateTime) value).toString());
            return;
        }

        // 其他类型保持原有对象绑定方式。
        ps.setObject(index, value);
    }



    // 识别服务端生成的 CDC 时间字段，参数使用小写字段名。
    private boolean isCdcTimeField(String fieldName) {
        // 只识别明确约定的字段，不将所有 cdc_ 前缀字段都当作时间。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // 服务端日期值不需要源表的时间单位信息。
                return true;
            default:
                // 其他字段按照普通字段或源表日期字段处理。
                return false;
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

    public static void main(String[] args) throws SQLException {

        // 1. 连接信息
        String url = "jdbc:oracle:thin:@192.168.174.65:1521/prod.enmotech.com";
        String username = "spring_db";
        String password = "spring_db";
//
//        // 2. 示例 epoch（你也可以换成 Debezium 产生的值）
//        long epochMillis = 1767988311000L; // 2026-01-09 19:51:51 UTC
//
//        String s = "1767988311000";
//        long t = Long.parseLong(s);
//        Date date = new Date(t);


        OracleAdapter oracleAdapter = new OracleAdapter();
        String EPOCH_DATE = oracleAdapter.fastParseDateForKafka("1767988311000");
        String STRING_DATE = oracleAdapter.fastParseDateForKafka("2026-01-01 00:00:00");
        String LOCALDATE_DATE = oracleAdapter.fastParseDateForKafka(LocalDateTime.now());


        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        // 5. JDBC 写入
        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement ps =
                     conn.prepareStatement("INSERT INTO TEST (EPOCH_DATE,STRING_DATE,LOCALDATE_DATE) VALUES (TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'),TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'),TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'))")) {

            ps.setString(1, EPOCH_DATE);
            ps.setString(2, STRING_DATE);
            ps.setString(3, LOCALDATE_DATE);
            ps.executeUpdate();


        }
        System.out.println("插入完成，epoch = ");

//        System.out.println("插入完成，epoch = " + epochMillis);


//
//
//        OracleAdapter oracleAdapter = new OracleAdapter();
//        oracleAdapter.fastParseDateForKafka(t);
//        if (s.length() == 10) t = t * 1000L; // 可选：兼容秒级


//        Date date = new Date(t);
//        String formatted = ldt.format(FMT_DATETIME);
//        System.out.println(oracleAdapter.fastParseDateForKafka(s));


////        2026/1/9 19:51:51
//// 1767988311000
//        LocalDateTime ldt = LocalDateTime.of(2026, 1, 9, 19, 51, 51);
//
//        Instant instant1 = ldt.atZone(ZoneOffset.UTC).toInstant();
//        long epoch1 = instant1.toEpochMilli();
//
//        System.out.println(epoch1);
//
//
//        Instant instant2 = ldt.atZone(ZoneId.of("Asia/Shanghai")).toInstant();
//        long epoch2 = instant2.toEpochMilli();
//
//        System.out.println(epoch2);
    }














}