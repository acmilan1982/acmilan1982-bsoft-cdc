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
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;

/**
 * Oracle数据库插入策略实现
 * 修改策略：只插入新数据，如果主键重复则忽略（不插入不更新）
 */
public class OracleAdapter extends AbstractDatabaseAdapter {
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter FMT_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FMT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public OracleAdapter() {
    }


    /**
     * 通用的参数设置方法 - 处理日期字段转换
     */
    public void setParameterValue(PreparedStatement ps, int index, Object value,
                                     String fieldName, Set<String> dateFieldSet) throws SQLException {

        // dateFieldSet / fieldName 都是小写（你已说明），这里不再额外 normalize
        if (dateFieldSet.contains(fieldName)) {
            // 关键：date 字段的 SQL 是 TO_DATE(?)，因此这里必须 setString
            String dateTimeText = fastParseDateForKafka(value); // 统一输出 "yyyy-MM-dd HH:mm:ss" 或 null
            if (dateTimeText == null) {
                ps.setString(index, null);
            } else {
                ps.setString(index, dateTimeText);
            }
            return;
        }

        // 非日期字段：减少 setObject 推断成本
        if (value == null) {
            ps.setObject(index, null);
            return;
        }

        if (value instanceof String) {
            ps.setString(index, (String) value);
        } else if (value instanceof LocalDateTime) {
            // 如果你的非日期字段里不会出现 LocalDateTime，可以删掉；保留是安全兜底
            ps.setString(index, ((LocalDateTime) value).toString());
        } else {
            ps.setObject(index, value);
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