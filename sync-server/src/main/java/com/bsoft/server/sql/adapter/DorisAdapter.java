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

/**
 * Oracle数据库插入策略实现
 * 修改策略：只插入新数据，如果主键重复则忽略（不插入不更新）
 */
public class DorisAdapter extends AbstractDatabaseAdapter  {

    // doris4版本， 全线支持 DateTimeV2 类型
    protected boolean shouldEnableDateTimeV2;

    /**
     * 通用的参数设置方法 - 处理日期字段转换
     */
    public void setParameterValue(PreparedStatement ps, int index, Object value,
                                     String fieldName, Set<String> dateFieldSet) throws SQLException {
        try {
            if (isDateField(dateFieldSet, fieldName) && value != null) {
                // 日期字段转换为字符串格式，
                String formattedValue = fastParseDateForKafka(value);
                // 处理时间字段格式转换
                if(shouldEnableDateTimeV2){
                    ps.setString(index, formattedValue); // dorsi4中DATETIMEV2的处理
                }else{
                    ps.setString(index, formattedValue); // doris3中DATETIME的处理
                }

            } else {
                ps.setObject(index, value);
            }
        } catch (Exception e) {
            // 异常处理 - 尝试转换为日期
            e.printStackTrace();
        }
    }

    private boolean isDateField(Set<String> dateFieldSet, String fieldName) {
        return dateFieldSet != null
                && fieldName != null
                && dateFieldSet.contains(fieldName.toLowerCase(Locale.ROOT));
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
