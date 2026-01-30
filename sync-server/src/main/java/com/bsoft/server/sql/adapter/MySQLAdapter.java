package com.bsoft.server.sql.adapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Set;

/**
 * ClassName: MySQLStrategy
 * Package:   com.bsoft.server.sql.common
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/9/18 - 16:56
 */
public  class MySQLAdapter extends AbstractDatabaseAdapter  {


    public MySQLAdapter() {
    }

    /**
     * 通用的参数设置方法 - 处理时间戳转换
     */
    public void setParameterValue(PreparedStatement ps, int index, Object value,
                                     String fieldName, Set<String> dateFieldSet) throws SQLException {
        try {
            if (dateFieldSet.contains(fieldName.toUpperCase()) && value != null) {
                // 处理时间戳转换
                Date dateValue = convertTimestampToDate(value);
                ps.setTimestamp(index, new java.sql.Timestamp(dateValue.getTime()));
            } else {
                ps.setObject(index, value);
            }
        } catch (Exception e) {
            // 异常情况下尝试时间戳转换
            Date dateValue = convertTimestampToDate(value);
            ps.setTimestamp(index, new java.sql.Timestamp(dateValue.getTime()));
        }
    }

    /**
     * 将各种格式转换为 Date 对象
     */
    protected Date convertTimestampToDate(Object value) {
        if (value == null) {
            return null;
        }

        String strValue = value.toString().trim();

        // 处理时间戳格式：1695115878000
        if (strValue.matches("^-?\\d+$")) {
            long timestamp = Long.parseLong(strValue);
            return new Date(timestamp);
        }

        // 其他日期格式处理...
        return (Date) value;
    }
}
