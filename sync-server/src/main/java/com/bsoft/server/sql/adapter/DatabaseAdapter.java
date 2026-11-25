package com.bsoft.server.sql.adapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

// 定义数据库参数绑定和日期转换接口，由具体适配器决定日期输出精度。
public interface DatabaseAdapter {

    // 保留现有参数绑定入口，供尚未接入单位映射的调用使用。
    void setParameterValue(PreparedStatement ps,
                           int index,
                           Object value,
                           String fieldName,
                           Set<String> dateFieldSet) throws SQLException;

    // 根据消息中的日期字段单位绑定参数，具体数据库应按需重写。
    default void setParameterValue(PreparedStatement ps,
                                   int index,
                                   Object value,
                                   String fieldName,
                                   Set<String> dateFieldSet,
                                   Map<String, String> dateFieldUnits) throws SQLException {
        // 默认委托原入口，保留其他现有实现的调用关系。
        setParameterValue(ps, index, value, fieldName, dateFieldSet);
    }

    // 保留现有无单位转换入口，供尚未迁移的调用使用。
    String fastParseDateForKafka(Object value);

    // 按明确单位解释日期值，输出精度由具体数据库适配器决定。
    String fastParseDateForKafka(Object value, String unit);
}