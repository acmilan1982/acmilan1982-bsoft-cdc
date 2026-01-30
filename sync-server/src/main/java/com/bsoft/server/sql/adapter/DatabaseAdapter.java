package com.bsoft.server.sql.adapter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;

/**
 *  * 数据库适配器接口
 *  * 封装不同数据库的参数处理、类型转换等差异化逻辑
 * ClassName: DatabaseAdapter
 * Package:   com.bsoft.server.sql.adapter
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/30 - 17:38
 */
public interface DatabaseAdapter {

    /**
     * 设置 PreparedStatement 参数
     *
     * @param ps PreparedStatement 对象
     * @param index 参数索引（从1开始）
     * @param value 参数值
     * @param fieldName 字段名
     * @param dateFieldSet 日期字段集合
     * @throws SQLException SQL异常
     */
    void setParameterValue(PreparedStatement ps, int index, Object value,
                           String fieldName, Set<String> dateFieldSet) throws SQLException;

     String fastParseDateForKafka(Object value);

}
