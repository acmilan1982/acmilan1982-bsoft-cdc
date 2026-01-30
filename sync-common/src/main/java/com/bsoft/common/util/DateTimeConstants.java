package com.bsoft.common.util;

/**
 * ClassName: DateTimeConstants
 * Package:   com.bsoft.common.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/1/16 - 9:44
 */

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 日期时间格式化常量类
 */
public final class DateTimeConstants {

    private DateTimeConstants() {
        // 私有构造函数，防止实例化
    }

    /**
     * UTC 时区
     */
    public static final ZoneId UTC = ZoneId.of("UTC");

    /**
     * 日期时间格式: yyyy-MM-dd HH:mm:ss
     */
    public static final DateTimeFormatter FMT_DATETIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 日期格式: yyyy-MM-dd
     */
    public static final DateTimeFormatter FMT_DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Oracle DATE 格式
     */
    public static final String ORACLE_DATE_FORMAT = "yyyy-mm-dd hh24:mi:ss";
}
