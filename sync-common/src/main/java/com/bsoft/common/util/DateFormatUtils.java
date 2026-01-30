package com.bsoft.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * ClassName: DateFormatUtils
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/21 - 16:31
 */
public class DateFormatUtils {


    // 推荐线程安全的 DateTimeFormatter
    private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static LocalDateTime convertToLocalDateTime(Object value) {
        if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value)
                    .atZone(ZoneId.of("Asia/Shanghai"))
                    .toLocalDateTime();
        } else if (value instanceof String) {
            String str = (String) value;
            try {
                return LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (DateTimeParseException e) {
                // 日志或忽略
            }
        }
        return null;
    }

    /**
     * 获取当前时间的字符串格式
     * @return 格式为 "yyyy-MM-dd HH:mm:ss" 的当前时间字符串
     */
    public static String getCurrentTimeString() {
        return LocalDateTime.now(ZoneId.of("Asia/Shanghai")).format(DEFAULT_FORMATTER);
    }


}
