package com.bsoft.common.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ClassName: MyUitl
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/5/8 - 23:18
 */
public class MyUitl {
    public static void main(String[] args) {
        String dateTimeString = "2018-02-01 10:20:00";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
        long milliseconds = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

        System.out.println("原始字符串: " + dateTimeString);
        System.out.println("转换后的 long 类型: " + milliseconds);
    }
}
