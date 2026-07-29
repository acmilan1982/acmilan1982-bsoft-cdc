package com.bsoft.server.sql.adapter;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */



import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;


/**
 * Oracle数据库插入策略实现
 * 修改策略：只插入新数据，如果主键重复则忽略（不插入不更新）
 */
public abstract class AbstractDatabaseAdapter implements DatabaseAdapter {
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DateTimeFormatter FMT_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FMT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public AbstractDatabaseAdapter() {
    }




    @Override
    public String fastParseDateForKafka(Object value) {
        if (value == null) return null;

        // 1) LocalDateTime：认为是“纯字段值”，直接格式化（不做时区转换）
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(FMT_DATETIME);
        }

        // 2) LocalDate：补齐到当天 00:00:00
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay().format(FMT_DATETIME);
        }

        // 3) java.sql.Timestamp / java.util.Date：视为 epoch instant
        if (value instanceof java.sql.Timestamp) {
            long epochMillis = ((java.sql.Timestamp) value).getTime();
            return Instant.ofEpochMilli(epochMillis).atZone(UTC).format(FMT_DATETIME);
        }
        if (value instanceof Date) {
            long epochMillis = ((Date) value).getTime();
            return Instant.ofEpochMilli(epochMillis).atZone(UTC).format(FMT_DATETIME);
        }

        // 4) Number：支持 Long/Integer（epoch 毫秒/秒）
        if (value instanceof Number) {
            long t = ((Number) value).longValue();
            // 兼容 10 位秒级
            if (t > 0 && t < 100000000000L) { // < 1e11 认为是秒
                t *= 1000L;
            }
            return Instant.ofEpochMilli(t).atZone(UTC).format(FMT_DATETIME);
        }

        // 5) String：epoch 或 yyyy-MM-dd HH:mm:ss 或 yyyy-MM-dd
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.isEmpty()) return null;

            // 5.1 纯数字：epoch
            if (isSignedInteger(s)) {
                try {
                    long t = Long.parseLong(s);
                    int digitLength = getDigitLength(s);

                    // 10 位数字按秒级时间戳处理，其他长度按毫秒级时间戳处理
                    if (digitLength == 10) {
                        t *= 1000L;
                    }
                    return Instant.ofEpochMilli(t).atZone(UTC).format(FMT_DATETIME);
                } catch (NumberFormatException ignore) {
                    // fall through
                }
            }

            // 5.2 yyyy-MM-dd HH:mm:ss：纯字段值（原样入库）
            if (s.length() == 19 && s.charAt(4) == '-' && s.charAt(7) == '-' && s.charAt(10) == ' ') {
                // 做一次 parse 校验，避免脏数据直接入库；如果你更追求性能可去掉
                try {
                    LocalDateTime.parse(s, FMT_DATETIME);
                    return s;
                } catch (Exception e) {
                    return null;
                }
            }

            // 5.3 yyyy-MM-dd：补齐到 00:00:00
            if (s.length() == 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
                try {
                    LocalDate ld = LocalDate.parse(s, FMT_DATE);
                    return ld.atStartOfDay().format(FMT_DATETIME);
                } catch (Exception e) {
                    return null;
                }
            }

            // 5.4 其他格式不猜（避免引入隐性时区语义）
            return null;
        }

        return null;
    }



    // 判断字符串是否为可选正负号开头的整数
    private boolean isSignedInteger(String s) {
        if (s == null || s.isEmpty()) return false;

        // 跳过开头的正负号
        int start = (s.charAt(0) == '-' || s.charAt(0) == '+') ? 1 : 0;
        if (start == s.length()) return false;

        // 正负号之后必须全部是数字
        for (int i = start; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') return false;
        }

        return true;
    }

    // 获取去掉正负号后的数字位数
    private int getDigitLength(String s) {
        return (s.charAt(0) == '-' || s.charAt(0) == '+') ? s.length() - 1 : s.length();
    }
}