package com.bsoft.server.sql.adapter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.sql.Timestamp;

// 数据库适配器基类，统一日期转换，具体参数绑定由子类实现。
public abstract class AbstractDatabaseAdapter implements DatabaseAdapter {

    // 数字时间戳和 Date 对象统一按 UTC 输出。
    private static final ZoneId UTC = ZoneId.of("UTC");

    // 使用严格模式校验日期，避免无效日期被自动调整。
    private static final DateTimeFormatter FMT_DATETIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);

    // 日期字符串使用严格模式解析。
    private static final DateTimeFormatter FMT_DATE =
            DateTimeFormatter.ofPattern("uuuu-MM-dd")
                    .withResolverStyle(ResolverStyle.STRICT);


    // 微秒单位输出固定 6 位小数秒，对应 Oracle 的 FF6。
    private static final DateTimeFormatter FMT_DATETIME_MICROS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS")
                    .withResolverStyle(ResolverStyle.STRICT);

    // 纳秒单位输出固定 9 位小数秒，对应 Oracle 的 FF9。
    private static final DateTimeFormatter FMT_DATETIME_NANOS =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS")
                    .withResolverStyle(ResolverStyle.STRICT);

    // 显式单位模式下支持标准日期时间，以及可选的 1 至 9 位小数秒。
    private static final DateTimeFormatter FMT_DATETIME_WITH_FRACTION =
            new DateTimeFormatterBuilder()
                    .appendPattern("uuuu-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                    .optionalEnd()
                    .toFormatter()
                    .withResolverStyle(ResolverStyle.STRICT);


    // 转换日期值，输出精确到秒的日期字符串；无法识别的输入抛出异常。
    @Override
    public String fastParseDateForKafka(Object value) {
        // 空值保持为空。
        if (value == null) {
            return null;
        }

        // 本地日期时间保留字段时间，不执行时区转换，格式化时舍弃小数秒。
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(FMT_DATETIME);
        }

        // 本地日期补齐当天零点。
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay().format(FMT_DATETIME);
        }

        // Timestamp 属于 Date 子类，通过毫秒时间戳统一处理。
        if (value instanceof Date) {
            return formatInstant(
                    Instant.ofEpochMilli(((Date) value).getTime())
            );
        }

        // 数字与数字字符串采用相同的单位识别规则。
        if (value instanceof Number) {
            return parseNumericTimestamp(value.toString());
        }

        // 字符串支持数字时间戳、标准日期时间和标准日期。
        if (value instanceof String) {
            String text = ((String) value).trim();

            // 空字符串保持为空。
            if (text.isEmpty()) {
                return null;
            }

            // 带可选正负号的整数按时间戳解析。
            if (isSignedInteger(text)) {
                return parseNumericTimestamp(text);
            }

            try {
                // 标准日期时间直接解析和格式化，不执行时区转换。
                if (text.length() == 19) {
                    return LocalDateTime.parse(text, FMT_DATETIME)
                            .format(FMT_DATETIME);
                }

                // 标准日期补齐当天零点。
                if (text.length() == 10) {
                    return LocalDate.parse(text, FMT_DATE)
                            .atStartOfDay()
                            .format(FMT_DATETIME);
                }
            } catch (java.time.format.DateTimeParseException exception) {
                // 无效日期明确报错，避免转换成 null 后静默入库。
                throw new IllegalArgumentException(
                        "日期格式或日期值无效: " + text, exception
                );
            }

            // 未支持的字符串格式明确报错。
            throw new IllegalArgumentException(
                    "不支持的日期格式: " + text
                            + "，仅支持 yyyy-MM-dd HH:mm:ss、yyyy-MM-dd"
                            + " 或 10/13/16 位整数时间戳"
            );
        }

        // 未支持的对象类型明确报错。
        throw new IllegalArgumentException(
                "不支持的日期值类型: " + value.getClass().getName()
        );
    }

    // 临时按数字位数识别时间单位，正负号不计入位数，最终舍弃小数秒。
    private String parseNumericTimestamp(String text) {
        // 拒绝小数、科学计数法及其他非整数形式。
        if (!isSignedInteger(text)) {
            throw new IllegalArgumentException(
                    "时间戳必须是整数: " + text
            );
        }

        // 统计数字位数，忽略开头的正负号。
        int digitLength = getDigitLength(text);

        // 仅接受临时约定的三种长度。
        if (digitLength != 10 && digitLength != 13 && digitLength != 16) {
            throw new IllegalArgumentException(
                    "不支持的时间戳长度: " + digitLength
                            + "，值: " + text
                            + "；仅支持 10 位秒、13 位毫秒、16 位微秒"
            );
        }

        // 当前长度限制内的整数均可以使用 long 保存。
        long timestamp = Long.parseLong(text);
        long epochSeconds;

        // 秒级时间戳无需转换。
        if (digitLength == 10) {
            epochSeconds = timestamp;
        } else if (digitLength == 13) {
            // 毫秒转秒，向下取整以正确处理 1970 年以前的时间。
            epochSeconds = Math.floorDiv(timestamp, 1_000L);
        } else {
            // 微秒转秒，直接舍弃不足一秒的部分。
            epochSeconds = Math.floorDiv(timestamp, 1_000_000L);
        }

        // 统一通过秒级时间戳生成 UTC 日期字符串。
        return formatInstant(Instant.ofEpochSecond(epochSeconds));
    }


    // 按明确单位转换日期，不允许缺失单位，也不根据时间戳位数猜测单位。
    public String fastParseDateForKafka(Object value, String unit) {
        // 校验单位并确定输出格式，缺失或非法单位直接报错。
        // 由各自子类实现，原则上根据 dateFieldUnits 中的精度决定输出格式(oracle)，但mysql,doris直接舍弃 秒之后的精度
        DateTimeFormatter formatter = getDateTimeFormatterForUnit(unit);

        // 有效单位下，空值保持为空。
        if (value == null) {
            return null;
        }

        // 本地日期时间保留字段时间，不执行时区转换。
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(formatter);
        }

        // 本地日期补齐当天零点，并按单位补齐小数秒。
        if (value instanceof LocalDate) {
            return ((LocalDate) value).atStartOfDay().format(formatter);
        }

        // Timestamp 必须先于 Date 处理，通过 toInstant 保留纳秒精度。
        if (value instanceof Timestamp) {
            return ((Timestamp) value).toInstant()
                    .atZone(UTC)
                    .format(formatter);
        }

        // 普通 Date 本身使用毫秒表示，不因字段单位再次缩放。
        if (value instanceof Date) {
            return Instant.ofEpochMilli(((Date) value).getTime())
                    .atZone(UTC)
                    .format(formatter);
        }

        // Number 按明确单位解析，整数要求由转换方法校验。
        if (value instanceof Number) {
            return parseNumericTimestamp(value.toString(), unit)
                    .atZone(UTC)
                    .format(formatter);
        }

        // 字符串支持整数时间戳、标准日期及带可选小数秒的日期时间。
        if (value instanceof String) {
            String text = ((String) value).trim();

            // 保持原有空字符串转换为空值的行为。
            if (text.isEmpty()) {
                return null;
            }

            // 整数时间戳严格按单位解释，不限制数字位数。
            if (isSignedInteger(text)) {
                return parseNumericTimestamp(text, unit)
                        .atZone(UTC)
                        .format(formatter);
            }

            try {
                // 标准日期补齐当天零点。
                if (text.length() == 10) {
                    return LocalDate.parse(text, FMT_DATE)
                            .atStartOfDay()
                            .format(formatter);
                }

                // 标准日期时间保留字段时间，并按所需精度输出。
                return LocalDateTime.parse(text, FMT_DATETIME_WITH_FRACTION)
                        .format(formatter);
            } catch (java.time.format.DateTimeParseException exception) {
                // 日期格式或日期值错误时明确失败，避免静默写入 null。
                throw new IllegalArgumentException(
                        "日期格式或日期值无效: " + text
                                + "，时间单位: " + unit,
                        exception
                );
            }
        }

        // 未支持的对象类型明确报错。
        throw new IllegalArgumentException(
                "不支持的日期值类型: " + value.getClass().getName()
                        + "，时间单位: " + unit
        );
    }

    // 根据明确单位选择输出格式，与 Oracle 的日期转换表达式保持一致。
    protected DateTimeFormatter getDateTimeFormatterForUnit(String unit) {
        // 缺失单位属于调用错误，不允许退回旧的数字位数识别逻辑。
        if (unit == null) {
            throw new IllegalArgumentException(
                    "日期字段缺少时间单位，必须提供 ms、us、ns 之一"
            );
        }

        // 严格按照协议约定选择格式，不接受空字符串或其他单位。
        switch (unit) {
            case "ms":
                // 毫秒单位按既定约定保留到秒，供 TO_DATE 使用。
                return FMT_DATETIME;
            case "us":
                // 微秒单位保留 6 位小数秒，供 TO_TIMESTAMP 的 FF6 使用。
                return FMT_DATETIME_MICROS;
            case "ns":
                // 纳秒单位保留 9 位小数秒，供 TO_TIMESTAMP 的 FF9 使用。
                return FMT_DATETIME_NANOS;
            default:
                // 非法单位明确报错，避免使用错误单位解释时间数值。
                throw new IllegalArgumentException(
                        "不支持的日期字段单位: " + unit
                                + "，仅支持 ms、us、ns"
                );
        }
    }

    // 按明确单位将整数时间戳转换为 Instant，保留微秒和纳秒部分。
    private Instant parseNumericTimestamp(String text, String unit) {
        // 拒绝小数、科学计数法及其他非整数形式。
        if (!isSignedInteger(text)) {
            throw new IllegalArgumentException(
                    "时间戳必须是整数: " + text
            );
        }

        // 校验 long 范围，避免溢出后得到错误日期。
        long timestamp;
        try {
            timestamp = Long.parseLong(text);
        } catch (NumberFormatException exception) {
            // 超出范围时明确报错。
            throw new IllegalArgumentException(
                    "时间戳超出 long 范围: " + text,
                    exception
            );
        }

        // 按单位拆分整秒和纳秒余数，向下取整以正确处理负时间戳。
        switch (unit) {
            case "ms":
                // 毫秒直接转换为 Instant，小数秒是否保留由输出格式决定。
                return Instant.ofEpochMilli(timestamp);
            case "us":
                // 微秒拆成整秒与不足一秒的部分，再将余数转换为纳秒。
                return Instant.ofEpochSecond(
                        Math.floorDiv(timestamp, 1_000_000L),
                        Math.floorMod(timestamp, 1_000_000L) * 1_000L
                );
            case "ns":
                // 纳秒拆成整秒与纳秒余数，避免经过毫秒而丢失精度。
                return Instant.ofEpochSecond(
                        Math.floorDiv(timestamp, 1_000_000_000L),
                        Math.floorMod(timestamp, 1_000_000_000L)
                );
            default:
                // 未知单位明确报错。
                throw new IllegalArgumentException(
                        "不支持的日期字段单位: " + unit
                                + "，仅支持 ms、us、ns"
                );
        }
    }



    // 将时间点格式化为 UTC 日期字符串，输出不包含小数秒。
    private String formatInstant(Instant instant) {
        // 保持原有 UTC 时间语义。
        return instant.atZone(UTC).format(FMT_DATETIME);
    }

    // 判断字符串是否为带可选正负号的十进制整数。
    private boolean isSignedInteger(String text) {
        // 空字符串不属于整数。
        if (text == null || text.isEmpty()) {
            return false;
        }

        // 跳过可选的正负号。
        int startIndex =
                (text.charAt(0) == '-' || text.charAt(0) == '+') ? 1 : 0;

        // 只有符号而没有数字时不属于整数。
        if (startIndex == text.length()) {
            return false;
        }

        // 剩余字符必须全部为数字。
        for (int index = startIndex; index < text.length(); index++) {
            char character = text.charAt(index);
            if (character < '0' || character > '9') {
                return false;
            }
        }

        return true;
    }

    // 获取整数的数字位数，不包含开头的正负号。
    private int getDigitLength(String text) {
        // 调用前已确认字符串为非空整数。
        return (text.charAt(0) == '-' || text.charAt(0) == '+')
                ? text.length() - 1
                : text.length();
    }
}