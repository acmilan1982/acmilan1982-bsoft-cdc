package com.bsoft.common.util;

// 异常详情工具类，用于跨模块生成简短、稳定的异常摘要
public final class ExceptionDetailUtil {

    // 默认异常详情最大长度
    public static final int DEFAULT_MAX_LENGTH = 100;

    // 私有构造方法，禁止实例化工具类
    private ExceptionDetailUtil() {
    }

    // 构造默认长度以内的 root cause 异常详情
    public static String buildRootCauseDetail(Throwable throwable) {
        // 使用默认长度生成异常详情
        return buildRootCauseDetail(throwable, DEFAULT_MAX_LENGTH);
    }

    // 构造指定长度以内的 root cause 异常详情
    public static String buildRootCauseDetail(Throwable throwable, int maxLength) {
        // 获取最底层异常
        Throwable rootCause = rootCause(throwable);

        // 获取异常类名
        String className = rootCause.getClass().getName();

        // 获取异常消息
        String message = rootCause.getMessage();

        // 拼接异常详情
        String detail = message == null || message.trim().isEmpty()
                ? className
                : className + ": " + message;

        // 截断异常详情
        return truncate(detail, maxLength);
    }

    // 查找异常链中的 root cause
    public static Throwable rootCause(Throwable throwable) {
        // 空异常兜底
        if (throwable == null) {
            return new RuntimeException("unknown failure");
        }

        // 从当前异常开始向下查找 cause
        Throwable current = throwable;

        // 沿 cause 链查找最底层异常
        while (current.getCause() != null) {
            current = current.getCause();
        }

        // 返回最底层异常
        return current;
    }

    // 截断字符串到指定长度
    public static String truncate(String value, int maxLength) {
        // 空字符串兜底
        if (value == null) {
            return "";
        }

        // 非法长度兜底为默认长度
        int safeMaxLength = maxLength <= 0 ? DEFAULT_MAX_LENGTH : maxLength;

        // 未超过长度时直接返回
        if (value.length() <= safeMaxLength) {
            return value;
        }

        // 超过长度时截断
        return value.substring(0, safeMaxLength);
    }
}