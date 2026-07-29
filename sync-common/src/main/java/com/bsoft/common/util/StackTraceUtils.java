package com.bsoft.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 异常堆栈处理工具。
 */
public final class StackTraceUtils {

    /**
     * 私有构造方法，禁止实例化工具类。
     */
    private StackTraceUtils() {
        // 工具类不需要实例化。
    }

    /**
     * 获取安全截断后的异常堆栈头部。
     */
    public static String getSafeStackTraceHead(Throwable e, int maxLines, int maxLength) {
        // 异常为空时返回空字符串。
        if (e == null) {
            return "";
        }

        // 创建字符串写入器，用于接收异常堆栈。
        StringWriter sw = new StringWriter();

        // 创建 PrintWriter，用于输出异常堆栈。
        PrintWriter pw = new PrintWriter(sw);

        // 将异常堆栈写入字符串写入器。
        e.printStackTrace(pw);

        // 按换行拆分堆栈。
        String[] lines = sw.toString().split("\\r?\\n");

        // 创建截断后的堆栈内容。
        StringBuilder limited = new StringBuilder();

        // 只保留指定行数以内的堆栈。
        for (int i = 0; i < Math.min(maxLines, lines.length); i++) {
            // 追加当前堆栈行。
            limited.append(lines[i]).append(System.lineSeparator());
        }

        // 超过最大行数时追加截断提示。
        if (lines.length > maxLines) {
            limited.append("... (truncated, total ").append(lines.length).append(" lines)");
        }

        // 转成字符串结果。
        String result = limited.toString();

        // 超过最大长度时截断字符串。
        if (result.length() > maxLength) {
            result = result.substring(0, maxLength - 20) + "... (truncated)";
        }

        // 返回安全堆栈文本。
        return result;
    }
}