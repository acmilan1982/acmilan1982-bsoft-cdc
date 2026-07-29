package com.bsoft.common.enums;

/**
 * ClassName: KafkaWriteMode
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/5/7 - 17:16
 */
// Kafka 写入模式
public enum KafkaWriteMode {

    // 高吞吐模式：按表分流，多 KafkaSink 并发写入不同 topic
    PARALLEL,

    // 顺序模式：单一自定义 sink 按输入顺序写入 Kafka
    ORDERED;

    // 从配置字符串解析写入模式
    public static KafkaWriteMode fromConfig(String value) {
        // 配置为空时默认使用高吞吐模式
        if (value == null || value.trim().isEmpty()) {
            return PARALLEL;
        }

        // 规范化配置值
        String normalized = value.trim().toUpperCase();

        // 遍历枚举值
        for (KafkaWriteMode mode : values()) {
            // 匹配配置值
            if (mode.name().equals(normalized)) {
                return mode;
            }
        }

        // 配置非法时直接失败
        throw new IllegalArgumentException("Unsupported kafka-write-mode: " + value);
    }
}
