package com.bsoft.common.enums;

/**
 * ClassName: KafkaTopicCleanMode
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/8/17 - 11:18
 */

// Kafka topic 清理方式枚举
public enum KafkaTopicCleanMode {

    // 删除 topic 后重新创建
    RECREATE_TOPIC("recreate-topic"),

    // 保留 topic，仅清除 topic 数据
    CLEAR_DATA("clear-data");

    // 配置文件中的原始值
    private final String configValue;

    // 构造清理方式枚举
    KafkaTopicCleanMode(String configValue) {
        // 保存配置值
        this.configValue = configValue;
    }

    // 获取配置值
    public String getConfigValue() {
        // 返回配置值
        return configValue;
    }

    // 从配置文本解析清理方式
    public static KafkaTopicCleanMode fromConfig(String value) {
        // 空配置默认保持历史行为
        if (value == null || value.trim().isEmpty()) {
            return RECREATE_TOPIC;
        }

        // 标准化配置文本
        String normalized = value.trim().toLowerCase().replace("_", "-");

        // 遍历枚举匹配配置值
        for (KafkaTopicCleanMode mode : values()) {
            if (mode.configValue.equals(normalized)) {
                return mode;
            }
        }

        // 非法配置直接失败，避免误删或误清理
        throw new IllegalArgumentException("Unsupported clean-topics-before-snapshot-mode: " + value);
    }
}