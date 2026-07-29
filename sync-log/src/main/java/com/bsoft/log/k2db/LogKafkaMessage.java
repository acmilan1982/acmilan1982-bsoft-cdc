package com.bsoft.log.k2db;

import com.bsoft.common.bean.SyncLogEntry;

import java.io.Serializable;

/**
 * sync-log 从日志 Kafka topic 读取到的一条消息。
 */
public class LogKafkaMessage implements Serializable {

    /**
     * 序列化版本号。
     */
    private static final long serialVersionUID = 1L;

    /**
     * 日志 Kafka topic 名称。
     */
    private String kafkaTopic;

    /**
     * 日志 Kafka partition。
     */
    private int kafkaPartition;

    /**
     * 日志 Kafka offset。
     */
    private long kafkaOffset;

    /**
     * Kafka record timestamp。
     */
    private long kafkaTimestamp;

    /**
     * Kafka key 字符串。
     */
    private String kafkaKey;

    /**
     * Kafka 原始 value 字符串。
     */
    private String rawKafkaMessage;

    /**
     * 解析后的业务日志对象。
     */
    private SyncLogEntry syncLogEntry;

    /**
     * 默认构造方法，供 Flink 序列化使用。
     */
    public LogKafkaMessage() {
    }

    /**
     * 创建日志 Kafka 消息包装对象。
     */
    public LogKafkaMessage(String kafkaTopic,
                           int kafkaPartition,
                           long kafkaOffset,
                           long kafkaTimestamp,
                           String kafkaKey,
                           String rawKafkaMessage,
                           SyncLogEntry syncLogEntry) {
        // 设置日志 topic。
        this.kafkaTopic = kafkaTopic;

        // 设置日志 partition。
        this.kafkaPartition = kafkaPartition;

        // 设置日志 offset。
        this.kafkaOffset = kafkaOffset;

        // 设置 Kafka timestamp。
        this.kafkaTimestamp = kafkaTimestamp;

        // 设置 Kafka key。
        this.kafkaKey = kafkaKey;

        // 设置 Kafka 原始 value。
        this.rawKafkaMessage = rawKafkaMessage;

        // 设置解析后的日志对象。
        this.syncLogEntry = syncLogEntry;
    }

    /**
     * 获取日志 Kafka topic。
     */
    public String getKafkaTopic() {
        // 返回日志 Kafka topic。
        return kafkaTopic;
    }

    /**
     * 设置日志 Kafka topic。
     */
    public void setKafkaTopic(String kafkaTopic) {
        // 保存日志 Kafka topic。
        this.kafkaTopic = kafkaTopic;
    }

    /**
     * 获取日志 Kafka partition。
     */
    public int getKafkaPartition() {
        // 返回日志 Kafka partition。
        return kafkaPartition;
    }

    /**
     * 设置日志 Kafka partition。
     */
    public void setKafkaPartition(int kafkaPartition) {
        // 保存日志 Kafka partition。
        this.kafkaPartition = kafkaPartition;
    }

    /**
     * 获取日志 Kafka offset。
     */
    public long getKafkaOffset() {
        // 返回日志 Kafka offset。
        return kafkaOffset;
    }

    /**
     * 设置日志 Kafka offset。
     */
    public void setKafkaOffset(long kafkaOffset) {
        // 保存日志 Kafka offset。
        this.kafkaOffset = kafkaOffset;
    }

    /**
     * 获取 Kafka record timestamp。
     */
    public long getKafkaTimestamp() {
        // 返回 Kafka timestamp。
        return kafkaTimestamp;
    }

    /**
     * 设置 Kafka record timestamp。
     */
    public void setKafkaTimestamp(long kafkaTimestamp) {
        // 保存 Kafka timestamp。
        this.kafkaTimestamp = kafkaTimestamp;
    }

    /**
     * 获取 Kafka key。
     */
    public String getKafkaKey() {
        // 返回 Kafka key。
        return kafkaKey;
    }

    /**
     * 设置 Kafka key。
     */
    public void setKafkaKey(String kafkaKey) {
        // 保存 Kafka key。
        this.kafkaKey = kafkaKey;
    }

    /**
     * 获取 Kafka 原始 value。
     */
    public String getRawKafkaMessage() {
        // 返回 Kafka 原始 value。
        return rawKafkaMessage;
    }

    /**
     * 设置 Kafka 原始 value。
     */
    public void setRawKafkaMessage(String rawKafkaMessage) {
        // 保存 Kafka 原始 value。
        this.rawKafkaMessage = rawKafkaMessage;
    }

    /**
     * 获取解析后的日志对象。
     */
    public SyncLogEntry getSyncLogEntry() {
        // 返回 SyncLogEntry。
        return syncLogEntry;
    }

    /**
     * 设置解析后的日志对象。
     */
    public void setSyncLogEntry(SyncLogEntry syncLogEntry) {
        // 保存 SyncLogEntry。
        this.syncLogEntry = syncLogEntry;
    }
}