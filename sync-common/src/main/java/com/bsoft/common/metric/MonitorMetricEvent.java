package com.bsoft.common.metric;


import com.bsoft.common.enums.MetricType;

import java.io.Serializable;
import java.util.Objects;

/**
 * sync-server -> sync-monitor 监控事件
 * monitorOffset 为近似值（非精确落库位点）
 */
public class MonitorMetricEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 协议版本，便于后续兼容 */
    private String version;

    /** 事件时间戳（毫秒） */
    private long eventTime;

    private String serverId;
    private String topic;
    private String targetDataSourceId;
    private String targetTableName;

    /** 当前先用 OFFSET，后续可扩展 */
    private MetricType metricType;

    /** 指标值（offset/lag 等） */
    private long value;

    public MonitorMetricEvent() {
    }

    public MonitorMetricEvent(String version,
                              long eventTime,
                              String serverId,
                              String topic,
                              String targetDataSourceId,
                              String targetTableName,
                              MetricType metricType,
                              long value) {
        this.version = version;
        this.eventTime = eventTime;
        this.serverId = serverId;
        this.topic = topic;
        this.targetDataSourceId = targetDataSourceId;
        this.targetTableName = targetTableName;
        this.metricType = metricType;
        this.value = value;
    }

    public static MonitorMetricEvent offsetEvent(String serverId,
                                                 String topic,
                                                 String targetDataSourceId,
                                                 String targetTableName,
                                                 long offset) {
        return new MonitorMetricEvent(
                "v1",
                System.currentTimeMillis(),
                serverId,
                topic,
                targetDataSourceId,
                targetTableName,
                MetricType.OFFSET,
                offset
        );
    }

    public void validate() {
        if (isBlank(version)) throw new IllegalArgumentException("version is blank");
        if (eventTime <= 0) throw new IllegalArgumentException("eventTime must be > 0");
        if (isBlank(serverId)) throw new IllegalArgumentException("serverId is blank");
        if (isBlank(topic)) throw new IllegalArgumentException("topic is blank");
        if (isBlank(targetDataSourceId)) throw new IllegalArgumentException("targetDataSourceId is blank");
        if (isBlank(targetTableName)) throw new IllegalArgumentException("targetTableName is blank");
        if (metricType == null) throw new IllegalArgumentException("metricType is null");
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public MetricType getMetricType() {
        return metricType;
    }

    public void setMetricType(MetricType metricType) {
        this.metricType = metricType;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MonitorMetricEvent)) return false;
        MonitorMetricEvent that = (MonitorMetricEvent) o;
        return eventTime == that.eventTime
                && value == that.value
                && Objects.equals(version, that.version)
                && Objects.equals(serverId, that.serverId)
                && Objects.equals(topic, that.topic)
                && Objects.equals(targetDataSourceId, that.targetDataSourceId)
                && Objects.equals(targetTableName, that.targetTableName)
                && metricType == that.metricType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, eventTime, serverId, topic, targetDataSourceId, targetTableName, metricType, value);
    }

    @Override
    public String toString() {
        return "MonitorMetricEvent{" +
                "version='" + version + '\'' +
                ", eventTime=" + eventTime +
                ", serverId='" + serverId + '\'' +
                ", topic='" + topic + '\'' +
                ", targetDataSourceId='" + targetDataSourceId + '\'' +
                ", targetTableName='" + targetTableName + '\'' +
                ", metricType=" + metricType +
                ", value=" + value +
                '}';
    }
}