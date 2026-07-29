package com.bsoft.server.offset;

import java.io.Serializable;

/**
 * topic offset 推进事件。
 *
 * 语义：
 * 1. 表示某个 topic 已经成功处理到某个 nextOffset
 * 2. nextOffset 是“下次应开始读取的 offset”
 * 3. 该事件由业务处理成功后产生，再由集中式 sink 统一落库
 */
public class TopicOffsetCommitEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    // 当前 server 标识
    private String serverId;

    // Kafka topic 名称
    private String kafkaTopic;

    // 下次应开始读取的 offset
    private long nextOffset;

    public TopicOffsetCommitEvent() {
    }

    /**
     * 全参构造。
     */
    public TopicOffsetCommitEvent(String serverId, String kafkaTopic, long nextOffset) {
        // 直接使用 setter，复用参数校验逻辑
        setServerId(serverId);
        setKafkaTopic(kafkaTopic);
        setNextOffset(nextOffset);
    }

    /**
     * 静态工厂方法，便于调用端创建事件。
     */
    public static TopicOffsetCommitEvent of(String serverId, String kafkaTopic, long nextOffset) {
        return new TopicOffsetCommitEvent(serverId, kafkaTopic, nextOffset);
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        // serverId 不能为空，否则无法唯一定位 offset 记录
        if (serverId == null || serverId.trim().isEmpty()) {
            throw new IllegalArgumentException("serverId is blank");
        }
        this.serverId = serverId.trim();
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        // topic 不能为空，否则无法定位 Kafka 位点
        if (kafkaTopic == null || kafkaTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("kafkaTopic is blank");
        }
        this.kafkaTopic = kafkaTopic.trim();
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        // nextOffset 不能为负数
        if (nextOffset < 0L) {
            throw new IllegalArgumentException("nextOffset cannot be negative");
        }
        this.nextOffset = nextOffset;
    }

    @Override
    public String toString() {
        return "TopicOffsetCommitEvent{" +
                "serverId='" + serverId + '\'' +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", nextOffset=" + nextOffset +
                '}';
    }
}