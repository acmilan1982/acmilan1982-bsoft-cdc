package com.bsoft.client.scn;

import java.io.Serializable;

/**
 * topic offset 推进事件。
 *
 * 语义：
 * 1. 表示某个 topic 已经成功处理到某个 nextOffset
 * 2. nextOffset 是“下次应开始读取的 offset”
 * 3. 该事件由业务处理成功后产生，再由集中式 sink 统一落库
 */
public class DataSourceScnCommitEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    // 当前 server 标识
    private String clientId;

    // Kafka topic 名称
    private String dataSourceId;

    // 下次应开始读取的 offset
    private long nextScn;

    public DataSourceScnCommitEvent() {
    }

    /**
     * 全参构造。
     */
    public DataSourceScnCommitEvent(String clientId, String dataSourceId, long nextScn) {
        // 直接使用 setter，复用参数校验逻辑
        setClientId(clientId);
        setDataSourceId(dataSourceId);
        setNextScn(nextScn);
    }

    /**
     * 静态工厂方法，便于调用端创建事件。
     */
    public static DataSourceScnCommitEvent of(String clientId, String dataSourceId, long nextScn) {
        return new DataSourceScnCommitEvent(clientId, dataSourceId, nextScn);
    }



    public void setClientId(String clientId) {
        // serverId 不能为空，否则无法唯一定位 offset 记录
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId is blank");
        }
        this.clientId = clientId.trim();
    }



    public void setDataSourceId(String dataSourceId) {
        // topic 不能为空，否则无法定位 Kafka 位点
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("dataSourceId is blank");
        }
        this.dataSourceId = dataSourceId.trim();
    }



    public void setNextScn(long nextScn) {
        // nextScn 不能为负数
        if (nextScn < 0L) {
            throw new IllegalArgumentException("nextScn cannot be negative");
        }
        this.nextScn = nextScn;
    }

    public String getClientId() {
        return clientId;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public long getNextScn() {
        return nextScn;
    }


    @Override
    public String toString() {
        return "DataSourceScnCommitEvent{" +
                "clientId='" + clientId + '\'' +
                ", dataSourceId='" + dataSourceId + '\'' +
                ", nextScn=" + nextScn +
                '}';
    }
}