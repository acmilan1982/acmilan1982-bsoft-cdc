package com.bsoft.common.bean;

import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;

/**
 * ClassName: TopicInfo
 * Package:   com.bsoft.bean
 * Description: 持有主题信息，
 *              kafka上的每一个主题，都对应唯一一个businessDataSourceId，唯一一个targetDataSourceId
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:25
 */
public class ClientTopicInfo implements Serializable {

    private String businessDataSourceId;
    private String targetDataSourceId;




    // 源库schema
    private String schemaName;
    // 源表名
    private String tableName;
    // 主题名
    private String topic;

    private SimpleClientInfo clientInfo;
    public ClientTopicInfo() {
    }

    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFullyQualifiedTableName() {
        return schemaName + "." + tableName;
    }

    public SimpleClientInfo getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(SimpleClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }

    // 兼容旧调用方
    public String getClientId() {
        return clientInfo == null ? null : clientInfo.getClientId();
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return clientInfo == null ? null : clientInfo.getTableNamingStrategy();
    }

    public String getTableNamePrefix() {
        return clientInfo == null ? null : clientInfo.getTableNamePrefix();
    }

    public String getTableNameSuffix() {
        return clientInfo == null ? null : clientInfo.getTableNameSuffix();
    }
}
