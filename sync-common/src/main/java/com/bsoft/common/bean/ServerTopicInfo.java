package com.bsoft.common.bean;

import com.bsoft.common.enums.TopicSourceKind;

/**
 * ClassName: TopicInfo
 * Package:   com.bsoft.bean
 * Description: 持有主题信息，
 *              kafka上的每一个主题，都对应唯一一个businessDataSourceId，唯一一个targetDataSourceId
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:25
 */
public class ServerTopicInfo extends ClientTopicInfo {

    // 目标表名
    private String targetTableName;

    // 新增：topic 来源
    private TopicSourceKind sourceKind = TopicSourceKind.TABLE_CDC;

    // 新增：用于 SQL 结果场景的追踪信息
    private String rulePlanId;
    private String sinkNodeId;
    private String sqlNodeId;
    private String sqlQuery;
    private String sqlSchemaName;


    public ServerTopicInfo() {
        super();
    }


    public TopicSourceKind getSourceKind() {
        return sourceKind;
    }

    public void setSourceKind(TopicSourceKind sourceKind) {
        this.sourceKind = sourceKind;
    }

    public String getRulePlanId() {
        return rulePlanId;
    }

    public void setRulePlanId(String rulePlanId) {
        this.rulePlanId = rulePlanId;
    }

    public String getSinkNodeId() {
        return sinkNodeId;
    }

    public void setSinkNodeId(String sinkNodeId) {
        this.sinkNodeId = sinkNodeId;
    }

    public String getSqlNodeId() {
        return sqlNodeId;
    }

    public void setSqlNodeId(String sqlNodeId) {
        this.sqlNodeId = sqlNodeId;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public String getSqlSchemaName() {
        return sqlSchemaName;
    }

    public void setSqlSchemaName(String sqlSchemaName) {
        this.sqlSchemaName = sqlSchemaName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }



}
