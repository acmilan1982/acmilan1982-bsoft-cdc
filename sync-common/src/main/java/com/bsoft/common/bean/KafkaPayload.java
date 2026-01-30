package com.bsoft.common.bean;

import com.bsoft.common.util.DateFormatUtils;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaPayload {
    private String schemaName;
    private String tableName;
    private String op;
    private Map<String, Object> beforeData;
    private Map<String, Object> afterData;
    private Set<String> dateFieldSet;
    private Set<String> primaryKeySet;
    // 业务库的事务时间
    private String dmlTime;
    // Kafka 消息入队时间
    private String kafkaEnqueueTime;
    // 消息队列的偏移量
    private Long offset = 0L;
    private String businessDataSourceId;

    private String rawMessage;
    // ddl语句
    private String ddlStatement;

    // 表示是否是快照数据
    private boolean snapshot;


    //dml相关
    public KafkaPayload(String schemaName,
                        String tableName,
                        String op,
                        String dmlTime,
                        String kafkaEnqueueTime,
                        Long offset,
                        String businessDataSourceId,
                        Map<String, Object> beforeData,
                        Map<String, Object> afterData,
                        Set<String> dateFieldSet,
                        Set<String> primaryKeySet,
                        String rawMessage,
                        boolean snapshot) {
        // 操作类型
        this.op = op;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.beforeData = beforeData;
        this.afterData = afterData;
        this.dmlTime = dmlTime;
        this.kafkaEnqueueTime = kafkaEnqueueTime;
        this.offset = offset;
        this.businessDataSourceId = businessDataSourceId;

        this.dateFieldSet = dateFieldSet != null ? dateFieldSet : new HashSet<>();
        this.primaryKeySet = primaryKeySet != null ? primaryKeySet : new HashSet<>();

        // long类型的时间 转换成 LocalDateTime
        convertToLocalDateTime(beforeData);
        convertToLocalDateTime(afterData);

        this.rawMessage = rawMessage;
        this.snapshot = snapshot;
    }

//    //ddl时生成
//    public KafkaPayload(String tableName,
//                        String op,
//                        LocalDateTime dmlTime,
//                        LocalDateTime kafkaEnqueueTime,
//                        Long offset,
//                        String businessDataSourceId,
//                        String ddlStatement,
//                        String rawMessage) {
//        // 操作类型
//        this.op = op;
//        this.tableName = tableName;
//
//        this.dmlTime = dmlTime;
//        this.kafkaEnqueueTime = kafkaEnqueueTime;
//        this.offset = offset;
//        this.businessDataSourceId = businessDataSourceId;
//
//        this.ddlStatement = ddlStatement;
//
//        this.rawMessage = rawMessage;
//    }


    //ddl时生成
    public KafkaPayload(String op,
                        String ddlStatement) {
        // 操作类型
        this.op = op;
        this.ddlStatement = ddlStatement;
    }

    // long 类型的时间 转换成  LocalDateTime
    private void convertToLocalDateTime(Map<String, Object> dataMap) {
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            // 字段名
            String key = entry.getKey().toUpperCase();
            // 字段值
            Object value = entry.getValue();
            if (value instanceof Long && dateFieldSet.contains(key)) {
                try {
                    entry.setValue(DateFormatUtils.convertToLocalDateTime(value));
                } catch (Exception e) {
                    // fallback 不处理
                }
            }
        }
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Map<String, Object> getBeforeData() {
        return beforeData;
    }

    public void setBeforeData(Map<String, Object> beforeData) {
        this.beforeData = beforeData;
    }

    public Map<String, Object> getAfterData() {
        return afterData;
    }

    public void setAfterData(Map<String, Object> afterData) {
        this.afterData = afterData;
    }

    public Set<String> getDateFieldSet() {
        return dateFieldSet;
    }

    public void setDateFieldSet(Set<String> dateFieldSet) {
        this.dateFieldSet = dateFieldSet;
    }

    public Set<String> getPrimaryKeySet() {
        return primaryKeySet;
    }

    public void setPrimaryKeySet(Set<String> primaryKeySet) {
        this.primaryKeySet = primaryKeySet;
    }

    public String getDmlTime() {
        return dmlTime;
    }

    public void setDmlTime(String dmlTime) {
        this.dmlTime = dmlTime;
    }

    public String getKafkaEnqueueTime() {
        return kafkaEnqueueTime;
    }

    public void setKafkaEnqueueTime(String kafkaEnqueueTime) {
        this.kafkaEnqueueTime = kafkaEnqueueTime;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }


    public String getDdlStatement() {
        return ddlStatement;
    }

    public void setDdlStatement(String ddlStatement) {
        this.ddlStatement = ddlStatement;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    public void setSnapshot(boolean snapshot) {
        this.snapshot = snapshot;
    }
}
