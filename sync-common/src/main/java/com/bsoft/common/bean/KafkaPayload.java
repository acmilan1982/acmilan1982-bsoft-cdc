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

    // 表示是否是最后一条快照数据
    private boolean lastSnapshot;

    // 表示是否为 当前表的最后一条快照数据
    private boolean lastRecordInTable;

    // Kafka 原始 topic 名，用于多 topic 合并后按真实 topic 推进 offset
    private String kafkaTopic;

    // Kafka 分区号，当前业务只有 0 分区，保留用于排查
    private Integer kafkaPartition;

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
                        boolean snapshot,
                        boolean lastSnapshot) {
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
        this.lastSnapshot = lastSnapshot;
    }


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
                        boolean snapshot,
                        boolean lastSnapshot,
                        boolean lastRecordInTable) {
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
        this.lastSnapshot = lastSnapshot;
        this.lastRecordInTable = lastRecordInTable;

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

    // dml 相关构造方法，包含 Kafka topic 元数据
    public KafkaPayload(String kafkaTopic,
                        Integer kafkaPartition,
                        String schemaName,
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
                        boolean snapshot,
                        boolean lastSnapshot,
                        boolean lastRecordInTable) {
        // 保存 Kafka 原始 topic
        this.kafkaTopic = kafkaTopic;

        // 保存 Kafka 分区号
        this.kafkaPartition = kafkaPartition;

        // 保存操作类型
        this.op = op;

        // 保存源 schema
        this.schemaName = schemaName;

        // 保存源表名
        this.tableName = tableName;

        // 保存 before 数据
        this.beforeData = beforeData;

        // 保存 after 数据
        this.afterData = afterData;

        // 保存 DML 时间
        this.dmlTime = dmlTime;

        // 保存 Kafka 入队时间
        this.kafkaEnqueueTime = kafkaEnqueueTime;

        // 保存 Kafka offset
        this.offset = offset;

        // 保存业务数据源 ID
        this.businessDataSourceId = businessDataSourceId;

        // 保存日期字段集合
        this.dateFieldSet = dateFieldSet != null ? dateFieldSet : new HashSet<>();

        // 保存主键字段集合
        this.primaryKeySet = primaryKeySet != null ? primaryKeySet : new HashSet<>();

        // 转换 before 数据中的日期字段
        convertToLocalDateTime(beforeData);

        // 转换 after 数据中的日期字段
        convertToLocalDateTime(afterData);

        // 保存原始消息
        this.rawMessage = rawMessage;

        // 保存是否快照数据
        this.snapshot = snapshot;

        // 保存是否最后一条快照数据
        this.lastSnapshot = lastSnapshot;

        // 保存是否当前表最后一条快照数据
        this.lastRecordInTable = lastRecordInTable;
    }

    //ddl时生成
    public KafkaPayload(String op,
                        String ddlStatement) {
        // 操作类型
        this.op = op;
        this.ddlStatement = ddlStatement;
    }

    // long 类型的时间转换成 LocalDateTime
    private void convertToLocalDateTime(Map<String, Object> dataMap) {
        // 空数据直接返回
        if (dataMap == null) {
            return;
        }

        // 遍历字段
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            // 字段名
            String key = entry.getKey().toUpperCase();

            // 字段值
            Object value = entry.getValue();

            // 日期字段尝试转换
            if (value instanceof Long && dateFieldSet.contains(key)) {
                try {
                    // 转换日期字段
                    entry.setValue(DateFormatUtils.convertToLocalDateTime(value));
                } catch (Exception e) {
                    // 转换失败时保留原值
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

    public boolean isLastSnapshot() {
        return lastSnapshot;
    }
    public boolean lastRecordInTable() {
        return lastRecordInTable;
    }

    public void setLastSnapshot(boolean lastSnapshot) {
        this.lastSnapshot = lastSnapshot;
    }


    public boolean isLastRecordInTable() {
        return lastRecordInTable;
    }

    public void setLastRecordInTable(boolean lastRecordInTable) {
        this.lastRecordInTable = lastRecordInTable;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public Integer getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(Integer kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    // 获取 topic + offset 组成的批量写入状态 key
    public String getTopicOffsetKey() {
        // topic 为空时退化为空字符串
        String topicPart = kafkaTopic == null ? "" : kafkaTopic.trim();

        // offset 为空时退化为 -1
        long offsetPart = offset == null ? -1L : offset;

        // 返回稳定 key
        return topicPart + "#" + offsetPart;
    }
}
