package com.bsoft.common.bean;

/**
 * 操作执行结果 - 用于在MySQLRichSink处理后传递统计信息
 */
public class OperationResult {
    private String serverId;
    private String dataFromSourceId;
    private String dateToSourceId;
    private String tableName;
    private String operation; // "c", "u", "d"
    private boolean success;
    private long executionTimeMs;
    private String errorMessage;
    private long processTimestamp;
    private KafkaPayload originalPayload; // 保留原始数据，方便后续处理

    public OperationResult() {
        this.processTimestamp = System.currentTimeMillis();
    }

    public OperationResult(String tableName, String operation, boolean success, long executionTimeMs) {
        this();
        this.tableName = tableName;
        this.operation = operation;
        this.success = success;
        this.executionTimeMs = executionTimeMs;
    }

    // Getters and Setters
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getOperation() { return operation; }
    public void setOperation(String operation) { this.operation = operation; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public long getExecutionTimeMs() { return executionTimeMs; }
    public void setExecutionTimeMs(long executionTimeMs) { this.executionTimeMs = executionTimeMs; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public long getProcessTimestamp() { return processTimestamp; }
    public void setProcessTimestamp(long processTimestamp) { this.processTimestamp = processTimestamp; }

    public KafkaPayload getOriginalPayload() { return originalPayload; }
    public void setOriginalPayload(KafkaPayload originalPayload) { this.originalPayload = originalPayload; }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getDataFromSourceId() {
        return dataFromSourceId;
    }

    public void setDataFromSourceId(String dataFromSourceId) {
        this.dataFromSourceId = dataFromSourceId;
    }

    public String getDateToSourceId() {
        return dateToSourceId;
    }

    public void setDateToSourceId(String dateToSourceId) {
        this.dateToSourceId = dateToSourceId;
    }

    @Override
    public String toString() {
        return String.format("OperationResult{table='%s', op='%s', success=%s, time=%dms}",
                tableName, operation, success, executionTimeMs);
    }
}