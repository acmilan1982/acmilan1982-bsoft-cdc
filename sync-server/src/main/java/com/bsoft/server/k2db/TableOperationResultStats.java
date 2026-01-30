package com.bsoft.server.k2db;

/**
 * ClassName: TableOperationResultStats
 * Package:   com.bsoft.server.k2m
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:37
package com.bsoft.server.k2m;

/**
 * 表操作结果统计数据结构 - 包含成功/失败次数和执行时间统计
 */
public class TableOperationResultStats {
    private String tableName;

    // 插入成功次数
    private long insertSuccess = 0;
    // 更新成功次数
    private long updateSuccess = 0;
    // 删除成功次数
    private long deleteSuccess = 0;

    // 插入失败次数
    private long insertFailure = 0;
    // 更新失败次数
    private long updateFailure = 0;
    // 删除失败次数
    private long deleteFailure = 0;

    // 执行时间统计
    private long totalExecutionTime = 0;
    private long operationCount = 0;

    // 窗口信息，本次统计的开始时间
    private long windowStart;
    // 窗口信息，本次统计的结束时间
    private long windowEnd;
    private String serverId;
    private String businessDataSourceId;
    private String targetDateSourceId;

    // 窗口信息，本次统计的开始时间
    private String windowStartFormatted;
    // 窗口信息，本次统计的结束时间
    private String windowEndFormatted;


    public TableOperationResultStats() {}

    public void incrementInsert(boolean success) {
        if (success) {
            insertSuccess++;
        } else {
            insertFailure++;
        }
        operationCount++;
    }

    public void incrementUpdate(boolean success) {
        if (success) {
            updateSuccess++;
        } else {
            updateFailure++;
        }
        operationCount++;
    }

    public void incrementDelete(boolean success) {
        if (success) {
            deleteSuccess++;
        } else {
            deleteFailure++;
        }
        operationCount++;
    }

    public void addExecutionTime(long executionTime) {
        this.totalExecutionTime += executionTime;
    }

    public long getTotalOperations() {
        return insertSuccess + insertFailure + updateSuccess + updateFailure + deleteSuccess + deleteFailure;
    }

    public long getTotalSuccess() {
        return insertSuccess + updateSuccess + deleteSuccess;
    }

    public long getTotalFailure() {
        return insertFailure + updateFailure + deleteFailure;
    }

    public double getSuccessRate() {
        long total = getTotalOperations();
        return total > 0 ? (double) getTotalSuccess() / total * 100 : 0.0;
    }

    public double getAvgExecutionTime() {
        return operationCount > 0 ? (double) totalExecutionTime / operationCount : 0.0;
    }

    public double getThroughputPerSecond() {
        long windowDuration = windowEnd - windowStart;
        if (windowDuration <= 0) return 0.0;
        return (double) getTotalOperations() / (windowDuration / 1000.0);
    }

    // Getters and Setters
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public long getInsertSuccess() { return insertSuccess; }
    public void setInsertSuccess(long insertSuccess) { this.insertSuccess = insertSuccess; }

    public long getInsertFailure() { return insertFailure; }
    public void setInsertFailure(long insertFailure) { this.insertFailure = insertFailure; }

    public long getUpdateSuccess() { return updateSuccess; }
    public void setUpdateSuccess(long updateSuccess) { this.updateSuccess = updateSuccess; }

    public long getUpdateFailure() { return updateFailure; }
    public void setUpdateFailure(long updateFailure) { this.updateFailure = updateFailure; }

    public long getDeleteSuccess() { return deleteSuccess; }
    public void setDeleteSuccess(long deleteSuccess) { this.deleteSuccess = deleteSuccess; }

    public long getDeleteFailure() { return deleteFailure; }
    public void setDeleteFailure(long deleteFailure) { this.deleteFailure = deleteFailure; }

    public long getTotalExecutionTime() { return totalExecutionTime; }
    public void setTotalExecutionTime(long totalExecutionTime) { this.totalExecutionTime = totalExecutionTime; }

    public long getOperationCount() { return operationCount; }
    public void setOperationCount(long operationCount) { this.operationCount = operationCount; }

    public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }


    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }

    public String getTargetDateSourceId() {
        return targetDateSourceId;
    }

    public void setTargetDateSourceId(String targetDateSourceId) {
        this.targetDateSourceId = targetDateSourceId;
    }


    public String getWindowStartFormatted() {
        return windowStartFormatted;
    }

    public void setWindowStartFormatted(String windowStartFormatted) {
        this.windowStartFormatted = windowStartFormatted;
    }

    public String getWindowEndFormatted() {
        return windowEndFormatted;
    }

    public void setWindowEndFormatted(String windowEndFormatted) {
        this.windowEndFormatted = windowEndFormatted;
    }

    @Override
    public String toString() {
        return String.format(
                "TableResultStats{table='%s', total=%d, success=%d(%.1f%%), failure=%d, " +
                        "insert=%d/%d, update=%d/%d, delete=%d/%d, avgTime=%.2fms, throughput=%.2f ops/s}",
                tableName, getTotalOperations(), getTotalSuccess(), getSuccessRate(), getTotalFailure(),
                insertSuccess, insertFailure, updateSuccess, updateFailure, deleteSuccess, deleteFailure,
                getAvgExecutionTime(), getThroughputPerSecond()
        );
    }
}