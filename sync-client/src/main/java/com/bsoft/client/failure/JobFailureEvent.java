package com.bsoft.client.failure;

import java.time.LocalDateTime;

// 作业失败事件实体，对应 CDC_JOB_FAILURE_EVENT。
public class JobFailureEvent {

    // 主键ID。
    private long id;

    // 客户端ID。
    private String clientId;

    // 业务数据源ID。
    private String dataSourceId;

    // 失败的Flink Job ID。
    private String failedJobId;

    // 失败发生时间。
    private LocalDateTime failureTime;

    // Flink作业状态。
    private String flinkStatus;

    // 失败原因摘要。
    private String failureReason;

    // 完整失败异常详情。
    private String failureDetail;

    // 失败事件初步处理结果。
    private JobFailureEventResult eventResult;

    // 失败事件被忽略的原因。
    private String ignoreReason;

    // 获取主键ID。
    public long getId() {
        // 返回主键ID。
        return id;
    }

    // 设置主键ID。
    public void setId(long id) {
        // 保存主键ID。
        this.id = id;
    }

    // 获取客户端ID。
    public String getClientId() {
        // 返回客户端ID。
        return clientId;
    }

    // 设置客户端ID。
    public void setClientId(String clientId) {
        // 保存客户端ID。
        this.clientId = clientId;
    }

    // 获取业务数据源ID。
    public String getDataSourceId() {
        // 返回业务数据源ID。
        return dataSourceId;
    }

    // 设置业务数据源ID。
    public void setDataSourceId(String dataSourceId) {
        // 保存业务数据源ID。
        this.dataSourceId = dataSourceId;
    }

    // 获取失败的Flink Job ID。
    public String getFailedJobId() {
        // 返回失败的Flink Job ID。
        return failedJobId;
    }

    // 设置失败的Flink Job ID。
    public void setFailedJobId(String failedJobId) {
        // 保存失败的Flink Job ID。
        this.failedJobId = failedJobId;
    }

    // 获取失败发生时间。
    public LocalDateTime getFailureTime() {
        // 返回失败发生时间。
        return failureTime;
    }

    // 设置失败发生时间。
    public void setFailureTime(LocalDateTime failureTime) {
        // 保存失败发生时间。
        this.failureTime = failureTime;
    }

    // 获取Flink作业状态。
    public String getFlinkStatus() {
        // 返回Flink作业状态。
        return flinkStatus;
    }

    // 设置Flink作业状态。
    public void setFlinkStatus(String flinkStatus) {
        // 保存Flink作业状态。
        this.flinkStatus = flinkStatus;
    }

    // 获取失败原因摘要。
    public String getFailureReason() {
        // 返回失败原因摘要。
        return failureReason;
    }

    // 设置失败原因摘要。
    public void setFailureReason(String failureReason) {
        // 保存失败原因摘要。
        this.failureReason = failureReason;
    }

    // 获取完整失败异常详情。
    public String getFailureDetail() {
        // 返回完整失败异常详情。
        return failureDetail;
    }

    // 设置完整失败异常详情。
    public void setFailureDetail(String failureDetail) {
        // 保存完整失败异常详情。
        this.failureDetail = failureDetail;
    }



    // 获取事件处理结果。
    public JobFailureEventResult getEventResult() {
        // 返回事件处理结果。
        return eventResult;
    }

    // 设置事件处理结果。
    public void setEventResult(JobFailureEventResult eventResult) {
        // 保存事件处理结果。
        this.eventResult = eventResult;
    }

    // 获取忽略原因。
    public String getIgnoreReason() {
        // 返回忽略原因。
        return ignoreReason;
    }

    // 设置忽略原因。
    public void setIgnoreReason(String ignoreReason) {
        // 保存忽略原因。
        this.ignoreReason = ignoreReason;
    }
}