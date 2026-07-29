package com.bsoft.client.failure;

import java.time.LocalDateTime;

// 作业失败处理记录实体，对应 CDC_JOB_FAILURE_HANDLE_LOG。
public class JobFailureHandleLog {

    // 主键ID。
    public long id;

    // 关联的失败事件ID。
    public long failureEventId;

    // 客户端ID。
    public String clientId;

    // 业务数据源ID。
    public String dataSourceId;

    // 失败的Flink Job ID。
    public String failedJobId;

    // 当前失败事件下的第几次重启尝试。
    public Integer attemptNo;

    // 处理阶段。
    public JobFailureHandleStage handleStage;

    // 当前处理动作发生时间。
    public LocalDateTime handleTime;

    // 当前连续失败次数。
    public Integer consecutiveFailures;

    // 当前累计重启次数。
    public Long restartCountTotal;

    // 本次重启退避延迟秒数。
    public Long restartDelaySeconds;

    // 下次计划重启时间。
    public LocalDateTime nextRestartTime;

    // 本次重启开始时间。
    public LocalDateTime restartStartTime;

    // 本次重启结束时间。
    public LocalDateTime restartEndTime;

    // 重启成功后的新Flink Job ID。
    public String newJobId;

    // 当前处理动作的异常详情。
    public String errorDetail;

    // 备注。
    public String remark;
}