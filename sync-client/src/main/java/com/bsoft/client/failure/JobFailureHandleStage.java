package com.bsoft.client.failure;

// 作业失败处理阶段枚举，对应 CDC_JOB_FAILURE_HANDLE_LOG.HANDLE_STAGE。
public enum JobFailureHandleStage {

    // 收到有效失败事件。
    JOB_FAILURE_RECEIVED("JOB_FAILURE_RECEIVED", "收到有效失败事件"),

    // 忽略无效失败事件。
    JOB_FAILURE_IGNORED_INVALID("JOB_FAILURE_IGNORED_INVALID", "忽略无效失败事件"),

    // 忽略旧 job 失败事件。
    JOB_FAILURE_IGNORED_STALE("JOB_FAILURE_IGNORED_STALE", "忽略旧 job 失败事件"),

    // 忽略重复失败事件。
    DUPLICATED_EVENT_IGNORED("DUPLICATED_EVENT_IGNORED", "忽略重复失败事件"),

    // 已安排自动重启。
    RESTART_SCHEDULED("RESTART_SCHEDULED", "已安排自动重启"),

    // 已跳过计划重启。
    SCHEDULED_RESTART_SKIPPED("SCHEDULED_RESTART_SKIPPED", "已跳过计划重启"),

    // 开始执行重启。
    RESTART_STARTED("RESTART_STARTED", "开始执行重启"),

    // 新 job 提交成功。
    NEW_JOB_SUBMIT_SUCCEEDED("NEW_JOB_SUBMIT_SUCCEEDED", "新 job 提交成功"),

    // 新 job 提交失败。
    NEW_JOB_SUBMIT_FAILED("NEW_JOB_SUBMIT_FAILED", "新 job 提交失败"),

    // 稳定运行检查通过。
    STABLE_CHECK_PASSED("STABLE_CHECK_PASSED", "稳定运行检查通过");

    // 入库编码。
    private final String code;

    // 阶段描述。
    private final String description;

    // 创建枚举项。
    JobFailureHandleStage(String code, String description) {
        // 保存入库编码。
        this.code = code;
        // 保存阶段描述。
        this.description = description;
    }

    // 获取入库编码。
    public String getCode() {
        // 返回入库编码。
        return code;
    }

    // 获取阶段描述。
    public String getDescription() {
        // 返回阶段描述。
        return description;
    }
}