package com.bsoft.client.failure;

// 作业失败事件处理结果枚举，对应 CDC_JOB_FAILURE_EVENT.EVENT_RESULT。
public enum JobFailureEventResult {

    // 有效失败事件，后续会进入失败处理流程。
    ACCEPTED("ACCEPTED", "有效失败事件"),

    // 无效失败事件，例如 dataSourceId 或 failedJobId 为空。
    IGNORED_INVALID("IGNORED_INVALID", "无效失败事件"),

    // 旧 job 回调事件，当前 runtime 已不再指向该 job。
    IGNORED_STALE("IGNORED_STALE", "旧 job 回调事件");

    // 入库编码。
    private final String code;

    // 枚举描述。
    private final String description;

    // 创建枚举实例。
    JobFailureEventResult(String code, String description) {
        // 保存入库编码。
        this.code = code;

        // 保存枚举描述。
        this.description = description;
    }

    // 获取入库编码。
    public String getCode() {
        // 返回入库编码。
        return code;
    }

    // 获取枚举描述。
    public String getDescription() {
        // 返回枚举描述。
        return description;
    }
}