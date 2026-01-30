package com.bsoft.common.enums;

/**
 * ClassName: JobStatus
 * Package:   com.bsoft.enums
 * Description: 作业状态枚举
 *
 * 作业状态分为3类：      0XXX: 因为缺少必要配置，程序无法正常启动，但是会继续运行，等待配置补全
 *                     1XXX: 正常状态
 *                     9XXX: 异常状态，需要人工干预
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 20:33
 */
public enum JobStatus {


    /**
     * 缺少必要配置（无法启动作业）
     */
    ORACLE_CDC_PRECONDITION_NOT_MET("0001", "Oracle CDC 前置条件不满足"),

    /**
     * 缺少必要配置（无法启动作业）
     */
    CONFIG_MISSING("0002", "缺少必要配置"),

    /**
     * 未知或未初始化
     */
    UNKNOWN("1000", "未知状态"),

    /**
     * 初始化状态（系统启动时的默认状态）
     */
    INITIALIZING("1001", "系统初始化"),


    /**
     * 增量模式运行中（增量同步模式）
     */
    INCREMENTAL_INITIALIZING("1100", "增量模式初始化"),

    /**
     * 增量模式运行中（增量同步模式）
     */
    INCREMENTAL_RUNNING("1101", "增量模式运行中"),

    /**
     * 快照模式运行中（正在做快照/补数）
     */
    SNAPSHOT_INITIALIZING("1200", "快照模式初始化"),
    /**
     * 快照模式运行中（正在做快照/补数）
     */
    SNAPSHOT_RUNNING("1201", "快照模式运行中"),

    /**
     * 模式切换中（正在从一种模式切换到另一种模式）
     */
    MODE_SWITCHING("1205", "快照任务已经完成，切换到增量模式"),

    /**
     * 已停止
     */
    STOPPED("2001", "已停止"),

    /**
     * 有异常/作业失败
     */
    FAILED("9001", "有异常"),

    /**
     * 快照模式已完成
     */
    SNAPSHOT_COMPLETED("1005", "快照模式已完成");

    private final String code;
    private final String description;
    private String detailInfo = "everything under control"; // 详细信息，可动态设置

    JobStatus(String code, String description) {
        this.code = code;
        this.description = description;
    }

    JobStatus(String code, String description, String detailInfo) {
        this.code = code;
        this.description = description;
        this.detailInfo = detailInfo;
    }

    /**
     * 创建带详细信息的状态实例
     */
    public JobStatus withDetailInfo(String detailInfo) {
        JobStatus status = JobStatus.valueOf(this.name());
        status.detailInfo = detailInfo;
        return status;
    }

    public String getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public String getDetailInfo() {
        return detailInfo;
    }

    public boolean hasDetailInfo() {
        return detailInfo != null && !detailInfo.trim().isEmpty();
    }

    @Override
    public String toString() {
        if (hasDetailInfo()) {
            return description + " - " + detailInfo;
        }
        return description;
    }

    /**
     * 获取完整的状态信息，包含状态码、描述和详细信息
     */
    public String getFullStatus() {
        if (hasDetailInfo()) {
            return code + ":" + description + " - " + detailInfo;
        }
        return code + ":" + description;
    }

    /**
     * 根据状态码获取对应的JobStatus
     */
    public static JobStatus getByCode(String code) {
        for (JobStatus status : values()) {
            if (status.code.equals(code)) {
                return status;
            }
        }
        return UNKNOWN;
    }
}