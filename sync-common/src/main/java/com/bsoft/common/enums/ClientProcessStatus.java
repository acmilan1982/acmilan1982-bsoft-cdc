package com.bsoft.common.enums;

/**
 * sync-client 进程级状态
 *
 * 设计目的：
 * 1. 用于表示“客户端进程本身”的整体状态
 * 2. 不再用于表达某个具体业务库 job 的运行模式
 * 3. 配合 ZooKeeper 节点 /clients/{clientId}/status 使用
 *
 * 与 JobStatus 的区别：
 * 1. JobStatus 表达的是单个 job 的运行状态，例如：
 *    - SNAPSHOT_RUNNING
 *    - INCREMENTAL_RUNNING
 *    - FAILED
 * 2. ClientProcessStatus 表达的是整个 sync-client 进程的状态，例如：
 *    - INITIALIZING
 *    - CONFIG_MISSING
 *    - RUNNING
 *    - PARTIALLY_DEGRADED
 *    - STOPPING
 *    - STOPPED
 *    - ERROR
 *
 * 使用原则：
 * 1. /clients/{clientId}/status 只写 ClientProcessStatus
 * 2. /clients/{clientId}/jobs/{dataSourceId}/status 才写 job 级状态
 *
 * 状态码规划：
 * 0XXX: 配置缺失或前置条件未满足，进程无法进入正常运行态，但仍会继续存活并重试
 * 1XXX: 正常运行相关状态
 * 2XXX: 停止相关状态
 * 9XXX: 进程级错误状态
 */
public enum ClientProcessStatus {

    /**
     * 缺少必要配置
     *
     * 典型场景：
     * 1. 当前客户端未被分配任何业务库数据源
     * 2. 订阅关系配置缺失
     * 3. 目标数据源配置缺失
     *
     * 说明：
     * 详细缺失内容通过 detailInfo 补充。
     */
    CONFIG_MISSING("0001", "缺少必要配置"),

    /**
     * 初始化中
     *
     * 典型场景：
     * 1. sync-client 刚启动
     * 2. 正在初始化 ZK 连接
     * 3. 正在加载配置
     * 4. 正在启动共享 MiniCluster
     * 5. 正在提交多个业务库 job
     */
    INITIALIZING("1001", "进程初始化中"),

    /**
     * 运行正常
     *
     * 典型含义：
     * 1. sync-client 进程已启动完成
     * 2. 共享 MiniCluster 正常
     * 3. 预期中的 job 均已正常提交并处于健康状态
     */
    RUNNING("1002", "进程运行正常"),

    /**
     * 部分降级
     *
     * 典型含义：
     * 1. sync-client 进程仍然存活
     * 2. 共享 MiniCluster 仍然可用
     * 3. 但至少一个业务库 job 启动失败、已停止、异常退出，或处于不健康状态
     *
     * 说明：
     * 这是多 job 模式下非常重要的一个状态，
     * 用于表达“进程活着，但部分 job 异常”。
     */
    PARTIALLY_DEGRADED("1003", "进程部分降级"),

    /**
     * 正在停止
     *
     * 典型场景：
     * 1. 进程收到关闭信号
     * 2. 正在 cancel jobs
     * 3. 正在关闭共享 MiniCluster
     * 4. 正在关闭 ZK / 其他资源
     */
    STOPPING("2001", "进程正在停止"),

    /**
     * 已停止
     *
     * 典型含义：
     * 1. 进程的关闭流程已经完成
     * 2. 所有 job 已停止
     * 3. 共享 MiniCluster 已关闭
     *
     * 注意：
     * 如果 JVM 直接退出，这个状态不一定总能成功写入 ZK。
     */
    STOPPED("2002", "进程已停止"),

    /**
     * 进程级错误
     *
     * 典型场景：
     * 1. 共享 MiniCluster 启动失败
     * 2. client 进程级 ZK 初始化失败
     * 3. 其他导致整个 sync-client 无法提供服务的严重错误
     *
     * 说明：
     * 这个状态应尽量保留给“进程级故障”，
     * 而不是单个 job 的故障。
     */
    ERROR("9001", "进程异常");

    /**
     * 状态码
     */
    private final String code;

    /**
     * 状态说明
     */
    private final String description;

    /**
     * 详细信息，可动态设置
     *
     * 说明：
     * 1. 用于补充说明当前进程状态的上下文
     * 2. 例如：
     *    - 当前客户端未被分配任何业务库数据源，将在10秒后重试
     *    - 订阅关系配置缺失，将在10秒后重试
     *    - Shared MiniCluster 启动失败: xxx
     */
    private String detailInfo = "";

    ClientProcessStatus(String code, String description) {
        this.code = code;
        this.description = description;
    }

    ClientProcessStatus(String code, String description, String detailInfo) {
        this.code = code;
        this.description = description;
        this.detailInfo = detailInfo;
    }

    /**
     * 创建带详细信息的状态实例
     *
     * 说明：
     * 1. 这里沿用 JobStatus 的风格
     * 2. 便于在写入 ZK 或日志时补充上下文信息
     */
    public ClientProcessStatus withDetailInfo(String detailInfo) {
        ClientProcessStatus status = ClientProcessStatus.valueOf(this.name());
        status.detailInfo = detailInfo;
        return status;
    }

    /**
     * 获取状态码
     */
    public String getCode() {
        return code;
    }

    /**
     * 获取状态说明
     */
    public String getDescription() {
        return description;
    }

    /**
     * 获取详细信息
     */
    public String getDetailInfo() {
        return detailInfo;
    }

    /**
     * 是否包含详细信息
     */
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
     * 获取完整状态信息
     *
     * 格式：
     * code:description - detailInfo
     */
    public String getFullStatus() {
        if (hasDetailInfo()) {
            return code + ":" + description + " - " + detailInfo;
        }
        return code + ":" + description;
    }

    /**
     * 根据状态码获取对应的 ClientProcessStatus
     */
    public static ClientProcessStatus getByCode(String code) {
        for (ClientProcessStatus status : values()) {
            if (status.code.equals(code)) {
                return status;
            }
        }
        return ERROR;
    }
}