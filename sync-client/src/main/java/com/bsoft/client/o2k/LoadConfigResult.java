package com.bsoft.client.o2k;

/**
 * 配置加载结果
 *
 * 设计目的：
 * 1. 用于表达 OracleToKafka 在加载配置阶段的结果
 * 2. 不直接等同于 ClientProcessStatus
 * 3. 由 ClientJobDispatcher 根据该结果决定如何映射到 ClientProcessStatus
 *
 * 职责边界：
 * 1. OracleToKafka 负责创建该对象
 * 2. ClientJobDispatcher 负责消费该对象并决定写入什么进程级状态
 *
 * 典型示例：
 * 1. SUCCESS
 * 2. BUSINESS_DATA_SOURCE_MISSING + 当前客户端未被分配任何业务库数据源，将在10秒后重试
 * 3. SUBSCRIBER_MAPPING_MISSING + 订阅关系配置缺失，将在10秒后重试
 * 4. TARGET_DATA_SOURCE_MISSING + 目标数据源配置缺失，将在10秒后重试
 */
public class LoadConfigResult {

    /**
     * 配置加载结果类型
     */
    private final LoadConfigResultType type;

    /**
     * 详细说明
     *
     * 说明：
     * 1. SUCCESS 时可为空
     * 2. 配置缺失时应尽量写清楚具体原因，供上层映射到 ClientProcessStatus.detailInfo
     */
    private final String detailInfo;

    private LoadConfigResult(LoadConfigResultType type, String detailInfo) {
        if (type == null) {
            throw new IllegalArgumentException("LoadConfigResultType cannot be null");
        }
        this.type = type;
        this.detailInfo = detailInfo;
    }

    /**
     * 创建成功结果
     */
    public static LoadConfigResult success() {
        return new LoadConfigResult(LoadConfigResultType.SUCCESS, "");
    }

    /**
     * 创建配置缺失类结果
     *
     * @param type 配置缺失类型
     * @param detailInfo 详细说明
     */
    public static LoadConfigResult of(LoadConfigResultType type, String detailInfo) {
        return new LoadConfigResult(type, detailInfo == null ? "" : detailInfo);
    }

    /**
     * 是否成功
     */
    public boolean isSuccess() {
        return type == LoadConfigResultType.SUCCESS;
    }

    public LoadConfigResultType getType() {
        return type;
    }

    public String getDetailInfo() {
        return detailInfo;
    }

    @Override
    public String toString() {
        return "LoadConfigResult{" +
                "type=" + type +
                ", detailInfo='" + detailInfo + '\'' +
                '}';
    }
}