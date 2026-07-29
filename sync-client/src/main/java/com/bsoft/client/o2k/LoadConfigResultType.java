package com.bsoft.client.o2k;

/**
 * 配置加载结果类型
 *
 * 说明：
 * 1. 只表达“结果类别”
 * 2. 不携带 detailInfo
 * 3. 具体缺失原因、补充说明等由 LoadConfigResult 承载
 */
public enum LoadConfigResultType {

    /**
     * 所有必要配置都已加载成功
     */
    SUCCESS,

    /**
     * 当前 client 未被分配任何业务库
     */
    BUSINESS_DATA_SOURCE_MISSING,

    /**
     * 订阅关系缺失
     */
    SUBSCRIBER_MAPPING_MISSING,

    /**
     * 目标数据源配置缺失
     */
    TARGET_DATA_SOURCE_MISSING
}