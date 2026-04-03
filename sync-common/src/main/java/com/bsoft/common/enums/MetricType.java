package com.bsoft.common.enums;

/**
 * ClassName: MetricType
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/2/28 - 16:09
 */
public enum MetricType {
    OFFSET("offset"),
    UNCONSUMED_MESSAGE_COUNT("unconsumedMessageCount");

    private final String code;

    MetricType(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
