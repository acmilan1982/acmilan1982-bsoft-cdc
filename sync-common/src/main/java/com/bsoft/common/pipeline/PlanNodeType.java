package com.bsoft.common.pipeline;

/**
 * ClassName: PlanNodeType
 * Package:   com.bsoft.common.pipeline
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 17:23
 */
public enum PlanNodeType {
    SOURCE_TOPIC,
    PROJECT,
    SQL,
    JOIN,
    ENRICH_HTTP,
    FUNCTION_CALLING,
    SINK_KAFKA,
    SINK_TABLE
}
