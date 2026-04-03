package com.bsoft.common.factory;

/**
 * ClassName: TopicNameFactory
 * Package:   com.bsoft.common.factory
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/30 - 13:59
 */
public class TopicNameFactory {

    private static final String ASSOC_TOPIC_PREFIX = "assoc";

    private TopicNameFactory() {
    }

    public static String generateAssocTopic(String tableName, String dataSourceId) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("tableName must not be blank");
        }
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("dataSourceId must not be blank");
        }
        return ASSOC_TOPIC_PREFIX + "." + tableName.trim() + "." + dataSourceId.trim();
    }
}
