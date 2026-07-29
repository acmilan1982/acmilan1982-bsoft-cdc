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


    private static final String TOPIC_SEPARATOR = ".";
    private static final String ASSOC_TOPIC_PREFIX = "assoc";

    private TopicNameFactory() {
    }

    public static String generateTopic(String clientId, String fromDataSourceId,String schemaName, String tableName, String toDataSourceId) {


        return join(
                clientId.trim(),
                fromDataSourceId.trim(),
                schemaName.trim(),
                tableName.trim(),
                toDataSourceId.trim()
        );
    }

    public static String generateAssocTopic(String clientId, String tableName, String dataSourceId) {


        return join(
                ASSOC_TOPIC_PREFIX,
                clientId.trim(),
                tableName.trim(),
                dataSourceId.trim()
        );
    }

    private static String join(String... parts) {
        return String.join(TOPIC_SEPARATOR, parts);
    }
}
