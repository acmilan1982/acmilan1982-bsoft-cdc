package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import com.bsoft.common.dbmeta.OracleColumnsQueryResult;
import com.bsoft.pipeline.k2k.RuntimeTableSchema;
import com.bsoft.pipeline.k2k.RuntimeTableSchemaBuilder;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolve {@link RuntimeTableSchema} from topic metadata and preloaded table schema cache.
 */
public class DefaultRuntimeTableSchemaResolver
        implements DefaultIncrementalPhaseRunner.RuntimeTableSchemaResolver {

    private final Map<String, OracleColumnsQueryResult> tableSchemaCache;

    public DefaultRuntimeTableSchemaResolver(Map<String, OracleColumnsQueryResult> tableSchemaCache) {
        this.tableSchemaCache = tableSchemaCache == null
                ? new ConcurrentHashMap<>()
                : tableSchemaCache;
    }

    @Override
    public RuntimeTableSchema resolve(ServerTopicInfo topicInfo) {
        Objects.requireNonNull(topicInfo, "topicInfo");

        String clientId = requireText(topicInfo.getClientId(), "topicInfo.clientId");
        String schemaName = requireText(topicInfo.getSchemaName(), "topicInfo.schemaName");
        String tableName = requireText(topicInfo.getTableName(), "topicInfo.tableName");

        String cacheKey = buildCacheKey(clientId, schemaName);
        OracleColumnsQueryResult result = tableSchemaCache.get(cacheKey);
        if (result == null) {
            throw new IllegalStateException("tableSchemaCache miss, key=" + cacheKey);
        }
        if (result.getTableColumns() == null || result.getTableColumns().isEmpty()) {
            throw new IllegalStateException("tableSchemaCache tableColumns is empty, key=" + cacheKey);
        }

        String upperTable = tableName.trim().toUpperCase(Locale.ROOT);
        List<OracleColumnMetadata> columns = result.getTableColumns().get(upperTable);
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("no columns found, key=" + cacheKey + ", table=" + upperTable);
        }

        return RuntimeTableSchemaBuilder.build(columns);
    }

    private String buildCacheKey(String clientId, String schemaName) {
        return clientId.trim() + "_" + schemaName.trim().toUpperCase(Locale.ROOT);
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }
}
