package com.bsoft.pipeline.k2k;

import com.bsoft.common.dbmeta.OracleColumnsQueryResult;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName: SqlNodeSchemaRegistry
 * Package:   com.bsoft.pipeline.k2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/3 - 14:53
 */

public class SqlNodeSchemaRegistry implements Serializable {

    private final Map<String, OracleColumnsQueryResult> cache = new ConcurrentHashMap<>();

    public void register(String planId, String nodeId, OracleColumnsQueryResult result) {
        cache.put(buildKey(planId, nodeId), result);
    }

    public OracleColumnsQueryResult resolve(String planId, String nodeId) {
        return cache.get(buildKey(planId, nodeId));
    }

    public OracleColumnsQueryResult resolveRequired(String planId, String nodeId) {
        OracleColumnsQueryResult result = resolve(planId, nodeId);
        if (result == null) {
            throw new IllegalStateException(
                    "sql node schema not found, planId=" + planId + ", nodeId=" + nodeId
            );
        }
        return result;
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    private String buildKey(String planId, String nodeId) {
        return String.valueOf(planId).trim() + "|" + String.valueOf(nodeId).trim();
    }
}