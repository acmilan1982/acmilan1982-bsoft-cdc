package com.bsoft.pipeline.plan.sql;

/**
 * ClassName: SqlScopeAliasRegistry
 * Package:   com.bsoft.pipeline.plan.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/11 - 16:11
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SqlScopeAliasRegistry {
    private static final Logger log = LoggerFactory.getLogger(SqlScopeAliasRegistry.class);

    private final Map<String, Map<String, String>> scopeAliasMap = new ConcurrentHashMap<>();
    // key: dsId|SCHEMA, value: shortTableName -> uniqueViewName

    public void register(String businessDataSourceId, String schemaName, String shortTableName, String uniqueViewName) {
        String scopeKey = scopeKey(businessDataSourceId, schemaName);
        String shortName = norm(shortTableName);

        scopeAliasMap.computeIfAbsent(scopeKey, k -> new ConcurrentHashMap<>())
                .merge(shortName, uniqueViewName, (oldV, newV) -> {
                    // 理论上同 scope 不应冲突；冲突时保留旧值，仅告警，不抛错
                    if (!Objects.equals(oldV, newV)) {
                        log.warn("Duplicate short table in same scope, keep old mapping. scope={}, table={}, old={}, new={}",
                                scopeKey, shortName, oldV, newV);
                    }
                    return oldV;
                });
    }

    public Map<String, String> findAliases(String businessDataSourceId, String schemaName) {
        String scopeKey = scopeKey(businessDataSourceId, schemaName);
        Map<String, String> map = scopeAliasMap.get(scopeKey);
        return map == null ? Collections.emptyMap() : map;
    }

    private String scopeKey(String dsId, String schema) {
        return norm(dsId) + "|" + norm(schema).toUpperCase(Locale.ROOT);
    }

    private String norm(String s) {
        return s == null ? "" : s.trim().toLowerCase(Locale.ROOT);
    }
}