package com.bsoft.common.bean;

import com.bsoft.common.config.properties.SimpleClientInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ClassName: ActiveClientCache
 * Package:   com.bsoft.common.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/26 - 16:58
 */
public class ActiveClientCache {

    private final Map<String, SimpleClientInfo> byDataSourceId;
    private final Map<String, SimpleClientInfo> byClientId;

    public ActiveClientCache(Map<String, SimpleClientInfo> byDataSourceId,
                              Map<String, SimpleClientInfo> byClientId) {
        this.byDataSourceId = byDataSourceId;
        this.byClientId = byClientId;
    }

    public static ActiveClientCache empty() {
        return new ActiveClientCache(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
    }

    public static ActiveClientCache from(List<SimpleClientInfo> clients) {
        // key:DATA_SOURCE_ID
        Map<String, SimpleClientInfo> byDataSourceId = new ConcurrentHashMap<>();

        // key:CLIENT_ID
        Map<String, SimpleClientInfo> byClientId = new ConcurrentHashMap<>();

        // 入参为空时直接返回空缓存
        if (clients == null || clients.isEmpty()) {
            return new ActiveClientCache(byDataSourceId, byClientId);
        }

        // 遍历所有激活客户端
        for (SimpleClientInfo info : clients) {
            // 跳过空对象
            if (info == null) {
                continue;
            }

            // 读取客户端 ID
            String clientId = info.getClientId();

            // 客户端 ID 非空时写入客户端缓存
            if (clientId != null && !clientId.trim().isEmpty()) {
                byClientId.putIfAbsent(clientId.trim(), info);
            }

            // 遍历当前客户端负责的所有数据源 ID
            for (String dataSourceId : info.getDataSourceIdList()) {
                // 同一个 DATA_SOURCE_ID 只保留第一条
                byDataSourceId.putIfAbsent(dataSourceId, info);
            }
        }

        // 返回构建完成的缓存对象
        return new ActiveClientCache(byDataSourceId, byClientId);
    }

    public SimpleClientInfo requireByDataSourceId(String dataSourceId) {
        SimpleClientInfo info = byDataSourceId.get(dataSourceId);
        if (info == null || info.getClientId() == null || info.getClientId().isEmpty()) {
            throw new IllegalStateException("未找到活跃客户端, dataSourceId=" + dataSourceId);
        }

        return info;
    }

    public Map<String, SimpleClientInfo> byDataSourceIdView() {
        return Collections.unmodifiableMap(byDataSourceId);
    }

    public Map<String, SimpleClientInfo> byClientIdView() {
        return Collections.unmodifiableMap(byClientId);
    }

    public int size() {
        return byDataSourceId.size();
    }
}
