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
        Map<String, SimpleClientInfo> byDataSourceId = new ConcurrentHashMap<>();
        Map<String, SimpleClientInfo> byClientId = new ConcurrentHashMap<>();

        for (SimpleClientInfo info : clients) {
            byDataSourceId.put(info.getDataSourceId(), info);
            byClientId.put(info.getClientId(), info);
        }

        return new ActiveClientCache(byDataSourceId, byClientId);
    }

    public SimpleClientInfo requireByDataSourceId(String dataSourceId) {
        if (byDataSourceId.isEmpty()) {
            throw new IllegalStateException("active client cache is not initialized");
        }

        SimpleClientInfo info = byDataSourceId.get(dataSourceId);
        if (info == null || info.getClientId() == null || info.getClientId().isEmpty()) {
            throw new IllegalStateException("未找到激活客户端, dataSourceId=" + dataSourceId);
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
