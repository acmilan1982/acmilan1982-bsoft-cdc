package com.bsoft.common.global;

/**
 * ClassName: GlobalLogCollectorManager
 * Package:   com.bsoft.global
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/27 - 9:53
 */


import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.log.LogCollector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 全局日志收集器管理器
 */
public class GlobalLogCollectorManager {

    private static final ConcurrentHashMap<String, LogCollector> collectors = new ConcurrentHashMap<>();
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * 获取或创建 LogCollector（按数据源分组），
     */
    public static LogCollector getInstance(ConfigDataSourceConfig configDataSourceConfig,RawMessageStorageStrategy rawMessageStorageStrategy) {
        return collectors.computeIfAbsent(configDataSourceConfig.getInstanceId(), key -> {
            LogCollector collector = new LogCollector(configDataSourceConfig,rawMessageStorageStrategy);
            return collector;
        });
    }


    public static LogCollector getInstance(ConfigDataSourceConfig configDataSourceConfig) {
        return collectors.computeIfAbsent(configDataSourceConfig.getInstanceId(), key -> {
            LogCollector collector = new LogCollector(configDataSourceConfig);
            return collector;
        });
    }
    /**
     * 关闭所有 LogCollector
     */
    public static void shutdownAll() {
        LOCK.lock();
        try {
            collectors.values().forEach(LogCollector::shutdown);
            collectors.clear();
        } finally {
            LOCK.unlock();
        }
    }
}