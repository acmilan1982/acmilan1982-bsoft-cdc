package com.bsoft.log.global;

import com.bsoft.common.config.properties.LogInfo;
import com.bsoft.log.function.LogCollector;
import com.bsoft.log.offset.JdbcLogTopicOffsetRepository;
import com.bsoft.log.offset.LogTopicOffsetRepository;
import com.bsoft.log.offset.LogTopicOffsetTracker;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * sync-log 专用 LogCollector 管理器。
 */
public class GlobalLogCollectorManager {

    /**
     * 按 logId 缓存 LogCollector。
     */
    private static final ConcurrentHashMap<String, LogCollector> collectors = new ConcurrentHashMap<>();

    /**
     * 关闭锁。
     */
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * 获取或创建当前 sync-log 实例的 LogCollector。
     */
    public static LogCollector getInstance(LogInfo logInfo) {
        // 校验 LogInfo 不能为空。
        if (logInfo == null) {
            throw new IllegalArgumentException("logInfo is null");
        }

        // 获取 logId 作为 collector 缓存 key。
        String logId = requireText(logInfo.getLogId(), "logId");

        // 按 logId 创建或复用 LogCollector。
        return collectors.computeIfAbsent(logId, key -> createCollector(logInfo));
    }

    /**
     * 创建 LogCollector。
     */
    private static LogCollector createCollector(LogInfo logInfo) {
        try {
            // 创建 offset 仓储。
            LogTopicOffsetRepository repository =
                    new JdbcLogTopicOffsetRepository(logInfo.getConfigDataSourceConfig().initDataSource());

            // 从 CDC_LOG_TOPIC_OFFSET 读取当前 topic 的 nextOffset，缺失时 repository 负责初始化为 0。
            Map<String, Long> offsets = repository.loadNextOffsets(
                    logInfo.getLogId(),
                    Collections.singletonList(logInfo.getLogTopic())
            );

            // 获取当前日志 topic 的初始 nextOffset。
            long initialNextOffset = offsets.getOrDefault(logInfo.getLogTopic(), 0L);

            // 创建连续 offset 水位线追踪器。
            LogTopicOffsetTracker offsetTracker = new LogTopicOffsetTracker(
                    logInfo.getLogId(),
                    logInfo.getLogTopic(),
                    initialNextOffset,
                    repository
            );

            // 创建 LogCollector，并注入 offsetTracker。
            return new LogCollector(
                    logInfo.getConfigDataSourceConfig(),
                    logInfo.getRawMessageStorageStrategy(),
                    offsetTracker
            );
        } catch (Exception e) {
            // 创建失败时抛出运行时异常，让 Flink 算子初始化失败。
            throw new RuntimeException("failed to create LogCollector", e);
        }
    }

    /**
     * 关闭所有 LogCollector。
     */
    public static void shutdownAll() {
        // 加锁避免并发创建和关闭交错。
        LOCK.lock();

        try {
            // 逐个关闭 LogCollector。
            collectors.values().forEach(LogCollector::shutdown);

            // 清空缓存。
            collectors.clear();
        } finally {
            // 释放锁。
            LOCK.unlock();
        }
    }

    /**
     * 校验文本不能为空。
     */
    private static String requireText(String value, String field) {
        // 空值或空白字符串都视为非法。
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }

        // 返回 trim 后文本。
        return value.trim();
    }
}