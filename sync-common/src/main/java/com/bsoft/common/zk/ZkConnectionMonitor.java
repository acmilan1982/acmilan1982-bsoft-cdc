package com.bsoft.common.zk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ZooKeeper 连接监控器。
 *
 * 职责：
 * 1. 记录当前 JVM 内创建了多少个 ZooKeeper 连接实例
 * 2. 记录每个连接实例的创建时间
 * 3. 定时输出连接健康信息
 *
 * 不负责：
 * 1. 创建 Curator 客户端
 * 2. 节点增删改查
 * 3. 任何业务路径语义
 */
public final class ZkConnectionMonitor {

    private static final Logger logger = LogManager.getLogger(ZkConnectionMonitor.class);

    /**
     * 当前活跃连接数。
     */
    private static final AtomicInteger CONNECTION_COUNT = new AtomicInteger(0);

    /**
     * 连接注册表。
     *
     * key: instanceId
     * value: createTimeMillis
     */
    private static final ConcurrentHashMap<String, Long> CONNECTION_REGISTRY =
            new ConcurrentHashMap<String, Long>();

    /**
     * 定时监控线程池。
     *
     * 这里只保留一个单线程，避免并发打印监控日志。
     */
    private static final ScheduledExecutorService MONITOR_EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "zk-connection-monitor");
                }
            });

    static {
        // 每 60 秒打印一次连接状态。
        MONITOR_EXECUTOR.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                printConnectionStatus();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    private ZkConnectionMonitor() {
        // 工具类不允许实例化。
    }

    /**
     * 注册一个新的 ZooKeeper 连接实例。
     *
     * @param instanceId 连接实例唯一标识
     * @param createTimeMillis 创建时间戳
     */
    public static void register(String instanceId, long createTimeMillis) {
        CONNECTION_REGISTRY.put(instanceId, createTimeMillis);
        int count = CONNECTION_COUNT.incrementAndGet();

        logger.info("Registered ZooKeeper connection, instanceId={}, totalConnections={}",
                instanceId,
                count);
    }

    /**
     * 注销一个 ZooKeeper 连接实例。
     *
     * @param instanceId 连接实例唯一标识
     */
    public static void unregister(String instanceId) {
        if (CONNECTION_REGISTRY.remove(instanceId) != null) {
            int count = CONNECTION_COUNT.decrementAndGet();

            logger.info("Unregistered ZooKeeper connection, instanceId={}, totalConnections={}",
                    instanceId,
                    count);
        }
    }

    /**
     * 获取当前活跃连接数。
     */
    public static int getCurrentConnectionCount() {
        return CONNECTION_COUNT.get();
    }

    /**
     * 获取当前连接注册表快照。
     *
     * 返回副本，避免外部修改内部状态。
     */
    public static Map<String, Long> getConnectionRegistrySnapshot() {
        return new LinkedHashMap<String, Long>(CONNECTION_REGISTRY);
    }

    /**
     * 关闭监控线程池。
     *
     * 一般只在应用关闭时调用。
     */
    public static void shutdown() {
        if (!MONITOR_EXECUTOR.isShutdown()) {
            MONITOR_EXECUTOR.shutdown();
            try {
                if (!MONITOR_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                    MONITOR_EXECUTOR.shutdownNow();
                }
            } catch (InterruptedException e) {
                MONITOR_EXECUTOR.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 打印当前连接状态。
     *
     * 这里只做日志监控，不做任何连接修复动作。
     */
    private static void printConnectionStatus() {
        int totalConnections = CONNECTION_COUNT.get();
        long now = System.currentTimeMillis();

        logger.info("ZooKeeper connection report: totalConnections={}, registrySize={}",
                totalConnections,
                CONNECTION_REGISTRY.size());

        // 连接数过多时给出告警。
        if (totalConnections > 20) {
            logger.warn("ZooKeeper connection count is high: {}", totalConnections);
        }

        // 当连接数较多时，输出每个连接的存活时间，便于排查泄漏。
        if (totalConnections > 10) {
            for (Map.Entry<String, Long> entry : CONNECTION_REGISTRY.entrySet()) {
                long ageMs = now - entry.getValue().longValue();
                logger.info("ZooKeeper connection detail: instanceId={}, ageMs={}",
                        entry.getKey(),
                        ageMs);
            }
        }
    }
}