package com.bsoft.common.zk;

import com.bsoft.common.bean.ZooKeeperConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JVM 内 ZkClient 复用注册表。
 *
 * 职责：
 * 1. 同一 JVM 内，按 ZooKeeperConfig 复用 ZkClient
 * 2. 管理引用计数
 * 3. 在最后一个使用者释放时关闭底层连接
 *
 * 复用边界：
 * 1. 只在同 JVM 内复用
 * 2. 不跨进程、不跨机器复用
 */
public final class ZkClientRegistry {

    private static final Logger logger = LogManager.getLogger(ZkClientRegistry.class);

    /**
     * 已注册客户端。
     *
     * key: 连接配置键
     * value: 引用包装
     */
    private static final ConcurrentHashMap<ZkClientKey, RefCountedZkClient> CLIENTS =
            new ConcurrentHashMap<ZkClientKey, RefCountedZkClient>();

    private ZkClientRegistry() {
        // 工具类不允许实例化。
    }

    /**
     * 获取一个可复用的 ZkClient。
     *
     * 规则：
     * 1. 如果当前 JVM 中还没有对应配置的 ZkClient，则创建并启动
     * 2. 如果已经存在，则直接复用并增加引用计数
     */
    public static ZkClient acquire(ZooKeeperConfig config) throws Exception {
        if (config == null) {
            throw new IllegalArgumentException("ZooKeeperConfig cannot be null");
        }

        ZkClientKey key = ZkClientKey.from(config);

        while (true) {
            RefCountedZkClient existing = CLIENTS.get(key);
            if (existing != null) {
                existing.retain();

                logger.info("Reusing ZkClient from registry, key={}, refCount={}",
                        key,
                        existing.getRefCount());

                return existing.getClient();
            }

            ZkClient newClient = new ZkClient(config);
            newClient.start();

            RefCountedZkClient created = new RefCountedZkClient(newClient);
            RefCountedZkClient previous = CLIENTS.putIfAbsent(key, created);

            if (previous == null) {
                logger.info("Created new ZkClient in registry, key={}, refCount={}",
                        key,
                        created.getRefCount());

                return created.getClient();
            }

            // 说明并发下已有别的线程先放进去了，这里把自己刚建的关闭掉再重试。
            newClient.close();
        }
    }

    /**
     * 释放一个 ZkClient 引用。
     *
     * 规则：
     * 1. 引用计数减一
     * 2. 若减到 0，则从注册表移除并关闭底层连接
     */
    public static void release(ZooKeeperConfig config) {
        if (config == null) {
            return;
        }

        ZkClientKey key = ZkClientKey.from(config);
        RefCountedZkClient existing = CLIENTS.get(key);

        if (existing == null) {
            logger.warn("Release ignored, ZkClient not found in registry, key={}", key);
            return;
        }

        int refCount = existing.release();

        logger.info("Released ZkClient reference, key={}, refCount={}", key, refCount);

        if (refCount <= 0) {
            if (CLIENTS.remove(key, existing)) {
                existing.getClient().close();

                logger.info("Closed ZkClient after final release, key={}", key);
            }
        }
    }

    /**
     * 获取当前注册表快照。
     *
     * 用于调试和观察。
     */
    public static Map<String, Integer> snapshot() {
        Map<String, Integer> result = new LinkedHashMap<String, Integer>();

        for (Map.Entry<ZkClientKey, RefCountedZkClient> entry : CLIENTS.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue().getRefCount());
        }

        return result;
    }

    /**
     * 关闭当前 JVM 内所有通过注册表持有的 ZkClient。
     *
     * 一般仅用于应用退出或测试清理。
     */
    public static void shutdownAll() {
        for (Map.Entry<ZkClientKey, RefCountedZkClient> entry : CLIENTS.entrySet()) {
            try {
                entry.getValue().getClient().close();
            } catch (Exception ignore) {
            }
        }

        CLIENTS.clear();
        logger.info("ZkClientRegistry shutdown completed");
    }

    /**
     * 带引用计数的客户端包装。
     */
    private static final class RefCountedZkClient {

        /**
         * 被复用的真实客户端。
         */
        private final ZkClient client;

        /**
         * 引用计数。
         *
         * 初始为 1，表示刚创建时已被一个调用方持有。
         */
        private final AtomicInteger refCount = new AtomicInteger(1);

        private RefCountedZkClient(ZkClient client) {
            this.client = client;
        }

        public ZkClient getClient() {
            return client;
        }

        public int retain() {
            return refCount.incrementAndGet();
        }

        public int release() {
            return refCount.decrementAndGet();
        }

        public int getRefCount() {
            return refCount.get();
        }
    }
}