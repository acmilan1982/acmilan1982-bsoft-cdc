package com.bsoft.common.zk;

import com.bsoft.common.bean.ZooKeeperConfig;

/**
 * ZkClient 复用键。
 *
 * 职责：
 * 1. 将 ZooKeeperConfig 中影响连接复用的关键字段抽取出来
 * 2. 用于 JVM 内识别“是否可以复用同一个 ZkClient”
 *
 * 说明：
 * 只有连接语义一致的配置，才能共享同一个 ZkClient。
 */
public final class ZkClientKey {

    /**
     * ZK 连接地址。
     */
    private final String connectString;

    /**
     * ZK namespace 对应的 basePath。
     */
    private final String basePath;

    /**
     * session timeout。
     */
    private final int sessionTimeoutMs;

    /**
     * connection timeout。
     */
    private final int connectionTimeoutMs;

    /**
     * retry count。
     */
    private final int retryCount;

    /**
     * retry interval。
     */
    private final int retryIntervalMs;

    public ZkClientKey(String connectString,
                       String basePath,
                       int sessionTimeoutMs,
                       int connectionTimeoutMs,
                       int retryCount,
                       int retryIntervalMs) {
        this.connectString = normalize(connectString);
        this.basePath = normalize(basePath);
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.retryCount = retryCount;
        this.retryIntervalMs = retryIntervalMs;
    }

    /**
     * 从 ZooKeeperConfig 构造复用键。
     */
    public static ZkClientKey from(ZooKeeperConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ZooKeeperConfig cannot be null");
        }

        return new ZkClientKey(
                config.getConnectString(),
                config.getBasePath(),
                config.getSessionTimeoutMs(),
                config.getConnectionTimeoutMs(),
                config.getRetryCount(),
                config.getRetryIntervalMs()
        );
    }

    public String getConnectString() {
        return connectString;
    }

    public String getBasePath() {
        return basePath;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getRetryIntervalMs() {
        return retryIntervalMs;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ZkClientKey.class) {
            return false;
        }

        ZkClientKey other = (ZkClientKey) obj;

        return sessionTimeoutMs == other.sessionTimeoutMs
                && connectionTimeoutMs == other.connectionTimeoutMs
                && retryCount == other.retryCount
                && retryIntervalMs == other.retryIntervalMs
                && safeEquals(connectString, other.connectString)
                && safeEquals(basePath, other.basePath);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + safeHash(connectString);
        result = 31 * result + safeHash(basePath);
        result = 31 * result + sessionTimeoutMs;
        result = 31 * result + connectionTimeoutMs;
        result = 31 * result + retryCount;
        result = 31 * result + retryIntervalMs;
        return result;
    }

    @Override
    public String toString() {
        return "ZkClientKey{" +
                "connectString='" + connectString + '\'' +
                ", basePath='" + basePath + '\'' +
                ", sessionTimeoutMs=" + sessionTimeoutMs +
                ", connectionTimeoutMs=" + connectionTimeoutMs +
                ", retryCount=" + retryCount +
                ", retryIntervalMs=" + retryIntervalMs +
                '}';
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    private static boolean safeEquals(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    private static int safeHash(Object value) {
        return value == null ? 0 : value.hashCode();
    }
}