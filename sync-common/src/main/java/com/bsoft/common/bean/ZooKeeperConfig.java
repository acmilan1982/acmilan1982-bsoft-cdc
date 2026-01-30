package com.bsoft.common.bean;

/**
 * ClassName: ZooKeeperConfig
 * Package:   com.bsoft.zk
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:54
 */

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * ZooKeeper配置类
 */
@Component
@ConfigurationProperties(prefix = "zookeeper")
public class ZooKeeperConfig  implements Serializable {
    private static final long serialVersionUID = 1L;

    private String connectString; // ZK连接地址，如 "localhost:2181"
    private int sessionTimeoutMs = 60000; // 会话超时时间
    private int connectionTimeoutMs = 15000; // 连接超时时间
    private String basePath = "/bsoft-cdc"; // 基础路径
    private int retryCount = 3; // 重试次数
    private int retryIntervalMs = 1000; // 重试间隔

    public ZooKeeperConfig() {}

    public ZooKeeperConfig(String connectString) {
        this.connectString = connectString;
    }

    // Getters and Setters
    public String getConnectString() { return connectString; }
    public void setConnectString(String connectString) { this.connectString = connectString; }

    public int getSessionTimeoutMs() { return sessionTimeoutMs; }
    public void setSessionTimeoutMs(int sessionTimeoutMs) { this.sessionTimeoutMs = sessionTimeoutMs; }

    public int getConnectionTimeoutMs() { return connectionTimeoutMs; }
    public void setConnectionTimeoutMs(int connectionTimeoutMs) { this.connectionTimeoutMs = connectionTimeoutMs; }

    public String getBasePath() { return basePath; }
    public void setBasePath(String basePath) { this.basePath = basePath; }

    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

    public int getRetryIntervalMs() { return retryIntervalMs; }
    public void setRetryIntervalMs(int retryIntervalMs) { this.retryIntervalMs = retryIntervalMs; }

    @Override
    public String toString() {
        return "ZooKeeperConfig{" +
                "connectString='" + connectString + '\'' +
                ", sessionTimeoutMs=" + sessionTimeoutMs +
                ", connectionTimeoutMs=" + connectionTimeoutMs +
                ", basePath='" + basePath + '\'' +
                ", retryCount=" + retryCount +
                ", retryIntervalMs=" + retryIntervalMs +
                '}';
    }
}
