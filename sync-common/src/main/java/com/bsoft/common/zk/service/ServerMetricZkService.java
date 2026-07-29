package com.bsoft.common.zk.service;

import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.metric.MonitorMetricEvent;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.bsoft.common.zk.ZkClientRegistry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;

/**
 * server 监控指标 ZooKeeper 服务。
 *
 * 职责：
 * 1. 负责 server 监控类节点的写入
 * 2. 负责 offset / lag / source-table-name / 统计窗口数据
 *
 * 生命周期说明：
 * 1. 推荐通过 open(...) 打开
 * 2. open(...) 内部会复用 JVM 内共享的 ZkClient
 * 3. 使用结束后调用 close() 释放引用
 */
public class ServerMetricZkService {

    private static final Logger logger = LogManager.getLogger(ServerMetricZkService.class);

    /**
     * server 配置。
     */
    private final ServerInfo serverInfo;

    /**
     * 底层 ZK 客户端。
     */
    private final ZkClient zkClient;

    /**
     * 当前实例是否持有共享连接引用。
     */
    private final boolean sharedClient;

    private ServerMetricZkService(ServerInfo serverInfo, ZkClient zkClient, boolean sharedClient) {
        if (serverInfo == null) {
            throw new IllegalArgumentException("ServerInfo cannot be null");
        }
        if (zkClient == null) {
            throw new IllegalArgumentException("ZkClient cannot be null");
        }

        this.serverInfo = serverInfo;
        this.zkClient = zkClient;
        this.sharedClient = sharedClient;
    }

    /**
     * 打开一个 server metric 级 ZK 服务。
     */
    public static ServerMetricZkService open(ServerInfo serverInfo) throws Exception {
        ZkClient zkClient = ZkClientRegistry.acquire(serverInfo.getZooKeeperConfig());
        return new ServerMetricZkService(serverInfo, zkClient, true);
    }

    /**
     * 使用外部提供的 ZkClient 创建 metric 服务。
     */
    public static ServerMetricZkService using(ServerInfo serverInfo, ZkClient zkClient) {
        return new ServerMetricZkService(serverInfo, zkClient, false);
    }

//    /**
//     * 立即写入任意统计数据节点。
//     *
//     * 说明：
//     * 1. 适用于类似 window 统计结果这种直接覆盖型数据
//     * 2. 若节点不存在，则自动创建
//     */
//    public void updateStatisticsImmediately(String path, Object data) throws Exception {
//        zkClient.createOrSet(path, data, CreateMode.PERSISTENT);
//    }

    /**
     * 记录 source table name 映射。
     */
    public void mergeSourceTableName(ServerTopicInfo topicInfo) throws Exception {
        if (topicInfo == null) {
            return;
        }

        String path = serverInfo.getMonitorDataPath()
                + "/" + topicInfo.getTargetDataSourceId()
                + "/" + topicInfo.getTargetTableName()
                + "/source_table_name";

        String value = topicInfo.getBusinessDataSourceId()
                + "." + topicInfo.getSchemaName()
                + "." + topicInfo.getTableName();

        readModifyWrite(path, new DataModifier<String>() {
            @Override
            public String modify(String originalData) {
                return JsonUtil.sourceTableNameToJson(originalData, value);
            }
        });
    }

    /**
     * 合并 offset 指标。
     */
    public void mergeOffset(MonitorMetricEvent event) throws Exception {
        if (event == null) {
            return;
        }

        String path = serverInfo.getMonitorDataPath()
                + "/" + event.getServerId()
                + "/" + event.getTargetDataSourceId()
                + "/" + event.getTargetTableName()
                + "/offset";

        readModifyWrite(path, new DataModifier<String>() {
            @Override
            public String modify(String originalData) {
                return JsonUtil.offsetToJson(originalData, event.getTopic(), event.getValue());
            }
        });
    }

    /**
     * 合并 lag 指标。
     */
    public void mergeLag(String topic,
                         String targetDataSourceId,
                         String targetTableName,
                         long lag) throws Exception {

        String path = serverInfo.getMonitorDataPath()
                + "/" + targetDataSourceId
                + "/" + targetTableName
                + "/unconsumed_message_count";

        readModifyWrite(path, new DataModifier<String>() {
            @Override
            public String modify(String originalData) {
                return JsonUtil.unconsumedCountToJson(originalData, topic, lag);
            }
        });
    }

    /**
     * 通用 read-modify-write。
     */
    public <T> T readModifyWrite(String path, DataModifier<T> modifier) throws Exception {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("path cannot be blank");
        }
        if (modifier == null) {
            throw new IllegalArgumentException("modifier cannot be null");
        }

        CuratorFramework client = zkClient.rawClient();
        int maxRetries = 3;

        for (int i = 0; i < maxRetries; i++) {
            try {
                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    try {
                        zkClient.createIfAbsent(path, null, CreateMode.PERSISTENT);
                    } catch (Exception ignore) {
                    }
                    stat = client.checkExists().forPath(path);
                }

                byte[] currentBytes = client.getData().forPath(path);
                String currentRaw = null;
                if (currentBytes != null && currentBytes.length > 0) {
                    currentRaw = new String(currentBytes, StandardCharsets.UTF_8);
                }

                @SuppressWarnings("unchecked")
                T originalData = (T) currentRaw;

                T modifiedData = modifier.modify(originalData);

                if (modifiedData == null) {
                    return originalData;
                }

                byte[] bytes = String.valueOf(modifiedData).getBytes(StandardCharsets.UTF_8);

                client.setData()
                        .withVersion(stat.getVersion())
                        .forPath(path, bytes);

                return modifiedData;
            } catch (org.apache.zookeeper.KeeperException.BadVersionException e) {
                logger.info("BadVersion on readModifyWrite, retry={}, path={}", i + 1, path);

                if (i == maxRetries - 1) {
                    throw e;
                }

                Thread.sleep(50L + i * 30L);
            }
        }

        throw new IllegalStateException("readModifyWrite failed after retries, path=" + path);
    }

    /**
     * 释放当前 metric 服务持有的共享连接引用。
     */
    public void close() {
        if (sharedClient) {
            ZkClientRegistry.release(serverInfo.getZooKeeperConfig());
        }
    }

    public interface DataModifier<T> {
        T modify(T originalData);
    }
}