package com.bsoft.common.zk.service;

import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.bsoft.common.zk.ZkClientRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * server 进程级 ZooKeeper 服务。
 *
 * 职责：
 * 1. 管理 server 进程级节点
 * 2. 管理 server 的 status / ip / monitor_data 根路径
 *
 * 只负责：
 * - /servers/{serverId}/status
 * - /servers/{serverId}/ip
 * - /servers/{serverId}/monitor_data
 *
 * 生命周期说明：
 * 1. 推荐通过 open(...) 打开
 * 2. open(...) 内部会复用 JVM 内共享的 ZkClient
 * 3. 使用结束后调用 close() 释放引用
 */
public class ServerProcessZkService {

    private static final Logger logger = LogManager.getLogger(ServerProcessZkService.class);

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

    private ServerProcessZkService(ServerInfo serverInfo, ZkClient zkClient, boolean sharedClient) {
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
     * 打开一个 server 进程级 ZK 服务。
     */
    public static ServerProcessZkService open(ServerInfo serverInfo) throws Exception {
        ZkClient zkClient = ZkClientRegistry.acquire(serverInfo.getZooKeeperConfig());
        return new ServerProcessZkService(serverInfo, zkClient, true);
    }

    /**
     * 使用外部提供的 ZkClient 创建 process 服务。
     */
    public static ServerProcessZkService using(ServerInfo serverInfo, ZkClient zkClient) {
        return new ServerProcessZkService(serverInfo, zkClient, false);
    }

    /**
     * 初始化 server 进程级节点结构。
     */
    public void initServerProcessNodes() throws Exception {
        // 结构节点也写 {}，保证节点内容始终是 JSON 对象文本。
        zkClient.createJsonIfAbsent(serverInfo.getServerStatusPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);
        zkClient.createJsonIfAbsent(serverInfo.getServerIpPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);
        zkClient.createJsonIfAbsent(serverInfo.getMonitorDataPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);
    }

    /**
     * 更新 server 进程状态节点。
     */
    public void updateStatus(JobStatus jobStatus) throws Exception {
        // JobStatus 转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkClient.createOrSetJson(
                serverInfo.getServerStatusPath(),
                JsonUtil.jobStatusToMap(jobStatus),
                CreateMode.PERSISTENT
        );
    }

    /**
     * 更新 server IP 节点。
     */
    public void updateIp() throws Exception {
        // IP 数据也统一构造成 Map，避免字符串 JSON 二次序列化。
        Map<String, String> data = JsonUtil.ipToMap(serverInfo.getIp() + ":" + serverInfo.getServerPort());

        // 只写结构化对象，由 ZkClient 序列化一次。
        zkClient.createOrSetJson(
                serverInfo.getServerIpPath(),
                data,
                CreateMode.PERSISTENT
        );
    }

    public String getMonitorDataPath() {
        return serverInfo.getMonitorDataPath();
    }

    public String getServerId() {
        return serverInfo.getServerId();
    }

    /**
     * 释放当前 process 服务持有的共享连接引用。
     */
    public void close() {
        if (sharedClient) {
            ZkClientRegistry.release(serverInfo.getZooKeeperConfig());
        }
    }
}