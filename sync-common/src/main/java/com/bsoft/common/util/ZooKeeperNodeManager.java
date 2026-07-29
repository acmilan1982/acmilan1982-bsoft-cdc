package com.bsoft.common.util;

import com.alibaba.fastjson.JSON;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.zk.ZooKeeperUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * ZooKeeper 节点管理器
 *
 * 职责：
 * 1. 统一封装客户端/服务端在 ZooKeeper 上的节点创建、更新、查询、清理操作
 * 2. 支持客户端“多数据源、多 job”模型
 * 3. 支持服务端节点管理
 *
 * 当前版本设计原则：
 * 1. 不再兼容旧版单 job 客户端路径
 * 2. 客户端所有运行状态按 dataSourceId 维度隔离
 * 3. sync-common 不能依赖 sync-client，因此 job 级路径由上层计算后传入
 *
 * 客户端路径模型：
 * 1. 客户端级路径
 *    - /clients/{clientId}/status
 *    - /clients/{clientId}/ip
 *    - /clients/{clientId}/jobs
 *
 * 2. job 级路径
 *    - /clients/{clientId}/jobs/{dataSourceId}/status
 *    - /clients/{clientId}/jobs/{dataSourceId}/signals
 *    - /clients/{clientId}/jobs/{dataSourceId}/scn
 */
public class ZooKeeperNodeManager {

    private static final Logger logger = LogManager.getLogger(ZooKeeperNodeManager.class);

    /**
     * 底层 ZK 工具
     */
    private ZooKeeperUtils zkUtils;

    /**
     * 客户端配置，仅客户端模式有效
     */
    private ClientInfo clientInfo;

    /**
     * 服务端配置，仅服务端模式有效
     */
    private ServerInfo serverInfo;

    /**
     * 当前 manager 绑定的状态路径
     *
     * 客户端 job 级路径：
     * /clients/{clientId}/jobs/{dataSourceId}/status
     *
     * 服务端路径：
     * /servers/{serverId}/status
     */
    private String statusPath;

    /**
     * 当前 manager 绑定的 IP 路径
     *
     * 客户端级：
     * /clients/{clientId}/ip
     *
     * 服务端级：
     * /servers/{serverId}/ip
     */
    private String ipPath;

    /**
     * 当前 manager 绑定的停止信号路径
     *
     * 客户端 job 级路径：
     * /clients/{clientId}/jobs/{dataSourceId}/signals
     *
     * 服务端通常不使用该字段
     */
    private String stopSignalPath;

    /**
     * 当前 manager 绑定的 SCN 路径
     *
     * 客户端 job 级路径：
     * /clients/{clientId}/jobs/{dataSourceId}/scn
     *
     * 服务端通常不使用该字段
     */
    private String clientScnPath;

    /**
     * 仅用于日志标识当前 manager 实例
     */
    private final String managerInstanceId;

    // -------------------------------------------------------------------------
    // 构造器：客户端多 job 模式
    // -------------------------------------------------------------------------

    /**
     * 客户端多 job 模式构造器
     *
     * 用法：
     * 1. 上层已经根据 dataSourceId 算好了 job 级路径
     * 2. 这里直接绑定这些路径
     *
     * @param clientInfo 客户端配置
     * @param dataSourceId 当前业务库 ID，仅用于日志标识
     */
    public ZooKeeperNodeManager(ClientInfo clientInfo, String dataSourceId) throws Exception {
        this.clientInfo = clientInfo;
        this.managerInstanceId = "ClientManager-" + clientInfo.getClientId() + "-" + dataSourceId;

        this.zkUtils = new ZooKeeperUtils(clientInfo.getZooKeeperConfig());

        this.statusPath = this.clientInfo.getJobStatusPath(dataSourceId);
        this.stopSignalPath = this.clientInfo.getJobStopSignalPath(dataSourceId);;
        this.ipPath = this.clientInfo.getClientIpPath();
//        this.clientScnPath = scnPath;

        logCreateManager();
        zkUtils.start();
        logger.info("ZooKeeperNodeManager started in client multi-job mode, dataSourceId={}", dataSourceId);
    }

    // -------------------------------------------------------------------------
    // 构造器：服务端模式
    // -------------------------------------------------------------------------

    /**
     * 服务端构造器
     */
    public ZooKeeperNodeManager(ServerInfo serverInfo) throws Exception {
        this.serverInfo = serverInfo;
        this.managerInstanceId = "ServerManager-" + serverInfo.getServerId();

        this.zkUtils = new ZooKeeperUtils(serverInfo.getZooKeeperConfig());

        this.statusPath = serverInfo.getServerStatusPath();
        this.ipPath = serverInfo.getServerIpPath();

        logCreateManager();
        zkUtils.start();
        logger.info("ZooKeeperNodeManager started in server mode");
    }

    /**
     * 统一打印 manager 创建日志
     */
    private void logCreateManager() {
        logger.info("Creating ZooKeeperNodeManager: {}", managerInstanceId);
        logger.info("Current ZooKeeper connection count: {}", ZooKeeperUtils.getCurrentConnectionCount());
    }

    // -------------------------------------------------------------------------
    // 节点创建
    // -------------------------------------------------------------------------

    /**
     * 创建客户端多 job 模式节点结构
     *
     * 需要创建：
     * 1. 客户端级 jobs 根路径
     * 2. 当前 job 自己的 status / signal / scn 节点
     * 3. 客户端级 ip 节点
     *
     * 说明：
     * 1. ip 节点是客户端级，不是 job 级
     * 2. 当前方法适合在每个业务库 job 启动前调用一次
     *
     * @param jobsRootPath 例如 /clients/{clientId}/jobs
     */
    public void createClientJobNodes(String jobsRootPath) throws Exception {
        requireClientMode();

        // 客户端级 jobs 根路径
        zkUtils.createNode(jobsRootPath, CreateMode.PERSISTENT);

        // 当前 job 的状态节点
        zkUtils.createNode(statusPath, CreateMode.PERSISTENT);

        // 当前 job 的停止信号节点
        zkUtils.createNode(stopSignalPath, CreateMode.PERSISTENT);

        // 当前 job 的 SCN 节点
        zkUtils.createNode(clientScnPath, CreateMode.PERSISTENT);

        // 客户端 IP 节点
        zkUtils.createNode(ipPath, CreateMode.PERSISTENT);
    }

    /**
     * 创建服务端节点结构
     */
    public void createServerNodes(ServerInfo serverInfo) throws Exception {
        zkUtils.createNode(statusPath, CreateMode.PERSISTENT);
        zkUtils.createNode(ipPath, CreateMode.PERSISTENT);
    }

    // -------------------------------------------------------------------------
    // 节点更新
    // -------------------------------------------------------------------------

    /**
     * 更新 IP 节点。
     */
    public void updateIpNode(String ip) throws Exception {
        // IP 转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkUtils.updateNode(ipPath, JsonUtil.ipToMap(ip));
    }

    /**
     * 更新状态节点。
     */
    public void updateStatusNode(JobStatus jobStatus) throws Exception {
        // 状态转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkUtils.updateNode(statusPath, JsonUtil.jobStatusToMap(jobStatus));
    }

    /**
     * 更新 SCN 节点。
     */
    public void updateScnNode(String scn) throws Exception {
        // 确保当前实例具备 SCN 路径。
        requireScnPath();

        // SCN 转 Map 后写入，确保 ZK 中是 JSON 对象文本。
        zkUtils.updateNode(clientScnPath, JsonUtil.scnToMap(scn));
    }

    /**
     * 发送作业停止信号
     *
     * 写入结构：
     * {
     *   "signal": "STOP",
     *   "source": "...",
     *   "timestamp": ...
     * }
     */
    public void sendJobStopSignal(String reason) throws Exception {
        requireStopSignalPath();

        Map<String, Object> signal = new HashMap<>();
        signal.put("signal", "STOP");
        signal.put("source", reason);
        signal.put("timestamp", System.currentTimeMillis());

        zkUtils.updateNode(stopSignalPath, signal);
        logger.info("Sent job stop signal, path={}, reason={}", stopSignalPath, reason);
    }

    // -------------------------------------------------------------------------
    // 节点读写
    // -------------------------------------------------------------------------

    /**
     * 通用 createOrUpdate
     *
     * @param path 节点路径
     * @param data 节点数据
     * @param createMode 创建模式
     */
    public void createOrUpdateNode(String path, Object data, CreateMode createMode) throws Exception {
        zkUtils.createOrUpdateNode(path, data, createMode);
    }

    /**
     * 获取节点数据
     *
     * 注意：
     * 当前保持与历史代码一致，统一返回 String。
     */
    public String getNodeData(String path) throws Exception {
        String data = zkUtils.getData(path);
        if (data != null) {
            return JSON.parseObject(data, String.class);
        }
        return null;
    }

    /**
     * 清空指定节点数据
     */
    public void clearNodeData(String path) throws Exception {
        zkUtils.clearNodeData(path);
    }

    /**
     * 清理当前 job 的 signal 与 scn
     *
     * 用途：
     * 1. job 启动前清理旧状态
     * 2. job 重启前避免使用旧 stop signal / scn
     */
    public void clearJobSignalAndScn() throws Exception {
        requireStopSignalPath();
        requireScnPath();

        zkUtils.clearNodeData(stopSignalPath);
        zkUtils.clearNodeData(clientScnPath);
    }

    // -------------------------------------------------------------------------
    // read-modify-write
    // -------------------------------------------------------------------------

    /**
     * 读取节点数据，修改后写回
     *
     * 特点：
     * 1. 节点不存在时自动创建
     * 2. 使用版本号避免并发覆盖
     * 3. 遇到版本冲突自动重试
     */
    public <T> T readModifyWrite(String path, DataModifier<T> modifier) throws Exception {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                // 1. 如果节点不存在，先创建
                if (zkUtils.getClient().checkExists().forPath(path) == null) {
                    try {
                        zkUtils.createOrUpdateNode(path, null, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        logger.debug("Node already created by another thread: {}", path);
                    }
                }

                // 2. 读取当前值与版本号
                byte[] currentData = zkUtils.getClient().getData().forPath(path);
                int version = zkUtils.getClient().checkExists().forPath(path).getVersion();

                T originalData = null;
                if (currentData != null && currentData.length > 0) {
                    String jsonData = new String(currentData, StandardCharsets.UTF_8);
                    originalData = (T) JSON.parseObject(jsonData, Object.class);
                }

                // 3. 调用修改器
                T modifiedData = modifier.modify(originalData);

                // 4. 无变化则直接返回
                if (modifiedData == null || modifiedData.equals(originalData)) {
                    return originalData;
                }

                // 5. 带版本写回
                String jsonData = JSON.toJSONString(modifiedData);
                byte[] dataBytes = jsonData.getBytes(StandardCharsets.UTF_8);

                zkUtils.getClient().setData()
                        .withVersion(version)
                        .forPath(path, dataBytes);

                return modifiedData;
            } catch (KeeperException.BadVersionException e) {
                retryCount++;

                logger.info("BadVersion on readModifyWrite, retry={}, path={}", retryCount, path);

                if (retryCount >= maxRetries) {
                    throw new Exception("readModifyWrite failed after retries, path=" + path, e);
                }

                Thread.sleep(50 + retryCount * 30L);
            } catch (KeeperException.NoNodeException e) {
                throw new Exception("node not exists, path=" + path, e);
            }
        }

        throw new Exception("readModifyWrite failed, path=" + path);
    }

    /**
     * 计数器加一示例
     */
    public void incrementCounter(String path) throws Exception {
        readModifyWrite(path, (Map<String, Object> data) -> {
            if (data == null) {
                data = new HashMap<>();
            }

            Integer count = (Integer) data.getOrDefault("count", 0);
            data.put("count", count + 1);
            data.put("lastUpdate", System.currentTimeMillis());

            return data;
        });
    }

    /**
     * 仅当新 SCN 更大时才更新
     *
     * 注意：
     * 该方法依赖当前 manager 已绑定当前 job 的 SCN 路径。
     */
    public void updateScnIfNewer(String scn) throws Exception {
        requireScnPath();

        readModifyWrite(clientScnPath, (String currentScn) -> {
            if (currentScn == null || scn.compareTo(currentScn) > 0) {
                return scn;
            }
            return null;
        });
    }

    @FunctionalInterface
    public interface DataModifier<T> {
        T modify(T original);
    }

    // -------------------------------------------------------------------------
    // 连接状态 / 生命周期
    // -------------------------------------------------------------------------

    /**
     * 检查当前 ZK 连接是否可用
     */
    public boolean isConnected() {
        return zkUtils != null && zkUtils.isConnected();
    }

    /**
     * 重新连接 ZK
     *
     * 注意：
     * 1. 这里只重建 ZooKeeperUtils 连接
     * 2. 不自动重建节点结构
     * 3. 节点结构是否需要重建，由上层决定
     */
    public void reconnect() throws Exception {
        if (zkUtils != null) {
            zkUtils.close();

            if (clientInfo != null) {
                zkUtils = new ZooKeeperUtils(clientInfo.getZooKeeperConfig());
            } else if (serverInfo != null) {
                zkUtils = new ZooKeeperUtils(serverInfo.getZooKeeperConfig());
            } else {
                throw new IllegalStateException("Neither clientInfo nor serverInfo is set");
            }

            zkUtils.start();
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        if (zkUtils != null) {
            try {
                logger.info("Closing ZooKeeperNodeManager: {}", managerInstanceId);
                zkUtils.close();
                logger.info("ZooKeeper connection count after close: {}", ZooKeeperUtils.getCurrentConnectionCount());
            } catch (Exception e) {
                logger.error("Close ZooKeeperNodeManager failed: {}", managerInstanceId, e);
            }
        }
    }

    // -------------------------------------------------------------------------
    // 静态代理方法
    // -------------------------------------------------------------------------

    /**
     * 获取当前 ZK 连接数
     */
    public static int getCurrentConnectionCount() {
        return ZooKeeperUtils.getCurrentConnectionCount();
    }

    /**
     * 获取连接注册表
     */
    public static Map<String, Long> getConnectionRegistry() {
        return ZooKeeperUtils.getConnectionRegistry();
    }

    /**
     * 关闭连接监控器
     */
    public static void shutdownMonitor() {
        ZooKeeperUtils.shutdownMonitor();
    }

    // -------------------------------------------------------------------------
    // 参数校验
    // -------------------------------------------------------------------------

    /**
     * 校验当前是客户端模式
     */
    private void requireClientMode() {
        if (clientInfo == null) {
            throw new IllegalStateException("Current ZooKeeperNodeManager is not in client mode");
        }
    }

    /**
     * 校验 stopSignalPath 已初始化
     */
    private void requireStopSignalPath() {
        if (stopSignalPath == null || stopSignalPath.trim().isEmpty()) {
            throw new IllegalStateException("stopSignalPath is not initialized");
        }
    }

    /**
     * 校验 scnPath 已初始化
     */
    private void requireScnPath() {
        if (clientScnPath == null || clientScnPath.trim().isEmpty()) {
            throw new IllegalStateException("clientScnPath is not initialized");
        }
    }
}