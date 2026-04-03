package com.bsoft.common.util;

/**
 * ClassName: ZooKeeperNodeManager
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/30 - 0:44
 */

import com.alibaba.fastjson.JSON;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.JobStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * ZooKeeper 节点管理器
 * 负责所有 ZooKeeper 节点的创建、更新、删除和查询操作
 *
 * @Author: snoopy
 * @Create: 2025/7/29
 */

public class ZooKeeperNodeManager {

    private static final Logger logger = LogManager.getLogger(ZooKeeperNodeManager.class);

    private ZooKeeperUtils zkUtils;

    private ClientInfo clientInfo;

    private ServerInfo serverInfo;

    // 客户端/服务端的job状态路径
    private String statusPath;


    // 客户端/服务端的job状态路径ip路径
    private String ipPath;

    //客户端 job信号路径
    private String stopSignalPath;


    //客户端 job信号路径
    private String clientScnPath;

    // 实例标识
    private final String managerInstanceId;


    public ZooKeeperNodeManager(ClientInfo clientInfo) throws Exception {
        this.managerInstanceId = "ClientManager-" + clientInfo.getClientId();

        zkUtils = new ZooKeeperUtils(clientInfo.getZooKeeperConfig());
        // 客户端/服务端的job状态路径
        statusPath = clientInfo.getClientStatusPath();
        // job信号路径
        stopSignalPath = clientInfo.getStopSignalPath();
        // 客户端/服务端的job状态路径ip路径
        ipPath = clientInfo.getClientIpPath();
        // zk中客户端当前scn路径
        clientScnPath = clientInfo.getClientScnPath();

        // 记录管理器实例
        logger.info("🏗️ 创建ZooKeeperNodeManager实例: {}", managerInstanceId);
        logger.info("📈 当前ZK连接总数: {}", ZooKeeperUtils.getCurrentConnectionCount());

        zkUtils.start();

        logger.info("ClientInfo 中的 ZooKeeper 已启动");
    }

    public ZooKeeperNodeManager(ServerInfo serverInfo) throws Exception {
        this.managerInstanceId = "ServerManager-" + serverInfo.getServerId();
        this.zkUtils = new ZooKeeperUtils(serverInfo.getZooKeeperConfig());
        zkUtils = new ZooKeeperUtils(serverInfo.getZooKeeperConfig());
        // job状态路径
        statusPath = serverInfo.getServerStatusPath();
        // job信号路径
        ipPath = serverInfo.getServerIpPath();
        // 记录管理器实例
        logger.info("🏗️ 创建ZooKeeperNodeManager实例: {}", managerInstanceId);
        logger.info("📈 当前ZK连接总数: {}", ZooKeeperUtils.getCurrentConnectionCount());
        zkUtils.start();

        logger.info("ZooKeeper 已启动");
    }

    // ========== 节点创建 ==========

    /**
     * 创建客户端节点结构
     */
    public void createClientNodes() throws Exception {

        zkUtils.createNode(statusPath, CreateMode.PERSISTENT);
        zkUtils.createNode(stopSignalPath, CreateMode.PERSISTENT);
        zkUtils.createNode(ipPath, CreateMode.PERSISTENT);
        zkUtils.createNode(clientScnPath, CreateMode.PERSISTENT);
    }


    //----------通用操作--------------------

    // 更新ip地址
    public void updateIpNode(String ip) throws Exception {
        zkUtils.updateNode(ipPath, JsonUtil.ipToJson(ip));
    }

    // 更新job状态
    public void updateStatusNode(JobStatus jobStatus) throws Exception {
        zkUtils.updateNode(statusPath, JsonUtil.jobStatusToJson(jobStatus));
    }

    // 更新job状态
    public void updateScnNode(String scn) throws Exception {
        zkUtils.updateNode(clientScnPath, JsonUtil.scnToJson(scn));
    }

    //----------通用操作--------------------

    /**
     * 发送作业停止信号
     */
    public void sendJobStopSignal(String reason) throws Exception {


        Map<String, Object> signal = new HashMap<>();
        signal.put("signal", "STOP");
        signal.put("source", reason);
        signal.put("timestamp", System.currentTimeMillis());

        zkUtils.updateNode(stopSignalPath, signal);
        logger.info("📡 已发送作业停止信号:  {}", reason);
    }

    /**
     * 创建作业信号节点
     */
    public void createJobSignalNodes(String jobId) throws Exception {
        zkUtils.createNode("/signals", CreateMode.PERSISTENT);
        zkUtils.createNode("/signals/jobs", CreateMode.PERSISTENT);
        zkUtils.createNode("/signals/jobs/" + jobId, CreateMode.PERSISTENT);
    }



    /**
     * 创建作业信号节点
     */
    public void createOrUpdateNode(String path, Object data,CreateMode createMode) throws Exception {
        zkUtils.createOrUpdateNode(path, data,CreateMode.PERSISTENT);

    }







    // ========== 节点数据操作 ==========

    /**
     * 设置节点数据
     */
    public void setNodeData(String path, Object data) throws Exception {
        String jsonData = JSON.toJSONString(data);
//        zkUtils.setData(path, jsonData.getBytes());
    }

    /**
     * 获取节点数据
     */
    public String getNodeData(String path) throws Exception {
        String data = zkUtils.getData(path);
        if (data != null) {
            return JSON.parseObject(data, String.class);
        }
        return null;
    }


    /**
     * 创建作业信号节点
     */
    public void clearNodeData(String path) throws Exception {
        zkUtils.clearNodeData(path);
    }



    // ========== 服务端操作 ==========


    /**
     * 创建客户端节点结构
     */
    /**
     * 创建服务器节点结构
     */
    public void createServerNodes(ServerInfo serverInfo) throws Exception {
        zkUtils.createNode(statusPath, CreateMode.PERSISTENT);
        zkUtils.createNode(ipPath, CreateMode.PERSISTENT);
    }

    /**
     * 读取节点数据，修改后写回 - 带并发控制
     *
     * @param path 节点路径
     * @param modifier 数据修改函数，返回null表示不需要更新
     * @param <T> 数据类型
     * @return 更新后的数据，如果没有更新则返回原数据
     */
    public <T> T readModifyWrite(String path, DataModifier<T> modifier) throws Exception {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                // 1. 检查节点是否存在，不存在则创建（移除synchronized）
                if (zkUtils.getClient().checkExists().forPath(path) == null) {
                    try {
                        // 创建节点及其父路径
                        zkUtils.createOrUpdateNode(path, null, CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {
                        // 节点已被其他线程创建，忽略异常继续执行
                        logger.debug("节点已存在，继续执行: {}", path);
                    }
                }

                // 2. 读取当前数据和版本
                byte[] currentData = zkUtils.getClient().getData().forPath(path);
                int version = zkUtils.getClient().checkExists().forPath(path).getVersion();

                T originalData = null;
                if (currentData != null && currentData.length > 0) {
                    String jsonData = new String(currentData, StandardCharsets.UTF_8);
                    originalData = (T) JSON.parseObject(jsonData, Object.class);
                }

                // 3. 应用修改函数
                T modifiedData = modifier.modify(originalData);

                // 4. 如果数据没有变化，直接返回
                if (modifiedData == null || modifiedData.equals(originalData)) {
                    logger.debug("数据无变化，跳过更新: {}", path);
                    return originalData;
                }

                // 5. 写回数据 - 使用版本控制防止并发冲突
                String jsonData = JSON.toJSONString(modifiedData);
                byte[] dataBytes = jsonData.getBytes(StandardCharsets.UTF_8);

                zkUtils.getClient().setData()
                        .withVersion(version)  // 关键：使用版本号防止并发覆盖
                        .forPath(path, dataBytes);

                logger.debug("读取-修改-写回成功: {} (重试次数: {})", path, retryCount);
                return modifiedData;

            } catch (KeeperException.BadVersionException e) {
                // 版本冲突，说明有并发修改，重试
                retryCount++;
                logger.info("版本冲突，重试读取-修改-写回: {} (重试次数: {})", path, retryCount);

                if (retryCount >= maxRetries) {
                    throw new Exception("读取-修改-写回失败，版本冲突次数过多: " + path);
                }

                // 短暂延迟后重试，避免竞争
                Thread.sleep(50 + retryCount * 30);

            } catch (KeeperException.NoNodeException e) {
                throw new Exception("节点不存在: " + path, e);
            }
        }

        throw new Exception("读取-修改-写回失败，已达最大重试次数: " + path);
    }
    // 在ZooKeeperNodeManager中使用
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

    // 更新SCN示例
    public void updateScnIfNewer(String scn) throws Exception {
        readModifyWrite(clientScnPath, (String currentScn) -> {
            if (currentScn == null || scn.compareTo(currentScn) > 0) {
                return scn; // 返回新的SCN
            }
            return null; // 返回null表示无需更新
        });
    }

    /**
     * 数据修改函数接口
     */
    @FunctionalInterface
    public interface DataModifier<T> {
        /**
         * 修改数据
         * @param original 原始数据
         * @return 修改后的数据，返回null表示无需更新
         */
        T modify(T original);
    }


    /**
     * 关闭资源
     */
    public void close() {
        if (zkUtils != null) {
            try {
                logger.info("🔧 关闭ZooKeeperNodeManager实例: {}", managerInstanceId);
                zkUtils.close();
                logger.info("📉 关闭后ZK连接总数: {}", ZooKeeperUtils.getCurrentConnectionCount());
                logger.debug("ZooKeeper 连接已关闭");
            } catch (Exception e) {
                logger.error("关闭 ZooKeeper 连接失败", e);
            }
        }
    }

    /**
     * 获取当前ZK连接数（代理方法）
     */
    public static int getCurrentConnectionCount() {
        return ZooKeeperUtils.getCurrentConnectionCount();
    }

    /**
     * 获取连接注册表（代理方法）
     */
    public static Map<String, Long> getConnectionRegistry() {
        return ZooKeeperUtils.getConnectionRegistry();
    }

    /**
     * 关闭连接监控器（代理方法）
     */
    public static void shutdownMonitor() {
        ZooKeeperUtils.shutdownMonitor();
    }

    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return zkUtils != null && zkUtils.isConnected();
    }

    /**
     * 重连ZK
     */
    public void reconnect() throws Exception {
        if (zkUtils != null) {
            zkUtils.close();
            // 重新创建 ZooKeeperUtils 实例
            if (clientInfo != null) {
                zkUtils = new ZooKeeperUtils(clientInfo.getZooKeeperConfig());
            } else if (serverInfo != null) {
                zkUtils = new ZooKeeperUtils(serverInfo.getZooKeeperConfig());
            }
            zkUtils.start();
        }
    }


}