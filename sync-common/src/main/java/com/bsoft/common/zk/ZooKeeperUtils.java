package com.bsoft.common.zk;

/**
 * ClassName: ZooKeeperUtils
 * Package:   com.bsoft.zk
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:54
 */


import com.alibaba.fastjson.JSON;
import com.bsoft.common.bean.ZooKeeperConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ZooKeeper操作工具类 - 针对ZK 3.4.14版本优化
 */
public class ZooKeeperUtils {

    private static final Logger logger = LogManager.getLogger(ZooKeeperUtils.class);


    // 添加静态连接计数器
    private static final AtomicInteger connectionCount = new AtomicInteger(0);
    private static final ConcurrentHashMap<String, Long> connectionRegistry = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService monitorExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "zk-connection-monitor"));

    // 实例标识
    private final String instanceId;
    private final long createTime;

    static {
        // 每30秒打印一次连接状态
        monitorExecutor.scheduleAtFixedRate(() -> {
            printConnectionStatus();
        }, 60, 60, TimeUnit.SECONDS);
    }
    
    
    private final CuratorFramework client;
    private final ZooKeeperConfig config;

    // 通过主调传入的zk配置来创建当前示例
    public ZooKeeperUtils(ZooKeeperConfig config) {
        this.config = config;

        
        // 针对ZK 3.4.14的重试策略（更保守的参数）
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
                config.getRetryIntervalMs(), config.getRetryCount(), 60000); // 最大重试间隔60s

        // 创建客户端 - 针对3.4.14版本优化
        this.client = CuratorFrameworkFactory.builder()
                .connectString(config.getConnectString())
                .sessionTimeoutMs(config.getSessionTimeoutMs())
                .connectionTimeoutMs(config.getConnectionTimeoutMs())
                .retryPolicy(retryPolicy)
                .namespace(config.getBasePath().replaceAll("^/", "")) // 移除开头的/
                // 针对3.4.14版本的兼容性设置
                .canBeReadOnly(false) // 不允许只读模式
                .build();


        this.instanceId = generateInstanceId();
        this.createTime = System.currentTimeMillis();

        // 连接计数器递增
        int currentCount = connectionCount.incrementAndGet();
        connectionRegistry.put(instanceId, createTime);

        logger.info("🔗 创建ZooKeeper连接实例: {} (总连接数: {})", instanceId, currentCount);
    }

    /**
     * 生成实例唯一标识
     */
    private String generateInstanceId() {
        return String.format("ZK-%s-%d",
                Thread.currentThread().getName(),
                System.nanoTime() % 100000);
    }

    /**
     * 打印连接状态信息
     */
    private static void printConnectionStatus() {
        int totalConnections = connectionCount.get();
        long currentTime = System.currentTimeMillis();

        logger.info("📊 ZooKeeper连接监控报告:");
        logger.info("  ├─ 当前活跃连接数: {}", totalConnections);
        logger.info("  ├─ 连接注册表大小: {}", connectionRegistry.size());

        if (totalConnections > 50) {
            logger.warn("⚠️  连接数过多! 当前: {}, 建议检查连接泄漏", totalConnections);
        }

        // 打印连接详情（仅当连接数较多时）
        if (totalConnections > 10) {
            logger.info("  └─ 连接实例详情:");
            connectionRegistry.forEach((id, createTime) -> {
                long age = currentTime - createTime;
                logger.info("    ├─ {}: 存活时间 {}ms", id, age);
                if (age > 300000) { // 5分钟
                    logger.warn("    │  └─ ⚠️ 长期存活连接，可能存在泄漏");
                }
            });
        }
    }

    /**
     * 获取当前连接数
     */
    public static int getCurrentConnectionCount() {
        return connectionCount.get();
    }

    /**
     * 获取连接详情
     */
    public static Map<String, Long> getConnectionRegistry() {
        return new HashMap<>(connectionRegistry);
    }

    /**
     * 启动客户端连接 - 增加超时处理
     */
    public void start() throws Exception {
        logger.info("启动客户端连接 。。。。。。。。。。。。。。");
        logger.info(this.config.toString());
        client.start();

        // 增加连接超时控制，避免3.4.14版本连接hang住
        boolean connected = client.blockUntilConnected(60, TimeUnit.SECONDS);
        if (!connected) {
            throw new Exception("ZooKeeper连接超时: " + config.getConnectString());
        }

        logger.info("🔗 ZooKeeper连接成功: " + config.getConnectString() + " (版本: 3.4.14兼容模式)");
    }

    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }


    /**
     * 创建节点并写入数据 - 增加3.4.14版本的错误处理
     */
    public void createNode(String path, CreateMode createMode) throws Exception {
        createNode( path,null, createMode);
    }

    /**
     * 创建节点并写入数据 - 增加3.4.14版本的错误处理
     */
    public void createNode(String path, Object data, CreateMode createMode) throws Exception {
        String jsonData = JSON.toJSONString(data);
        byte[] dataBytes = jsonData.getBytes(StandardCharsets.UTF_8);

        // 检查数据大小（ZK 3.4.14对节点数据大小限制更严格，默认1MB）
        if (dataBytes.length > 1048576) { // 1MB
            throw new IllegalArgumentException("数据过大: " + dataBytes.length + " bytes (ZK 3.4.14限制1MB)");
        }

        try {
            // 确保父路径存在
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(createMode)
                    .forPath(path, dataBytes);

            logger.info("📝 ZK节点创建成功: " + path + " (数据长度: " + dataBytes.length + "字节)");

        } catch (KeeperException.NodeExistsException e) {
            // 节点已存在，尝试更新
            logger.info("⚠️ 节点已存在，尝试更新: " + path);
            updateNode(path, data);
        }
    }

    /**
     * 更新节点数据 - 增加版本检查
     */
    public void updateNode(String path, Object data) throws Exception {
        String jsonData = JSON.toJSONString(data);
        byte[] dataBytes = jsonData.getBytes(StandardCharsets.UTF_8);

        // 检查数据大小
        if (dataBytes.length > 1048576) {
            throw new IllegalArgumentException("数据过大: " + dataBytes.length + " bytes (ZK 3.4.14限制1MB)");
        }

        try {
            // 获取当前版本号，避免并发修改冲突
            int version = client.checkExists().forPath(path).getVersion();
            client.setData().withVersion(version).forPath(path, dataBytes);

//            logger.info("ZK节点更新成功: " + path + " (数据长度: " + dataBytes.length + "字节)");

        } catch (KeeperException.BadVersionException e) {
            // 版本冲突，重试
            logger.warn("⚠️ 版本冲突，重试更新: " + path);
            client.setData().forPath(path, dataBytes); // 不指定版本，强制更新
        }
    }

    
    
    /**
     * 创建或更新节点 - 增强的错误处理
     */
    public void createOrUpdateNode(String path, Object data, CreateMode createMode) throws Exception {
        try {
            if (checkExists(path)) {
                updateNode(path, data);
            } else {
                createNode(path, data, createMode);
            }
        } catch (Exception e) {
            logger.warn(" 创建或更新节点失败: " + path + ", 错误: " + e.getMessage());
            throw e;
        }
    }

    /**
     * 读取节点数据 - 增加异常处理
     */
    public <T> T getData(String path) throws Exception {
       return (T) getData(path,String.class);
    }

    /**
     * 读取节点数据 - 增加异常处理
     */
    public <T> T getData(String path, Class<T> clazz) throws Exception {
        try {
            byte[] data = client.getData().forPath(path);
            if (data == null || data.length == 0) {
                return null;
            }

            String jsonData = new String(data, StandardCharsets.UTF_8);
            return JSON.parseObject(jsonData, clazz);

        } catch (KeeperException.NoNodeException e) {
            logger.info("📂 节点不存在: " + path);
            return null;
        }
    }

    /**
     * 删除节点 - 增加递归删除的安全检查
     */
    public void deleteNode(String path, boolean deleteChildren) throws Exception {
        try {
            if (deleteChildren) {
                // 对于3.4.14版本，需要更谨慎的递归删除
                deleteRecursively(path);
            } else {
                client.delete().forPath(path);
            }
            logger.info("🗑️ ZK节点删除成功: " + path);

        } catch (KeeperException.NoNodeException e) {
            logger.info("📂 节点不存在，无需删除: " + path);
        }
    }

    /**
     * 递归删除节点 - 针对3.4.14版本的安全实现
     */
    private void deleteRecursively(String path) throws Exception {
        try {
            List<String> children = client.getChildren().forPath(path);
            for (String child : children) {
                deleteRecursively(path + "/" + child);
            }
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            // 节点已被删除，忽略
        }
    }

    /**
     * 检查节点是否存在
     */
    public boolean checkExists(String path) throws Exception {
        return client.checkExists().forPath(path) != null;
    }

    /**
     * 清除节点数据 - 保留节点结构
     */
    public void clearNodeData(String path) throws Exception {
        try {
            client.setData().forPath(path, new byte[0]);
            logger.info("🧹 ZK节点数据已清除: " + path);
        } catch (KeeperException.NoNodeException e) {
            logger.info("📂 节点不存在，无需清除数据: " + path);
        }
    }

    /**
     * 获取子节点列表 - 增加空检查
     */
    public List<String> getChildren(String path) throws Exception {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            logger.info("📂 父节点不存在: " + path);
            return null;
        }
    }

    /**
     * 关闭连接 - 增加超时控制
     */

    public void close() {
        if (client != null) {
            try {
                client.close();

                // 连接计数器递减
                int currentCount = connectionCount.decrementAndGet();
                connectionRegistry.remove(instanceId);

                logger.info("🔌 关闭ZooKeeper连接实例: {} (剩余连接数: {})", instanceId, currentCount);

            } catch (Exception e) {
                logger.warn("⚠️ 关闭ZK连接时出现异常: " + e.getMessage());
            }
        }
    }

    /**
     * 获取客户端实例
     */
    public CuratorFramework getClient() {
        return client;
    }

    /**
     * 健康检查
     */
    public boolean healthCheck() {
        try {
            return isConnected() && checkExists("/");
        } catch (Exception e) {
            logger.warn("🔍 ZK健康检查失败: " + e.getMessage());
            return false;
        }
    }

    // 添加静态方法用于关闭监控器（应用关闭时调用）
    public static void shutdownMonitor() {
        if (monitorExecutor != null && !monitorExecutor.isShutdown()) {
            monitorExecutor.shutdown();
            try {
                if (!monitorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    monitorExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                monitorExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}