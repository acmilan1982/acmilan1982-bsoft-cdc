package com.bsoft.common.zk;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 底层 ZooKeeper 客户端，负责把结构化对象序列化为 JSON 对象文本。
 */
public class ZkClient {

    private static final Logger logger = LogManager.getLogger(ZkClient.class);

    /**
     * 单个节点允许写入的最大字节数。
     *
     * 这里沿用原有设计，按 1MB 做保护。
     */
    private static final int MAX_NODE_BYTES = 1024 * 1024;

    /**
     * ZooKeeper 配置。
     */
    private final ZooKeeperConfig config;

    /**
     * 底层 Curator 客户端。
     *
     * start() 之后才会被初始化。
     */
    private CuratorFramework client;

    /**
     * 当前 ZK 客户端实例标识。
     *
     * 用于连接监控。
     */
    private final String instanceId;

    /**
     * 当前实例创建时间。
     *
     * 用于连接监控。
     */
    private final long createTimeMillis;

    public ZkClient(ZooKeeperConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("ZooKeeperConfig cannot be null");
        }

        this.config = config;
        this.instanceId = buildInstanceId();
        this.createTimeMillis = System.currentTimeMillis();
    }

    /**
     * 启动底层 Curator 连接。
     *
     * 说明：
     * 1. 重复调用是幂等的
     * 2. 连接超时会抛出异常
     */
    public synchronized void start() throws Exception {
        if (client != null) {
            return;
        }

        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(
                config.getRetryIntervalMs(),
                config.getRetryCount(),
                60000
        );

        this.client = CuratorFrameworkFactory.builder()
                .connectString(config.getConnectString())
                .sessionTimeoutMs(config.getSessionTimeoutMs())
                .connectionTimeoutMs(config.getConnectionTimeoutMs())
                .retryPolicy(retryPolicy)
                .namespace(normalizeNamespace(config.getBasePath()))
                .canBeReadOnly(false)
                .build();

        this.client.start();

        boolean connected = this.client.blockUntilConnected(60, TimeUnit.SECONDS);
        if (!connected) {
            closeSilently();
            throw new IllegalStateException("ZooKeeper connect timeout: " + config.getConnectString());
        }

        ZkConnectionMonitor.register(instanceId, createTimeMillis);

        logger.info("ZooKeeper connected, instanceId={}, connectString={}, basePath={}",
                instanceId,
                config.getConnectString(),
                config.getBasePath());
    }

    /**
     * 重连。
     *
     * 说明：
     * 1. 这里会先关闭旧连接
     * 2. 再重新创建并启动一个新连接
     */
    public synchronized void reconnect() throws Exception {
        closeSilently();
        this.client = null;
        start();
    }

    /**
     * 判断当前连接是否可用。
     */
    public boolean isConnected() {
        return client != null && client.getZookeeperClient().isConnected();
    }

    /**
     * 判断节点是否存在。
     */
    public boolean exists(String path) throws Exception {
        requireStarted();
        return client.checkExists().forPath(path) != null;
    }

    /**
     * 创建节点。
     *
     * 如果节点已存在，会抛出 NodeExistsException。
     */
    public void create(String path, Object data, CreateMode mode) throws Exception {
        requireStarted();

        byte[] bytes = toBytes(data);

        client.create()
                .creatingParentsIfNeeded()
                .withMode(mode)
                .forPath(path, bytes);
    }

    /**
     * 若节点不存在则创建，存在则忽略。
     */
    public void createIfAbsent(String path, Object data, CreateMode mode) throws Exception {
        requireStarted();

        try {
            create(path, data, mode);
        } catch (KeeperException.NodeExistsException ignore) {
            // 已存在时忽略。
        }
    }

    /**
     * 更新节点数据。
     *
     * 要求节点已存在。
     */
    public void set(String path, Object data) throws Exception {
        requireStarted();

        byte[] bytes = toBytes(data);
        client.setData().forPath(path, bytes);
    }

//    /**
//     * 若节点存在则更新，否则创建。
//     */
//    public void createOrSet(String path, Object data, CreateMode mode) throws Exception {
//        requireStarted();
//
//        if (exists(path)) {
//            set(path, data);
//        } else {
//            try {
//                create(path, data, mode);
//            } catch (KeeperException.NodeExistsException e) {
//                // 并发下可能别的线程刚创建完成，这时退化成更新。
//                set(path, data);
//            }
//        }
//    }

    /**
     * 读取节点原始字符串。
     *
     * 返回的是 UTF-8 文本，不做二次 JSON 解析。
     */
    public String getString(String path) throws Exception {
        requireStarted();

        try {
            byte[] bytes = client.getData().forPath(path);
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    /**
     * 按指定类型读取并解析对象。
     */
    public <T> T getObject(String path, Class<T> clazz) throws Exception {
        String json = getString(path);
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        return JSON.parseObject(json, clazz);
    }



    /**
     * 删除节点。
     *
     * @param recursive 是否递归删除子节点
     */
    public void delete(String path, boolean recursive) throws Exception {
        requireStarted();

        try {
            if (recursive) {
                deleteRecursively(path);
            } else {
                client.delete().forPath(path);
            }
        } catch (KeeperException.NoNodeException ignore) {
            // 节点不存在时忽略。
        }
    }

    /**
     * 获取子节点列表。
     *
     * 若父节点不存在，返回 null。
     */
    public List<String> children(String path) throws Exception {
        requireStarted();

        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        }
    }

    /**
     * 暴露底层 CuratorFramework。
     *
     * 仅当上层确实需要做低级操作时使用。
     */
    public CuratorFramework rawClient() {
        requireStarted();
        return client;
    }

    /**
     * 健康检查。
     *
     * 当前实现较保守：
     * 1. 连接必须处于 connected
     * 2. namespace 根路径必须可访问
     */
    public boolean healthCheck() {
        try {
            return isConnected() && exists("/");
        } catch (Exception e) {
            logger.warn("ZooKeeper health check failed, instanceId={}, message={}",
                    instanceId,
                    e.getMessage());
            return false;
        }
    }

    /**
     * 关闭当前连接。
     *
     * 关闭后会自动从连接监控器中注销。
     */
    public synchronized void close() {
        if (client != null) {
            try {
                client.close();
            } finally {
                client = null;
                ZkConnectionMonitor.unregister(instanceId);
            }
        }
    }

    /**
     * 递归删除节点。
     *
     * 这里保留一个简单递归实现，后续若路径规模大，可以再优化。
     */
    private void deleteRecursively(String path) throws Exception {
        List<String> children = children(path);
        if (children != null) {
            for (String child : children) {
                deleteRecursively(path + "/" + child);
            }
        }

        try {
            client.delete().forPath(path);
        } catch (KeeperException.NoNodeException ignore) {
            // 删除过程中节点被别人删掉时忽略。
        }
    }

    /**
     * 将对象统一序列化为 UTF-8 JSON 字节数组。
     */
    private byte[] toBytes(Object data) {
        String json = JSON.toJSONString(data);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        if (bytes.length > MAX_NODE_BYTES) {
            throw new IllegalArgumentException("ZooKeeper node data too large: " + bytes.length + " bytes");
        }

        return bytes;
    }

    /**
     * 确保客户端已经启动。
     */
    private void requireStarted() {
        if (client == null) {
            throw new IllegalStateException("ZkClient is not started");
        }
    }

    /**
     * 规范化 namespace。
     *
     * Curator 的 namespace 不能带前导斜杠。
     */
    private String normalizeNamespace(String basePath) {
        if (basePath == null || basePath.trim().isEmpty()) {
            return "";
        }
        return basePath.replaceFirst("^/+", "");
    }

    /**
     * 生成当前实例标识。
     */
    private String buildInstanceId() {
        return "zk-" + Thread.currentThread().getName() + "-" + (System.nanoTime() % 100000);
    }

    /**
     * 静默关闭。
     *
     * 主要用于 start 失败时的清理。
     */
    private void closeSilently() {
        try {
            close();
        } catch (Exception ignore) {
        }
    }


    //------------------新版方法---------------------------------------
//    /**
//     * 创建结构化 JSON 节点。
//     */
//    public void createJson(String path, Object data, CreateMode mode) throws Exception {
//        // 将对象序列化一次后写入节点。
//        createBytes(path, toJsonBytes(data), mode);
//    }



    /**
     * 节点不存在时创建 JSON 对象节点。
     */
    public void createJsonIfAbsent(String path, Object data, CreateMode mode) throws Exception {
        // 结构化节点禁止传入已经序列化的 JSON 字符串。
        createBytesIfAbsent(path, toJsonObjectBytes(data), mode);
    }



//    /**
//     * 更新结构化 JSON 节点。
//     */
//    public void setJson(String path, Object data) throws Exception {
//        // 将对象序列化一次后更新节点。
//        setBytes(path, toJsonBytes(data));
//    }
//
//    /**
//     * 更新普通文本节点。
//     */
//    public void setText(String path, String text) throws Exception {
//        // 将文本原样写入节点。
//        setBytes(path, toTextBytes(text));
//    }

    /**
     * 创建或更新 JSON 对象节点。
     */
    public void createOrSetJson(String path, Object data, CreateMode mode) throws Exception {
        // 结构化节点禁止传入已经序列化的 JSON 字符串。
        createOrSetBytes(path, toJsonObjectBytes(data), mode);
    }

    /**
     * 把结构化对象转换为 JSON 对象文本字节。
     */
    private byte[] toJsonObjectBytes(Object data) {
        // null 统一写成空 JSON 对象。
        Object value = data == null ? new LinkedHashMap<String, Object>() : data;

        // 禁止字符串进入结构化 JSON 写入入口，避免二次序列化。
        if (value instanceof CharSequence) {
            throw new IllegalArgumentException("ZK structured node data cannot be String");
        }

        // 序列化一次，得到 JSON 对象文本。
        String json = JSON.toJSONString(value);

        // 强制要求写入结果必须是 JSON 对象文本。
        if (!json.trim().startsWith("{") || !json.trim().endsWith("}")) {
            throw new IllegalArgumentException("ZK node data must be a JSON object: " + json);
        }

        // 使用 UTF-8 编码。
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        // 校验节点数据大小。
        validateNodeBytes(bytes);

        return bytes;
    }

    /**
     * 使用已经编码的字节创建节点。
     */
    private void createBytes(String path, byte[] bytes, CreateMode mode) throws Exception {
        // 确保底层客户端已经启动。
        requireStarted();

        // 检查节点数据大小。
        validateNodeBytes(bytes);

        // 创建节点并写入数据。
        client.create()
                .creatingParentsIfNeeded()
                .withMode(mode)
                .forPath(path, bytes);
    }

    /**
     * 节点不存在时使用已经编码的字节创建节点。
     */
    private void createBytesIfAbsent(String path, byte[] bytes, CreateMode mode) throws Exception {
        // 确保底层客户端已经启动。
        requireStarted();

        try {
            // 尝试创建节点。
            createBytes(path, bytes, mode);
        } catch (KeeperException.NodeExistsException ignore) {
            // 节点已存在时保持原值。
        }
    }

    /**
     * 使用已经编码的字节更新节点。
     */
    private void setBytes(String path, byte[] bytes) throws Exception {
        // 确保底层客户端已经启动。
        requireStarted();

        // 检查节点数据大小。
        validateNodeBytes(bytes);

        // 更新节点数据。
        client.setData().forPath(path, bytes);
    }

    /**
     * 使用已经编码的字节创建或更新节点。
     */
    private void createOrSetBytes(String path, byte[] bytes, CreateMode mode) throws Exception {
        // 确保底层客户端已经启动。
        requireStarted();

        if (exists(path)) {
            // 节点存在时直接更新。
            setBytes(path, bytes);
            return;
        }

        try {
            // 节点不存在时尝试创建。
            createBytes(path, bytes, mode);
        } catch (KeeperException.NodeExistsException e) {
            // 并发创建冲突时退化为更新。
            setBytes(path, bytes);
        }
    }



    /**
     * 校验节点数据大小。
     */
    private void validateNodeBytes(byte[] bytes) {
        // 防止写入超过 ZooKeeper 限制的数据。
        if (bytes.length > MAX_NODE_BYTES) {
            throw new IllegalArgumentException(
                    "ZooKeeper node data too large: " + bytes.length + " bytes"
            );
        }
    }

    /**
     * 创建 JSON 对象节点。
     */
    public void createJson(String path, Object data, CreateMode mode) throws Exception {
        // 创建节点时强制写入 JSON 对象文本。
        createBytes(path, toJsonObjectBytes(data), mode);
    }

    /**
     * 更新 JSON 对象节点。
     */
    public void setJson(String path, Object data) throws Exception {
        // 更新节点时强制写入 JSON 对象文本。
        setBytes(path, toJsonObjectBytes(data));
    }
}