package com.bsoft.common.global;

import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.config.properties.BaseInfo;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.constants.MetricKeys;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.MetricType;
import com.bsoft.common.metric.MonitorMetricEvent;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.util.ZooKeeperNodeManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 全局 ZooKeeper 管理器
 *
 * 设计目标：
 * 1. 作为“进程级”的全局 ZK 管理器使用
 * 2. 客户端只用它维护客户端级节点：
 *    - /clients/{clientId}/status
 *    - /clients/{clientId}/ip
 * 3. 服务端继续用它维护监控指标批量提交能力
 * 4. 不再承担客户端多业务库 job 的 stopSignal / scn / jobStatus 管理
 *
 * 重要说明：
 * 1. 客户端 job 级路径现在应由 ZooKeeperNodeManager 直接管理：
 *    - /clients/{clientId}/jobs/{dataSourceId}/status
 *    - /clients/{clientId}/jobs/{dataSourceId}/signals
 *    - /clients/{clientId}/jobs/{dataSourceId}/scn
 * 2. 这里保留的是“客户端进程级状态”和“服务端监控指标批量提交”
 *
 * 单例策略：
 * 1. 同一 JVM 内仅维护一个实例
 * 2. 一个进程只应以一种身份初始化：
 *    - ClientInfo
 *    - ServerInfo
 *    - PipeLineInfo（如果未来需要）
 */
public class GlobalZooKeeperManager {

    private static final Logger logger = LogManager.getLogger(GlobalZooKeeperManager.class);

    private static volatile GlobalZooKeeperManager instance;
    private static final Object LOCK = new Object();

    /**
     * 底层节点管理器
     *
     * 客户端模式下：
     * - 绑定客户端级 status / ip
     *
     * 服务端模式下：
     * - 绑定服务端级 status / ip
     */
    private final ZooKeeperNodeManager zkManager;

    /**
     * 定时任务线程池
     *
     * 用途：
     * 1. 服务端批量提交监控指标
     * 2. 定时监测 ZK 连接健康
     */
    private final ScheduledExecutorService scheduler;

    /**
     * 当前实例是否作为服务端角色运行
     */
    private final boolean serverMode;

    /**
     * 当前实例是否作为客户端角色运行
     */
    private final boolean clientMode;

    /**
     * 当前客户端配置
     */
    private ClientInfo clientInfo;

    /**
     * 当前服务端配置
     */
    private ServerInfo serverInfo;

    // -------------------------------------------------------------------------
    // 服务端监控指标缓存
    // -------------------------------------------------------------------------

    /**
     * 服务端 offset 指标当前缓冲区
     */
    private volatile ConcurrentHashMap<MetricKey, Long> serverOffsetCurrentMetrics;

    /**
     * 服务端 offset 指标待提交缓冲区
     */
    private volatile ConcurrentHashMap<MetricKey, Long> serverOffsetCommitMetrics;

    /**
     * offset 双缓冲交换锁
     */
    private final Object serverOffsetSwapLock = new Object();

    /**
     * 服务端 lag 指标当前缓冲区
     */
    private volatile ConcurrentHashMap<MetricKey, Long> serverLagCurrentMetrics;

    /**
     * 服务端 lag 指标待提交缓冲区
     */
    private volatile ConcurrentHashMap<MetricKey, Long> serverLagCommitMetrics;

    /**
     * lag 双缓冲交换锁
     */
    private final Object serverLagSwapLock = new Object();

    // -------------------------------------------------------------------------
    // 构造器：客户端模式
    // -------------------------------------------------------------------------

    /**
     * 客户端模式构造器
     *
     * 说明：
     * 1. 这里只负责客户端级 status / ip
     * 2. 不再负责 job 级 signal / scn / status
     */
    private GlobalZooKeeperManager(ClientInfo clientInfo) throws Exception {
        this.clientInfo = clientInfo;
        this.serverInfo = null;
        this.serverMode = false;
        this.clientMode = true;

        this.zkManager = new ZooKeeperNodeManager(clientInfo, "client-process");

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "zk-global-manager-client")
        );

        // 客户端模式下只保留连接健康检查
        scheduler.scheduleAtFixedRate(this::monitorZkConnections, 60, 60, TimeUnit.SECONDS);

        logger.info("GlobalZooKeeperManager initialized in client mode, clientId={}", clientInfo.getClientId());
    }

    // -------------------------------------------------------------------------
    // 构造器：服务端模式
    // -------------------------------------------------------------------------

    /**
     * 服务端模式构造器
     *
     * 说明：
     * 1. 保留服务端 offset / lag 指标批量提交
     * 2. 保留服务端级 status / ip 管理
     */
    private GlobalZooKeeperManager(ServerInfo serverInfo) throws Exception {
        this.serverInfo = serverInfo;
        this.clientInfo = null;
        this.serverMode = true;
        this.clientMode = false;

        this.zkManager = new ZooKeeperNodeManager(serverInfo);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "zk-global-manager-server")
        );

        this.serverOffsetCurrentMetrics = new ConcurrentHashMap<>();
        this.serverOffsetCommitMetrics = new ConcurrentHashMap<>();
        this.serverLagCurrentMetrics = new ConcurrentHashMap<>();
        this.serverLagCommitMetrics = new ConcurrentHashMap<>();

        // 定时监测连接
        scheduler.scheduleAtFixedRate(this::monitorZkConnections, 60, 60, TimeUnit.SECONDS);

        logger.info("GlobalZooKeeperManager initialized in server mode, serverId={}", serverInfo.getServerId());
    }

    // -------------------------------------------------------------------------
    // 单例工厂
    // -------------------------------------------------------------------------

    /**
     * 获取全局单例
     *
     * 说明：
     * 1. 同一 JVM 内只允许初始化一次
     * 2. 初始化身份必须一致
     */
    public static GlobalZooKeeperManager getInstance(BaseInfo baseInfo) throws Exception {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    if (baseInfo instanceof ServerInfo) {
                        instance = new GlobalZooKeeperManager((ServerInfo) baseInfo);
                    } else if (baseInfo instanceof ClientInfo) {
                        instance = new GlobalZooKeeperManager((ClientInfo) baseInfo);
                    } else if (baseInfo instanceof PipeLineInfo) {
                        throw new IllegalArgumentException("PipeLineInfo mode is not implemented yet");
                    } else {
                        throw new IllegalArgumentException("Unsupported BaseInfo type: " + baseInfo.getClass());
                    }
                }
            }
        }
        return instance;
    }

    // -------------------------------------------------------------------------
    // 客户端 / 服务端初始化
    // -------------------------------------------------------------------------

    /**
     * 初始化客户端级 ZK 节点
     *
     * 当前版本客户端只维护：
     * 1. /clients/{clientId}/status
     * 2. /clients/{clientId}/ip
     *
     * 注意：
     * 1. 客户端 job 级节点不在这里初始化
     * 2. job 级节点由上层按 dataSourceId 自己创建
     */
    public void initClientZKNode() throws Exception {
        requireClientMode();

        // 直接创建客户端级 status / ip
        zkManager.createOrUpdateNode(clientInfo.getClientStatusPath(), null, CreateMode.PERSISTENT);
        zkManager.createOrUpdateNode(clientInfo.getClientIpPath(), null, CreateMode.PERSISTENT);

        // 写入 IP
        zkManager.updateIpNode(clientInfo.getIp() + ":" + clientInfo.getServerPort());
    }

    /**
     * 初始化服务端级 ZK 节点
     */
    public void initServerZKNode() throws Exception {
        requireServerMode();
        zkManager.createServerNodes(serverInfo);
        zkManager.updateIpNode(serverInfo.getIp() + ":" + serverInfo.getServerPort());
    }

    /**
     * 更新进程级状态节点
     *
     * 用途：
     * 1. 客户端进程级状态
     * 2. 服务端进程级状态
     *
     * 注意：
     * 这不是 dataSourceId 级别的 job 状态。
     */
    public void updateStatusNode(JobStatus jobStatus) throws Exception {
        zkManager.updateStatusNode(jobStatus);
    }

    /**
     * 获取指定节点数据
     */
    public String getNodeData(String path) throws Exception {
        return zkManager.getNodeData(path);
    }

    // -------------------------------------------------------------------------
    // 服务端指标更新
    // -------------------------------------------------------------------------

    /**
     * 更新 offset 指标
     *
     * 仅服务端模式有效。
     */
    public void updateOffset(MonitorMetricEvent monitorMetricEvent) {
        requireServerModeSilently();

        String serverId = monitorMetricEvent.getServerId();
        String topic = monitorMetricEvent.getTopic();
        String targetDataSourceId = monitorMetricEvent.getTargetDataSourceId();
        String targetTableName = monitorMetricEvent.getTargetTableName();
        long offset = monitorMetricEvent.getValue();

        MetricKey key = new MetricKey(serverId, topic, targetDataSourceId, targetTableName, MetricType.OFFSET.code());

        serverOffsetCurrentMetrics.compute(key, (k, currentValue) ->
                currentValue == null ? offset : Math.max(currentValue, offset));
    }

    /**
     * 更新 lag 指标
     *
     * 仅服务端模式有效。
     */
    public void updateUnconsumedMessageCount(Map<String, String> metric) {
        requireServerModeSilently();

        String topic = metric.get(MetricKeys.TOPIC);
        String targetDataSourceId = metric.get(MetricKeys.TARGET_DATA_SOURCE_ID);
        String targetTableName = metric.get(MetricKeys.TARGET_TABLE_NAME);
        long lag = Long.parseLong(metric.get(MetricKeys.UNCONSUMED_MESSAGE_COUNT));

        MetricKey key = new MetricKey(
                serverInfo.getServerId(),
                topic,
                targetDataSourceId,
                targetTableName,
                MetricType.UNCONSUMED_MESSAGE_COUNT.code()
        );

        // lag 可以下降，因此直接覆盖
        serverLagCurrentMetrics.put(key, lag);
    }

    /**
     * 手动 flush offset 指标
     */
    public void flushOffsetMetrics() {
        requireServerModeSilently();
        batchCommitOffsetMetrics();
        if (!serverOffsetCurrentMetrics.isEmpty()) {
            batchCommitOffsetMetrics();
        }
    }

    /**
     * 手动 flush lag 指标
     */
    public void flushLagMetrics() {
        requireServerModeSilently();
        batchCommitLagMetrics();
        if (!serverLagCurrentMetrics.isEmpty()) {
            batchCommitLagMetrics();
        }
    }

    /**
     * 添加 source_table_name 映射信息
     *
     * 仅服务端模式使用。
     */
    public void addSourceTableName(ServerTopicInfo serverTopicInfo) throws Exception {
        requireServerMode();

        String path = serverInfo.getMonitorDataPath()
                + "/" + serverTopicInfo.getTargetDataSourceId()
                + "/" + serverTopicInfo.getTargetTableName()
                + "/source_table_name";

        String data = serverTopicInfo.getBusinessDataSourceId()
                + "." + serverTopicInfo.getSchemaName()
                + "." + serverTopicInfo.getTableName();

        zkManager.readModifyWrite(path, originalData ->
                JsonUtil.sourceTableNameToJson(originalData, data));
    }

    /**
     * 立即更新某个节点
     *
     * 主要用于服务端某些立即统计写入场景。
     */
    public void updateStatisticsImmediately(String path, Object data, CreateMode createMode) {
        try {
            if (!zkManager.isConnected()) {
                logger.warn("ZooKeeper not connected, try reconnect");
                zkManager.reconnect();
            }

            zkManager.createOrUpdateNode(path, data, createMode);
        } catch (Exception e) {
            logger.warn("Failed to update statistics immediately, path={}", path, e);
        }
    }

    // -------------------------------------------------------------------------
    // 批量提交：offset / lag
    // -------------------------------------------------------------------------

    /**
     * 批量提交服务端 offset 指标
     */
    private void batchCommitOffsetMetrics() {
        synchronized (serverOffsetSwapLock) {
            if (serverOffsetCurrentMetrics.isEmpty()) {
                return;
            }

            ConcurrentHashMap<MetricKey, Long> temp = serverOffsetCommitMetrics;
            serverOffsetCommitMetrics = serverOffsetCurrentMetrics;
            serverOffsetCurrentMetrics = temp;
            serverOffsetCurrentMetrics.clear();
        }

        serverOffsetCommitMetrics.forEach((metricKey, value) -> {
            try {
                String path = String.format(
                        serverInfo.getMonitorDataPath() + "/%s/%s/%s",
                        metricKey.getServerId(),
                        metricKey.getTargetDataSourceId(),
                        metricKey.getTargetTableName(),
                        MetricType.OFFSET.code()
                );

                zkManager.readModifyWrite(path,
                        originalData -> JsonUtil.offsetToJson(originalData, metricKey.getTopic(), value));
            } catch (Exception e) {
                synchronized (serverOffsetSwapLock) {
                    serverOffsetCurrentMetrics.compute(metricKey, (k, currentValue) ->
                            currentValue == null ? value : Math.max(currentValue, value));
                }
            }
        });

        serverOffsetCommitMetrics.clear();
    }

    /**
     * 批量提交服务端 lag 指标
     */
    private void batchCommitLagMetrics() {
        synchronized (serverLagSwapLock) {
            if (serverLagCurrentMetrics.isEmpty()) {
                return;
            }

            ConcurrentHashMap<MetricKey, Long> temp = serverLagCommitMetrics;
            serverLagCommitMetrics = serverLagCurrentMetrics;
            serverLagCurrentMetrics = temp;
            serverLagCurrentMetrics.clear();
        }

        serverLagCommitMetrics.forEach((metricKey, value) -> {
            try {
                String path = String.format(
                        serverInfo.getMonitorDataPath() + "/%s/%s/%s",
                        metricKey.getTargetDataSourceId(),
                        metricKey.getTargetTableName(),
                        "unconsumed_message_count"
                );

                zkManager.readModifyWrite(path,
                        originalData -> JsonUtil.unconsumedCountToJson(originalData, metricKey.getTopic(), value));
            } catch (Exception e) {
                synchronized (serverLagSwapLock) {
                    serverLagCurrentMetrics.put(metricKey, value);
                }
            }
        });

        serverLagCommitMetrics.clear();
    }

    // -------------------------------------------------------------------------
    // ZK 健康检查
    // -------------------------------------------------------------------------

    /**
     * 定时监测 ZK 连接状态
     */
    private void monitorZkConnections() {
        try {
            int connectionCount = ZooKeeperNodeManager.getCurrentConnectionCount();
            boolean isConnected = zkManager.isConnected();

            logger.info("ZooKeeper health check, mode={}, totalConnections={}, connected={}",
                    serverMode ? "server" : "client",
                    connectionCount,
                    isConnected);

            if (!isConnected) {
                logger.error("ZooKeeper disconnected, try reconnect");
                zkManager.reconnect();
            }

            if (connectionCount > 30) {
                logger.warn("ZooKeeper connection count is high: {}", connectionCount);
            }
        } catch (Exception e) {
            logger.error("ZooKeeper health check failed", e);
        }
    }

    // -------------------------------------------------------------------------
    // 关闭
    // -------------------------------------------------------------------------

    /**
     * 关闭全局管理器
     */
    public void shutdown() {
        logger.info("Shutting down GlobalZooKeeperManager, mode={}", serverMode ? "server" : "client");

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (zkManager != null) {
            zkManager.close();
        }

        ZooKeeperNodeManager.shutdownMonitor();

        int finalCount = ZooKeeperNodeManager.getCurrentConnectionCount();
        if (finalCount > 0) {
            logger.warn("ZooKeeper connections still alive after shutdown: {}", finalCount);
        }
    }

    // -------------------------------------------------------------------------
    // 模式校验
    // -------------------------------------------------------------------------

    private void requireClientMode() {
        if (!clientMode) {
            throw new IllegalStateException("Current GlobalZooKeeperManager is not in client mode");
        }
    }

    private void requireServerMode() {
        if (!serverMode) {
            throw new IllegalStateException("Current GlobalZooKeeperManager is not in server mode");
        }
    }

    /**
     * 服务端模式静默校验
     *
     * 说明：
     * 某些指标更新方法不希望在客户端场景直接抛异常中断流程，
     * 因此用这个方法做防御。
     */
    private void requireServerModeSilently() {
        if (!serverMode) {
            throw new IllegalStateException("Current GlobalZooKeeperManager is not in server mode");
        }
    }

    // -------------------------------------------------------------------------
    // MetricKey
    // -------------------------------------------------------------------------

    /**
     * 服务端监控指标唯一键
     */
    class MetricKey {

        private String serverId;
        private String topic;
        private String targetDataSourceId;
        private String targetTableName;
        private String metricType;

        public MetricKey(String serverId,
                         String topic,
                         String targetDataSourceId,
                         String targetTableName,
                         String metricType) {
            this.serverId = serverId;
            this.topic = topic;
            this.targetDataSourceId = targetDataSourceId;
            this.targetTableName = targetTableName;
            this.metricType = metricType;
        }

        public String getServerId() {
            return serverId;
        }

        public String getTopic() {
            return topic;
        }

        public String getTargetDataSourceId() {
            return targetDataSourceId;
        }

        public String getTargetTableName() {
            return targetTableName;
        }

        public String getMetricType() {
            return metricType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MetricKey)) return false;
            MetricKey metricKey = (MetricKey) o;
            return Objects.equals(serverId, metricKey.serverId)
                    && Objects.equals(topic, metricKey.topic)
                    && Objects.equals(targetDataSourceId, metricKey.targetDataSourceId)
                    && Objects.equals(targetTableName, metricKey.targetTableName)
                    && Objects.equals(metricType, metricKey.metricType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverId, topic, targetDataSourceId, targetTableName, metricType);
        }
    }
}