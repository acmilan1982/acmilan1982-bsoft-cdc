package com.bsoft.common.global;



import com.bsoft.common.bean.BaseInfo;
import com.bsoft.common.bean.ClientInfo;
import com.bsoft.common.bean.ServerInfo;
import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.enums.JobStatus;
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
 *  在zk上更新，offset，未消费消息数等监控数据的全局管理器
 */
public class GlobalZooKeeperManager {
    private static final Logger logger = LogManager.getLogger(GlobalZooKeeperManager.class);

    private static volatile GlobalZooKeeperManager instance;
    private static final Object LOCK = new Object();

    private final ZooKeeperNodeManager zkManager;
    private final ScheduledExecutorService scheduler;

    // 使用双缓冲机制-服务端
    private volatile ConcurrentHashMap<MetricKey, Long> serverCurrentMetrics;
    private volatile ConcurrentHashMap<MetricKey, Long> serverCommitMetrics;
    private final Object serverSwapLock = new Object();
    private  ServerInfo serverInfo;


    // 使用双缓冲机制-客户端
    private volatile ConcurrentHashMap<String, Long> clientCurrentMetrics;
    private volatile ConcurrentHashMap<String, Long> clientCommitMetrics;
    private final Object clientSwapLock = new Object();
    private  ClientInfo clientInfo;

    private GlobalZooKeeperManager(ServerInfo serverInfo) throws Exception {
        this.serverInfo = serverInfo;
        this.zkManager = new ZooKeeperNodeManager(serverInfo);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                new Thread(r, "zk-offset-commit-thread"));

        // 初始化双缓冲区
        this.serverCurrentMetrics = new ConcurrentHashMap<>();
        this.serverCommitMetrics = new ConcurrentHashMap<>();

        // 可配置的提交间隔（默认10秒）
        int commitIntervalSeconds = 10; // 或从配置文件读取
        scheduler.scheduleAtFixedRate(this::serverBatchCommitMetrics,
                commitIntervalSeconds, commitIntervalSeconds, TimeUnit.SECONDS);

        logger.info("GlobalZooKeeperOffsetManager已经初始化: {}", serverInfo.getServerId());

        // 添加连接监控任务
        scheduler.scheduleAtFixedRate(this::monitorZkConnections,
                60, 60, TimeUnit.SECONDS); // 每分钟检查一次
    }


    /**
     * 监控ZK连接状态
     */
    private void monitorZkConnections() {
        try {
            int connectionCount = ZooKeeperNodeManager.getCurrentConnectionCount();
            boolean isConnected = zkManager.isConnected();

            logger.info("🔍 ZK连接健康检查 - 总连接数: {}, 当前连接状态: {}",
                    connectionCount, isConnected ? "正常" : "异常");

            // 连接数异常告警
            if (connectionCount > 30) {
                logger.warn("⚠️ ZK连接数过多: {}, 可能存在连接泄漏!", connectionCount);

                // 打印连接详情
                Map<String, Long> registry = ZooKeeperNodeManager.getConnectionRegistry();
                registry.forEach((id, createTime) -> {
                    long age = System.currentTimeMillis() - createTime;
                    if (age > 600000) { // 10分钟以上的连接
                        logger.warn("  └─ 长期连接: {} ({}ms)", id, age);
                    }
                });
            }

            // 连接状态异常处理
            if (!isConnected) {
                logger.error("❌ ZK连接已断开，尝试重连...");
                zkManager.reconnect();
            }

        } catch (Exception e) {
            logger.error("ZK连接监控检查失败", e);
        }
    }

    // 探针端实例化
    private GlobalZooKeeperManager(ClientInfo clientInfo) throws Exception {
        this.clientInfo = clientInfo;
        this.zkManager = new ZooKeeperNodeManager(clientInfo);


        // 初始化双缓冲区
        this.clientCurrentMetrics = new ConcurrentHashMap<>();
        this.clientCommitMetrics = new ConcurrentHashMap<>();

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
                new Thread(r, "zk-offset-commit-thread"));

        // 可配置的提交间隔（默认10秒）
        int commitIntervalSeconds = 10; // 或从配置文件读取
        scheduler.scheduleAtFixedRate(this::clientBatchCommitMetrics,
                commitIntervalSeconds, commitIntervalSeconds, TimeUnit.SECONDS);

        logger.info("GlobalZooKeeperOffsetManager已经初始化: {}", clientInfo.getClientId());

        // 添加连接监控任务
        scheduler.scheduleAtFixedRate(this::monitorZkConnections,
                60, 60, TimeUnit.SECONDS); // 每分钟检查一次
    }

    // 单例方法
    public static GlobalZooKeeperManager getInstance(BaseInfo baseInfo) throws Exception {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    if (baseInfo instanceof ServerInfo) {
                        instance = new GlobalZooKeeperManager((ServerInfo) baseInfo);
                    } else if (baseInfo instanceof ClientInfo) {
                        instance = new GlobalZooKeeperManager((ClientInfo) baseInfo);
                    } else {
                        throw new IllegalArgumentException("不支持的BaseInfo类型: " + baseInfo.getClass());
                    }
                }
            }
        }
        return instance;
    }


    // 客户端：创建zk节点,ip，status，scn，stopSignal，并在zk上更新当前job状态
    public void initClientZKNode() throws Exception {
        // zk上创建客户端节点
        this.zkManager.createClientNodes();
        // 更新客户端IP节点
        this.zkManager.updateIpNode(clientInfo.getIp() + ":" + clientInfo.getServerPort());
        // 清空停止信号路径数据
        this.zkManager.clearNodeData(clientInfo.getStopSignalPath());
        // 清空scn路径数据
        this.zkManager.clearNodeData(clientInfo.getClientScnPath());

    }

    public void initServerZKNode() throws Exception {
        this.zkManager.createServerNodes(serverInfo);
        this.zkManager.updateIpNode(serverInfo.getIp() + ":" + serverInfo.getServerPort());
    }

    public void updateStatusNode(JobStatus jobStatus) throws Exception {
        this.zkManager.updateStatusNode(jobStatus);
    }

    public String getNodeData(String serverIPPath) throws Exception {
        return this.zkManager.getNodeData(serverIPPath);
    }

    /**
     * 更新offset（线程安全，非阻塞）
     */
    public void updateMetric(String topic,String targetDataSourceId,String targetTableName, String metricType, long value) {
        String key = targetDataSourceId + "_" + targetTableName + "_" + metricType;
        MetricKey metricKey = new MetricKey(topic,targetDataSourceId, targetTableName, metricType);
        serverCurrentMetrics.compute(metricKey, (k, currentValue) ->
                currentValue == null ? value : Math.max(currentValue, value));
    }

    public void updateOffset(Map<String,String> metric) {
        String topic = metric.get("topic");
        String targetDataSourceId = metric.get("targetDataSourceId");
        String targetTableName = metric.get("targetTableName");
        Long offset = Long.parseLong( metric.get("offset"));
        updateMetric(topic,targetDataSourceId,targetTableName,"offset",offset);
    }

    public void updateUnconsumedMessageCount(Map<String,String> metric) {
        String topic = metric.get("topic");
        String targetDataSourceId = metric.get("targetDataSourceId");
        String targetTableName = metric.get("targetTableName");
        Long unconsumedMessageCount = Long.parseLong( metric.get("unconsumedMessageCount"));
        updateMetric(topic,targetDataSourceId,targetTableName,"sumedMessageCount",unconsumedMessageCount);
    }

    // 客户端更新scn
    public void updateScn(String metric, Long scn) {
        clientCurrentMetrics.compute(metric, (k, currentValue) ->
                currentValue == null ? scn : Math.max(currentValue, scn));
    }
    /**
     * 批量提交所有pending的offset到ZK
     */

    private void serverBatchCommitMetrics() {
        synchronized (serverSwapLock) {
            if (serverCurrentMetrics.isEmpty()) {
                return;
            }

            ConcurrentHashMap<MetricKey, Long> temp = serverCommitMetrics;
            serverCommitMetrics = serverCurrentMetrics;
            serverCurrentMetrics = temp;
            serverCurrentMetrics.clear();
        }

        if (serverCommitMetrics.isEmpty()) {
            return;
        }

        logger.debug("Committing {} metrics to ZooKeeper", serverCommitMetrics.size());

        serverCommitMetrics.forEach((metricKey, value) -> {
            try {
                String topic = metricKey.getTopic();
                String targetDataSourceId = metricKey.getTargetDataSourceId();
                String targetTableName = metricKey.getTargetTableName();
                String metricType = metricKey.getMetricType();
                if("offset".equals(metricType)){
                    String path = String.format(serverInfo.getMonitorDataPath() + "/%s/%s/%s", targetDataSourceId,targetTableName, metricType);
                    zkManager.readModifyWrite(path, originalData -> {
                        return JsonUtil.offsetToJson(originalData,topic,value); // 返回修改后的数据
                    });
                }
                if("unconsumedMessageCount".equals(metricType)){
                    String path = String.format(serverInfo.getMonitorDataPath() + "/%s/%s/%s", targetDataSourceId,targetTableName, "unconsumed_message_count");
                    zkManager.readModifyWrite(path, originalData -> {
                        return JsonUtil.unconsumedCountToJson(originalData,topic,value); // 返回修改后的数据
                    });
                }


//                // 节点路径格式：/监控根路径/目标数据源ID/目标表名/unconsumed_message_count
//                String path = String.format("%s/%s/%s/unconsumed_message_count",
//                        serverInfo.getMonitorDataPath(),
//                        targetDataSourceId,
//                        targetTableName);

//                if(metricType.equals("offset") ){
//                    zkManager.createOrUpdateNode(path, JsonUtil.offsetToJson(value), CreateMode.PERSISTENT);
//                }


//
//                if(metricType.equals("unconsumedMessageCount") ){
//                    zkManager.createOrUpdateNode(path, JsonUtil.unconsumedCountToJson(value), CreateMode.PERSISTENT);
//                }


                logger.debug("Successfully committed {} {} for table {}", metricType, value, targetTableName);
            } catch (Exception e) {
                logger.warn("Failed to commit metric for key {}: {}", metricKey, e.getMessage());
                synchronized (serverSwapLock) {
                    serverCurrentMetrics.compute(metricKey, (k, currentValue) ->
                            currentValue == null ? value : Math.max(currentValue, value));
                }
            }
        });

        serverCommitMetrics.clear();
    }

    private void clientBatchCommitMetrics() {
        synchronized (clientSwapLock) {
            if (clientCurrentMetrics.isEmpty()) {
                return;
            }

            ConcurrentHashMap<String, Long> temp = clientCommitMetrics;
            clientCommitMetrics = clientCurrentMetrics;
            clientCurrentMetrics = temp;
            clientCurrentMetrics.clear();
        }

        if (clientCommitMetrics.isEmpty()) {
            return;
        }

        clientCommitMetrics.forEach((metric, value) -> {
            try {

                if("scn".equals(metric)){
                    this.zkManager.updateScnNode(value.toString());
                }

                logger.debug("客户端更新zk: 节点： {} ，data : {}", metric, value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        clientCommitMetrics.clear();
    }

    public void addSourceTableName(ServerTopicInfo serverTopicInfo) throws Exception {
        String path = serverInfo.getMonitorDataPath() + "/" + serverTopicInfo.getTargetDataSourceId() + "/" + serverTopicInfo.getTargetTableName() + "/" + "source_table_name";
        String data = serverTopicInfo.getBusinessDataSourceId() + "." + serverTopicInfo.getSchemaName() + "." + serverTopicInfo.getTableName();
        zkManager.readModifyWrite(path, originalData -> {
            return JsonUtil.sourceTableNameToJson(originalData,data); // 返回修改后的数据
        });

    }
    /**
     * 立即提交所有pending的offset（用于应用关闭时）
     */
    public void flushOffsets() {
        logger.info("Flushing all pending offsets to ZooKeeper");
        serverBatchCommitMetrics();
        // 再次检查是否还有未提交的offset
        if (!serverCurrentMetrics.isEmpty()) {
            serverBatchCommitMetrics();
        }
    }


    /**
     * 关闭管理器
     */
    public void shutdown() {
        logger.info("Shutting down Global ZooKeeper Offset Manager");

        // 先提交所有pending的offset
        flushOffsets();

        // 关闭定时任务
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

        // 关闭ZK连接
        if (zkManager != null) {
            zkManager.close();
        }

        // 关闭监控器
        ZooKeeperNodeManager.shutdownMonitor();

        // 最终检查
        int finalCount = ZooKeeperNodeManager.getCurrentConnectionCount();
        if (finalCount > 0) {
            logger.warn("⚠️ 关闭后仍有 {} 个ZK连接未释放!", finalCount);
        }

        logger.info("Global ZooKeeper Manager shutdown completed");
    }


    /**
     * 即时更新统计数据到ZK（同步操作）
     */
    public void updateStatisticsImmediately(String path, Object data, CreateMode createMode) {
        try {
            // 添加连接状态检查
            if (!zkManager.isConnected()) {
                logger.warn("ZooKeeper connection is not available, attempting to reconnect...");
                zkManager.reconnect();
            }

            zkManager.createOrUpdateNode(path, data, createMode);
            logger.debug("Successfully updated statistics to path: {}", path);
        } catch (IllegalStateException e) {
            e.printStackTrace();
            if (e.getMessage().contains("Expected state [STARTED] was [STOPPED]")) {
                logger.warn("ZooKeeper client is stopped, attempting to reconnect...");
                try {
                    zkManager.reconnect();
                    zkManager.createOrUpdateNode(path, data, createMode);
                } catch (Exception retryException) {
                    logger.error("Failed to reconnect and update statistics: {}", retryException.getMessage());
//                    throw new RuntimeException("Failed to update statistics after reconnection attempt", retryException);
                }
            } else {
//                throw new RuntimeException("Failed to update statistics", e);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.warn("Failed to update statistics to path {}: {}", path, e.getMessage());
//            throw new RuntimeException("Failed to update statistics", e);
        }
    }


    class MetricKey{


        private String topic;
        private String targetDataSourceId;
        private String targetTableName;
        private String metricType;

        public MetricKey(String topic,String targetDataSourceId, String targetTableName, String metricType) {
            this.topic = topic;
            this.targetDataSourceId = targetDataSourceId;
            this.targetTableName = targetTableName;
            this.metricType = metricType;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getTargetDataSourceId() {
            return targetDataSourceId;
        }

        public void setTargetDataSourceId(String targetDataSourceId) {
            this.targetDataSourceId = targetDataSourceId;
        }

        public String getTargetTableName() {
            return targetTableName;
        }

        public void setTargetTableName(String targetTableName) {
            this.targetTableName = targetTableName;
        }

        public String getMetricType() {
            return metricType;
        }

        public void setMetricType(String metricType) {
            this.metricType = metricType;
        }


        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            MetricKey metricKey = (MetricKey) o;
            return Objects.equals(topic, metricKey.topic) && Objects.equals(targetDataSourceId, metricKey.targetDataSourceId) && Objects.equals(targetTableName, metricKey.targetTableName) && Objects.equals(metricType, metricKey.metricType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, targetDataSourceId, targetTableName, metricType);
        }
    }
}