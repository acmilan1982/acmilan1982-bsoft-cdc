package com.bsoft.client.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * 进程内共享 MiniCluster 管理器
 *
 * 设计目标：
 * 1. 一个 sync-client 进程内，只启动一个 MiniCluster
 * 2. 多个 BusinessDataSourceConfig 对应的多个 Flink Job，共享这一个 MiniCluster
 * 3. 统一管理 MiniCluster 生命周期：启动 / 获取实例 / 关闭
 *
 * 说明：
 * 1. 这个类只负责“共享集群”层面的事情
 * 2. 不负责业务库配置解析
 * 3. 不负责 OracleSource / KafkaSink 的业务拼装
 * 4. 不负责 dataSourceId 级别的 stop signal / scn / checkpoint 逻辑
 * 5. 不在这里直接封装 job 提交动作，避免和上层 job graph 构建逻辑绑死
 *
 * 当前版本职责边界：
 * 1. start()：启动唯一的 MiniCluster
 * 2. getMiniCluster()：向上层暴露共享 MiniCluster 实例
 * 3. getClusterConfiguration()：向上层暴露集群级配置
 * 4. close()：关闭 MiniCluster
 *
 * 注意：
 * 1. 这里只保留“共享集群”能力
 * 2. Job 的 StreamExecutionEnvironment / StreamGraph / JobGraph 构建，放到上层去做
 * 3. Web UI 端口是集群级的，只保留一个
 */
public class SharedMiniClusterManager implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(SharedMiniClusterManager.class);

    /**
     * MiniCluster 的全局配置
     *
     * 这是“集群级配置”，不是某个具体 job 的配置。
     */
    private final Configuration clusterConfiguration;

    /**
     * 共享的 MiniCluster 实例
     */
    private MiniCluster miniCluster;

    /**
     * MiniCluster 是否已经启动
     */
    private volatile boolean started = false;

    /**
     * 共享 MiniCluster 的 Web UI 端口
     *
     * 说明：
     * 1. 这是整个共享集群唯一的 UI 端口
     * 2. 不再像旧版本那样为每个 dataSourceId 分配一个端口
     */
    private final int webUiPort;

    /**
     * 集群 TaskManager 数量
     */
    private final int numTaskManagers;

    /**
     * 每个 TaskManager 的 slot 数
     */
    private final int numSlotsPerTaskManager;

    /**
     * 构造器
     *
     * @param webUiPort 共享 MiniCluster 的 Web UI 端口
     * @param numTaskManagers TaskManager 数量
     * @param numSlotsPerTaskManager 每个 TaskManager 的 slot 数
     */
    public SharedMiniClusterManager(int webUiPort,
                                    int numTaskManagers,
                                    int numSlotsPerTaskManager) {
        this.webUiPort = webUiPort;
        this.numTaskManagers = numTaskManagers;
        this.numSlotsPerTaskManager = numSlotsPerTaskManager;

        this.clusterConfiguration = new Configuration();
        this.clusterConfiguration.setInteger(RestOptions.PORT, webUiPort);
    }

    /**
     * 启动共享 MiniCluster
     *
     * 说明：
     * 1. 整个 sync-client 进程生命周期内通常只调用一次
     * 2. 重复调用时直接忽略
     */
    public synchronized void start() throws Exception {
        if (started) {
            logger.info("Shared MiniCluster already started, skip.");
            return;
        }

        logger.info("Starting shared MiniCluster, webUiPort={}, numTaskManagers={}, numSlotsPerTaskManager={}",
                webUiPort, numTaskManagers, numSlotsPerTaskManager);

        MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(clusterConfiguration)
                        .setNumTaskManagers(numTaskManagers)
                        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                        .build();

        this.miniCluster = new MiniCluster(miniClusterConfiguration);
        this.miniCluster.start();

        this.started = true;

        logger.info("Shared MiniCluster started successfully, webUiPort={}", webUiPort);
    }

    /**
     * 获取共享 MiniCluster 实例
     *
     * 说明：
     * 1. 上层如果要提交 job，需要显式拿到共享 MiniCluster
     * 2. 当前类不再直接封装 submitJob(...)，避免参数错位
     */
    public MiniCluster getMiniCluster() {
        ensureStarted();
        return miniCluster;
    }

    /**
     * 获取集群级配置
     *
     * 说明：
     * 1. 上层如果需要构建和共享集群一致的执行环境/提交配置，可以读取这里
     * 2. 这里只是集群级配置，不应混入业务库级参数
     */
    public Configuration getClusterConfiguration() {
        return clusterConfiguration;
    }

    /**
     * 获取共享 MiniCluster 的 Web UI 端口
     */
    public int getWebUiPort() {
        return webUiPort;
    }

    /**
     * 获取 TaskManager 数量
     */
    public int getNumTaskManagers() {
        return numTaskManagers;
    }

    /**
     * 获取每个 TaskManager 的 slot 数
     */
    public int getNumSlotsPerTaskManager() {
        return numSlotsPerTaskManager;
    }

    /**
     * 判断共享 MiniCluster 是否已启动
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * 确保共享 MiniCluster 已启动
     */
    private void ensureStarted() {
        if (!started || miniCluster == null) {
            throw new IllegalStateException("Shared MiniCluster is not started");
        }
    }

    /**
     * 关闭共享 MiniCluster
     *
     * 说明：
     * 1. 一般在 sync-client 进程退出时调用
     * 2. 会关闭整个共享集群
     */
    @Override
    public synchronized void close() throws Exception {
        if (!started || miniCluster == null) {
            return;
        }

        logger.info("Closing shared MiniCluster, webUiPort={}", webUiPort);

        try {
            miniCluster.closeAsync().get(60, TimeUnit.SECONDS);
        } finally {
            miniCluster = null;
            started = false;
        }

        logger.info("Shared MiniCluster closed successfully.");
    }
}