package com.bsoft.common.core;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.ExecutionPlan;
import com.bsoft.common.bean.TopicInfoRegistry;
import com.bsoft.common.config.properties.ClientInfo;


import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.JobStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * CDC 基础作业类
 *
 * 职责：
 * 1. 为 client / server / log / pipeline 等作业提供公共字段
 * 2. 提供客户端级、服务端级的基础 ZK 初始化能力
 * 3. 提供“进程级状态”同步到 ZooKeeper 的基础方法
 *
 * 设计说明：
 * 1. 当前版本服务于“多数据源、多 job”的客户端模型
 * 2. 因此这里不再持有“单个 jobClient / 单个 topicInfoRegistry / 单个 executionPlan”
 * 3. 具体某个业务库对应的 job 状态，应该由上层按 dataSourceId 自己管理
 * 4. 这里保留的是“进程级状态”，例如：
 *    - 当前客户端进程是否初始化完成
 *    - 当前服务端进程是否已启动
 *
 * 注意：
 * 1. notifyJobStatusChange(...) 现在表达的是“进程级状态”
 * 2. 如果后续需要“job 级状态”同步，应在上层用 ZooKeeperNodeManager 按 dataSourceId 单独写入
 */
public class CDCJob {

    protected static final Logger logger = LogManager.getLogger(CDCJob.class);

    /**
     * 客户端配置
     */
    @Autowired(required = false)
    protected ClientInfo clientInfo;

    /**
     * 服务端配置
     */
    @Autowired(required = false)
    protected ServerInfo serverInfo;



    //全局执行计划
    protected ExecutionPlan executionPlan;

    /**
     * 当前进程级状态
     *
     * 注意：
     * 这是进程级状态，不是业务库 job 级状态。
     */
    protected JobStatus jobStatus = JobStatus.UNKNOWN;

//    /**
//     * 全局 ZooKeeper 管理器
//     *
//     * 说明：
//     * 1. 这里保留进程级全局管理器
//     * 2. 客户端 job 级的 signal/scn/status 不建议继续通过它维护
//     * 3. job 级状态应由上层通过 ZooKeeperNodeManager 直接按路径管理
//     */
//    protected transient GlobalZooKeeperManager globalZooKeeperManager;


    // 主题信息注册中心
    protected TopicInfoRegistry topicInfoRegistry = new TopicInfoRegistry();

    @Autowired
    protected ConfigDataSourceConfig configDataSourceConfig;

    protected String normalizeText(String value) {
        return value == null ? "" : value.trim();
    }

//    /**
//     * 初始化客户端级 ZK 节点
//     *
//     * 客户端级路径通常包括：
//     * 1. /clients/{clientId}/status
//     * 2. /clients/{clientId}/ip
//     *
//     * 注意：
//     * 1. 这里只负责客户端进程级节点
//     * 2. 业务库 job 级节点应由上层自行初始化：
//     *    /clients/{clientId}/jobs/{dataSourceId}/...
//     */
//    protected void initClientZKNode() throws Exception {
//        // 创建客户端级 ZK 节点
//        this.globalZooKeeperManager.initClientZKNode();
//
//        // 初始化完成后，同步进程级状态
//        notifyJobStatusChange(JobStatus.INITIALIZING);
//    }
//
//    /**
//     * 初始化服务端级 ZK 节点
//     */
//    public void initServerZKNode() throws Exception {
//        this.globalZooKeeperManager.initServerZKNode();
//    }
//
//    /**
//     * 更新进程级状态并同步到 ZooKeeper
//     *
//     * 注意：
//     * 1. 这里同步的是“当前 JVM / 当前服务实例”的总体状态
//     * 2. 不适合表达某个 dataSourceId 对应 job 的状态
//     * 3. 如果后续需要 job 级状态，请在上层单独实现，例如：
//     *    /clients/{clientId}/jobs/{dataSourceId}/status
//     */
//    protected void notifyJobStatusChange(JobStatus jobStatus) {
//        try {
//            JobStatus oldStatus = this.jobStatus;
//            this.jobStatus = jobStatus;
//
//            if (oldStatus != jobStatus) {
//                logger.info("Process-level job status changed: {} -> {}", oldStatus, jobStatus);
//
//                // 同步到客户端级 / 服务端级状态节点
//                this.globalZooKeeperManager.updateStatusNode(jobStatus);
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to update process-level job status", e);
//        }
//    }

    /**
     * 获取当前进程级状态
     */
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    /**
     * 设置当前进程级状态
     *
     * 注意：
     * 该方法只修改本地字段，不自动同步到 ZooKeeper。
     * 如需同步，使用 notifyJobStatusChange(...)。
     */
    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    /**
     * 获取当前进程级执行计划
     */
    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    /**
     * 设置当前进程级执行计划
     */
    public void setExecutionPlan(ExecutionPlan executionPlan) {
        this.executionPlan = executionPlan;
    }
}