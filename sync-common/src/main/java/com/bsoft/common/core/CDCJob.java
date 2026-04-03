package com.bsoft.common.core;

import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.LogServerInfo;
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.global.GlobalZooKeeperManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * ClassName: CDCJob
 * Package:   com.bsoft
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/3 - 19:05
 */
public class CDCJob {

    protected static final Logger logger = LogManager.getLogger(CDCJob.class);

    // 客户端信息
    @Autowired
    protected ClientInfo clientInfo;
    // 客户端信息
    @Autowired
    protected ServerInfo serverInfo;

    // 客户端信息
    @Autowired
    protected LogServerInfo logServerInfo;


    //
    @Autowired
    protected PipeLineInfo pipeLineInfo;

    // 主题信息注册中心
    protected TopicInfoRegistry topicInfoRegistry = new TopicInfoRegistry();


    // 当前运行时的执行计划
    protected ExecutionPlan executionPlan;

    // 作业状态
    protected JobStatus jobStatus = JobStatus.UNKNOWN;

    // 全局 ZooKeeper 节点管理器
    protected transient GlobalZooKeeperManager globalZooKeeperManager;

    // 客户端：创建zk节点,ip，status，scn，stopSignal，并在zk上更新当前job状态
    protected void initClientZKNode() throws Exception {
        // zk上创建节点
        this.globalZooKeeperManager.initClientZKNode();
        // 更新为初始化状态
        notifyJobStatusChange(JobStatus.INITIALIZING);
    }


    // 服务端：初始化zk节点,ip+port+status，
    public void initServerZKNode() throws Exception {
        this.globalZooKeeperManager.initServerZKNode();
    }

    // 设置作业状态并立即同步到ZK
    protected void notifyJobStatusChange(JobStatus jobStatus) {
        try {
            JobStatus oldStatus = this.jobStatus;
            this.jobStatus = jobStatus;
            // 状态变化时立即同步到 ZK
            if (oldStatus != jobStatus) {
                logger.info("作业状态变更: {} -> {}", oldStatus, jobStatus);
                this.jobStatus = jobStatus;
                this.globalZooKeeperManager.updateStatusNode(jobStatus);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }
}
