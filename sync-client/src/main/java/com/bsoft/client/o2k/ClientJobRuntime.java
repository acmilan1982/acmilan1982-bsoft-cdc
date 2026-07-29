package com.bsoft.client.o2k;



import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.ClientTopicInfo;
import com.bsoft.common.bean.ExecutionPlan;
import com.bsoft.common.bean.OracleStartupOffset;
import com.bsoft.common.enums.JobStartupMode;
import org.apache.flink.core.execution.JobClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 单个业务库对应的 Job 运行时上下文
 *
 * 设计背景：
 * 1. 现在一个 sync-client 进程可以同时采集多个业务库
 * 2. 每个业务库对应一个独立 Flink Job
 * 3. 因此需要一个对象，把“某个业务库对应 job 的所有运行时信息”集中管理
 *
 * 职责：
 * 1. 聚合当前业务库对应 job 所需的静态配置与运行态对象
 * 2. 作为 OracleToKafka 中 runtimeByDataSourceId 的 value
 * 3. 让“一个业务库 = 一个独立 job”的边界明确化
 *
 * 典型包含内容：
 * 1. 当前业务库配置
 * 2. 当前业务库自己的 scoped execution plan
 * 3. 当前业务库对应的 topics
 * 4. 当前业务库对应的启动模式
 * 5. 当前业务库实际运行中的 JobClient
 * 6. 当前业务库运行时推导出的 Kafka 启动位点
 */
public class ClientJobRuntime {

    /**
     * 当前业务库配置
     *
     * 说明：
     * 这里保存的是当前业务库自己的连接信息、表配置、主键信息等。
     */
    private final BusinessDataSourceConfig businessDataSourceConfig;

    /**
     * 当前业务库对应的局部执行计划
     *
     * 说明：
     * 1. 配置库里读出来的是“当前客户端的全局执行计划”
     * 2. OracleToKafka 会按 fromDataSourceId 过滤出当前业务库自己的 scopedExecutionPlan
     * 3. 该字段只包含当前 dataSourceId 需要执行的表计划
     */
    private final ExecutionPlan scopedExecutionPlan;

    /**
     * 当前业务库对应的 Topic 列表
     *
     * 说明：
     * 1. 这些 topic 是基于 scopedExecutionPlan 生成的
     * 2. 当前业务库 job 只会使用这批 topic
     * 3. 不会使用其它业务库的 topic
     */
    private final List<ClientTopicInfo> topicInfos;

    // 当前业务库 job 的启动模式，重启前会从 CDC_DATA_SOURCE_RUN_STATE 刷新。
    private volatile JobStartupMode startupMode;

    /**
     * 当前业务库对应的 Flink JobClient
     *
     * 说明：
     * 1. job 真正启动后才会赋值
     * 2. 在启动前该字段为空
     * 3. 用于 stop / wait / restart / 状态检查
     */
    private volatile JobClient jobClient;



    // 从 Kafka 最新消息中推断出的 Oracle CDC 启动位点。
    private volatile OracleStartupOffset kafkaStartupOffset;

    public ClientJobRuntime(BusinessDataSourceConfig businessDataSourceConfig,
                            ExecutionPlan scopedExecutionPlan,
                            List<ClientTopicInfo> topicInfos,
                            JobStartupMode startupMode) {
        this.businessDataSourceConfig = Objects.requireNonNull(businessDataSourceConfig);
        this.scopedExecutionPlan = Objects.requireNonNull(scopedExecutionPlan);
        this.topicInfos = new ArrayList<>(Objects.requireNonNull(topicInfos));
        this.startupMode = Objects.requireNonNull(startupMode);
    }

    /**
     * 获取当前业务库 ID
     *
     * 这是最常用的运行时标识。
     */
    public String getDataSourceId() {
        return businessDataSourceConfig.getDataSourceId();
    }

    public BusinessDataSourceConfig getBusinessDataSourceConfig() {
        return businessDataSourceConfig;
    }

    public ExecutionPlan getScopedExecutionPlan() {
        return scopedExecutionPlan;
    }

    public List<ClientTopicInfo> getTopicInfos() {
        return topicInfos;
    }

    // 获取当前业务库 job 的启动模式。
    public JobStartupMode getStartupMode() {
        // 返回当前内存中的启动模式。
        return startupMode;
    }

    // 更新当前业务库 job 的启动模式，供失败自动重启前刷新使用。
    public void setStartupMode(JobStartupMode startupMode) {
        // 启动模式不允许为空，避免后续构建 job 时出现隐式空指针。
        this.startupMode = Objects.requireNonNull(startupMode, "startupMode must not be null");
    }


    public JobClient getJobClient() {
        return jobClient;
    }

    public void setJobClient(JobClient jobClient) {
        this.jobClient = jobClient;
    }




    /**
     * 获取从 Kafka 推断出的 Oracle CDC 启动位点。
     */
    public OracleStartupOffset getKafkaStartupOffset() {
        // 返回当前启动位点。
        return kafkaStartupOffset;
    }

    /**
     * 设置从 Kafka 推断出的 Oracle CDC 启动位点。
     */
    public void setKafkaStartupOffset(OracleStartupOffset kafkaStartupOffset) {
        // 保存当前启动位点。
        this.kafkaStartupOffset = kafkaStartupOffset;
    }

    /**
     * 判断当前 runtime 是否存在 Kafka 启动位点。
     */
    public boolean hasKafkaStartupOffset() {
        // 判断启动位点是否存在。
        return kafkaStartupOffset != null;
    }


    /**
     * 判断当前运行时是否已经绑定了 Flink JobClient
     *
     * 用途：
     * 1. 调度器中快速判断该业务库 job 是否已经启动
     * 2. 避免空指针判断散落在各处
     */
    public boolean hasJobClient() {
        return jobClient != null;
    }



    @Override
    public String toString() {
        return "ClientJobRuntime{" +
                "dataSourceId='" + getDataSourceId() + '\'' +
                ", startupMode=" + startupMode +
                ", topicCount=" + topicInfos.size() +
                ", hasJobClient=" + (jobClient != null) +
                '}';
    }
}