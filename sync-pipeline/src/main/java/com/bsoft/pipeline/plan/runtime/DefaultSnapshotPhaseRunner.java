package com.bsoft.pipeline.plan.runtime;


import com.bsoft.common.bean.BusinessDataSourceConfigRegistry;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.flink.PipelineMiniClusterManager;
import com.bsoft.pipeline.k2k.SqlNodeSchemaRegistry;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanExecutor;
import com.bsoft.pipeline.plan.PlanNodeFactory;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * 默认的 snapshot 阶段执行器。
 *
 * 职责：
 * 1. 创建 snapshot 专用 Flink env / tableEnv
 * 2. 通过 PlanContextFactory 组装 snapshot 场景的 PlanContext
 * 3. 以 snapshot factory 执行单个 RulePlan
 * 4. 提交 snapshot 作业
 *
 * 不负责：
 * - 采集 snapshot SCN
 * - 推进 plan phase 状态机
 * - 持久化运行状态
 */
public class DefaultSnapshotPhaseRunner implements SnapshotPhaseRunner {

    private static final Logger logger = LogManager.getLogger(DefaultSnapshotPhaseRunner.class);

    private final PlanContextFactory planContextFactory;
    private final BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;
    private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry;
    private final String kafkaBootstrapServers;

    private final String jobNamePrefix;

    // 共享 MiniCluster 管理器，负责真正运行 snapshot job
    private final PipelineMiniClusterManager miniClusterManager;

    public DefaultSnapshotPhaseRunner(PlanContextFactory planContextFactory,
                                      BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                      SqlNodeSchemaRegistry sqlNodeSchemaRegistry,
                                      String kafkaBootstrapServers,
                                      PipelineMiniClusterManager miniClusterManager) {
        this(
                planContextFactory,
                businessDataSourceConfigRegistry,
                sqlNodeSchemaRegistry,
                kafkaBootstrapServers,
                miniClusterManager,
                "sync-pipeline-snapshot"
        );
    }

    public DefaultSnapshotPhaseRunner(PlanContextFactory planContextFactory,
                                      BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                      SqlNodeSchemaRegistry sqlNodeSchemaRegistry,
                                      String kafkaBootstrapServers,
                                      PipelineMiniClusterManager miniClusterManager,
                                      String jobNamePrefix) {
        // 保存 PlanContext 工厂
        this.planContextFactory = Objects.requireNonNull(planContextFactory, "planContextFactory");

        // 保存业务源配置注册表
        this.businessDataSourceConfigRegistry = Objects.requireNonNull(
                businessDataSourceConfigRegistry, "businessDataSourceConfigRegistry");

        // 保存 SQL 节点 schema 注册表
        this.sqlNodeSchemaRegistry = Objects.requireNonNull(sqlNodeSchemaRegistry, "sqlNodeSchemaRegistry");

        // 保存 Kafka 地址
        this.kafkaBootstrapServers = requireText(kafkaBootstrapServers, "kafkaBootstrapServers");

        // 保存共享 MiniCluster 管理器
        this.miniClusterManager = Objects.requireNonNull(miniClusterManager, "miniClusterManager");

        // 保存 job 名称前缀
        this.jobNamePrefix = requireText(jobNamePrefix, "jobNamePrefix");
    }

    @Override
    public void run(RulePlan plan, PipelinePlanRunState state) throws Exception {
        // 校验 plan 和 state
        validate(plan, state);

        // 当前 planId
        String planId = state.getPlanId();

        // 当前 runId
        String runId = state.getRunId();

        // 当前 snapshot 基线 SCN
        Long snapshotScn = state.getSnapshotScn();

        logger.info("start snapshot phase. planId={}, runId={}, snapshotScn={}", planId, runId, snapshotScn);

        // 每个 plan 创建自己的 job 构建配置
        Configuration configuration = buildSnapshotConfiguration();

        // 每个 plan 创建自己的 StreamExecutionEnvironment，只用于构建当前 JobGraph
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 每个 plan 创建自己的 StreamTableEnvironment，避免 TableEnv 状态互相污染
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 构建 snapshot 模式下的 PlanContext
        PlanContext context = planContextFactory.createSnapshotContext(
                PlanContextFactory.SnapshotContextSpec.builder()
                        .env(env)
                        .tableEnv(tableEnv)
                        .kafkaBootstrapServers(kafkaBootstrapServers)
                        .planId(planId)
                        .runId(runId)
                        .snapshotScn(snapshotScn)
                        .businessDataSourceConfigRegistry(businessDataSourceConfigRegistry)
                        .sqlNodeSchemaRegistry(sqlNodeSchemaRegistry)
                        .build()
        );

        // snapshot 阶段使用 SnapshotSqlPlanNodeCreator
        PlanNodeFactory factory = PipelinePlanNodeFactoryBuilder.buildSnapshotFactory(context);

        // 执行 plan，实际是在 env/tableEnv 上构建当前 job 的拓扑
        PlanExecutor executor = new PlanExecutor(factory);
        executor.execute(plan, context);

        // 构建当前 snapshot job 名称
        String jobName = buildJobName(planId, runId);

        logger.info("submit snapshot flink job to shared MiniCluster. planId={}, runId={}, jobName={}",
                planId, runId, jobName);

        // 提交到共享 MiniCluster，并等待当前 snapshot job 完成
        miniClusterManager.submitAndWait(env, jobName);

        logger.info("finish snapshot phase. planId={}, runId={}", planId, runId);
    }

    protected Configuration buildSnapshotConfiguration() {
        // 从共享 MiniCluster 管理器创建当前 job 的配置
        Configuration configuration = miniClusterManager.createJobConfiguration();

        // snapshot 当前先保持单 job 内并行度为 1
        configuration.setString("parallelism.default", "1");

        return configuration;
    }

    protected String buildJobName(String planId, String runId) {
        return jobNamePrefix + "-" + safe(planId) + "-" + safe(runId);
    }

    private void validate(RulePlan plan, PipelinePlanRunState state) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(state, "state");

        requireText(plan.getPlanId(), "plan.planId");
        requireText(state.getPlanId(), "state.planId");
        requireText(state.getRunId(), "state.runId");

        if (!plan.getPlanId().trim().equals(state.getPlanId().trim())) {
            throw new IllegalArgumentException(
                    "planId mismatch, plan.planId=" + plan.getPlanId() + ", state.planId=" + state.getPlanId()
            );
        }

        if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
            throw new IllegalArgumentException("plan nodes is empty, planId=" + plan.getPlanId());
        }

        if (state.getSnapshotScn() == null) {
            throw new IllegalArgumentException("snapshotScn is null, planId=" + state.getPlanId());
        }
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }

    private String safe(String raw) {
        String v = raw == null ? "null" : raw.trim().toLowerCase();
        v = v.replaceAll("[^a-z0-9_\\-]", "_");
        return v.isEmpty() ? "unknown" : v;
    }
}
