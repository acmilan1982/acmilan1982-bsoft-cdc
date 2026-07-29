package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.flink.PipelineMiniClusterManager;
import com.bsoft.pipeline.functions.OracleCdcRowMapper;
import com.bsoft.pipeline.functions.ScnFilteringMessageDeserializer;
import com.bsoft.pipeline.k2k.RuntimeTableSchema;
import com.bsoft.pipeline.k2k.SqlNodeSchemaRegistry;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanExecutor;
import com.bsoft.pipeline.plan.PlanNodeFactory;
import com.bsoft.pipeline.plan.sql.SqlScopeAliasRegistry;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 默认的增量阶段执行器。
 *
 * 职责：
 * 1. 为增量阶段创建 Flink env / tableEnv
 * 2. 从 Kafka 构建各单表 changelog 流
 * 3. 按 snapshotScn 过滤掉基线之前的数据
 * 4. 注册临时视图和 SQL alias
 * 5. 执行单个 RulePlan 的增量 SQL
 */
public class DefaultIncrementalPhaseRunner implements IncrementalPhaseRunner {

    private static final Logger logger = LogManager.getLogger(DefaultIncrementalPhaseRunner.class);

    private final PlanContextFactory planContextFactory;
    private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry;
    private final List<ServerTopicInfo> topicInfos;
    private final RuntimeTableSchemaResolver runtimeTableSchemaResolver;
    private final String kafkaBootstrapServers;

    private final String serverId;
    private final EnvironmentConfigurer environmentConfigurer;
    private final String jobNamePrefix;
    // incremental 专用 MiniCluster，长跑 job 提交后不等待结束
    private final PipelineMiniClusterManager miniClusterManager;

    public DefaultIncrementalPhaseRunner(PlanContextFactory planContextFactory,
                                         SqlNodeSchemaRegistry sqlNodeSchemaRegistry,
                                         List<ServerTopicInfo> topicInfos,
                                         RuntimeTableSchemaResolver runtimeTableSchemaResolver,
                                         String kafkaBootstrapServers,
                                         String serverId,
                                         PipelineMiniClusterManager miniClusterManager,
                                         EnvironmentConfigurer environmentConfigurer,
                                         String jobNamePrefix) {
        // 保存 PlanContextFactory
        this.planContextFactory = Objects.requireNonNull(planContextFactory, "planContextFactory");

        // 保存 SQL schema registry
        this.sqlNodeSchemaRegistry = Objects.requireNonNull(sqlNodeSchemaRegistry, "sqlNodeSchemaRegistry");

        // 保存 topic 列表快照
        this.topicInfos = topicInfos == null ? new ArrayList<>() : new ArrayList<>(topicInfos);

        // 保存运行时 schema resolver
        this.runtimeTableSchemaResolver = Objects.requireNonNull(runtimeTableSchemaResolver, "runtimeTableSchemaResolver");

        // 保存 Kafka 地址
        this.kafkaBootstrapServers = requireText(kafkaBootstrapServers, "kafkaBootstrapServers");

        // 保存 serverId / pipelineId
        this.serverId = requireText(serverId, "serverId");

        // 保存 incremental MiniCluster
        this.miniClusterManager = Objects.requireNonNull(miniClusterManager, "miniClusterManager");

        // 保存环境配置回调
        this.environmentConfigurer = environmentConfigurer == null ? EnvironmentConfigurer.noop() : environmentConfigurer;

        // 保存 job 名称前缀
        this.jobNamePrefix = requireText(jobNamePrefix, "jobNamePrefix");
    }
//
//    public DefaultIncrementalPhaseRunner(PlanContextFactory planContextFactory,
//                                         SqlNodeSchemaRegistry sqlNodeSchemaRegistry,
//                                         List<ServerTopicInfo> topicInfos,
//                                         RuntimeTableSchemaResolver runtimeTableSchemaResolver,
//                                         String kafkaBootstrapServers,
//                                         int flinkUiPort,
//                                         String serverId,
//                                         EnvironmentConfigurer environmentConfigurer,
//                                         String jobNamePrefix) {
//        this.planContextFactory = Objects.requireNonNull(planContextFactory, "planContextFactory");
//        this.sqlNodeSchemaRegistry = Objects.requireNonNull(sqlNodeSchemaRegistry, "sqlNodeSchemaRegistry");
//        this.topicInfos = topicInfos == null ? new ArrayList<>() : new ArrayList<>(topicInfos);
//        this.runtimeTableSchemaResolver = Objects.requireNonNull(runtimeTableSchemaResolver, "runtimeTableSchemaResolver");
//        this.kafkaBootstrapServers = requireText(kafkaBootstrapServers, "kafkaBootstrapServers");
//        this.flinkUiPort = flinkUiPort;
//        this.serverId = requireText(serverId, "serverId");
//        this.environmentConfigurer = environmentConfigurer == null ? EnvironmentConfigurer.noop() : environmentConfigurer;
//        this.jobNamePrefix = requireText(jobNamePrefix, "jobNamePrefix");
//    }



    protected Configuration buildConfiguration() {
        // 使用 incremental MiniCluster 的 job 配置
        Configuration configuration = miniClusterManager.createJobConfiguration();

        // 当前先保持默认并行度为 1
        configuration.setString("parallelism.default", "1");

        return configuration;
    }



    private void registerSourceViews(StreamExecutionEnvironment env,
                                     StreamTableEnvironment tableEnv,
                                     SqlScopeAliasRegistry aliasRegistry,
                                     Long snapshotScn) {
        if (topicInfos.isEmpty()) {
            throw new IllegalStateException("topicInfos is empty");
        }

        for (ServerTopicInfo topicInfo : topicInfos) {
            if (topicInfo == null) {
                continue;
            }

            String topic = requireText(topicInfo.getTopic(), "topic");
            String groupId = "flink-k2db-" + serverId + "-" + topic;

            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaBootstrapServers)
                    .setGroupId(groupId)
                    .setTopics(topic)
                    .setDeserializer(new ScnFilteringMessageDeserializer(topic, snapshotScn))
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .build();

            DataStreamSource<String> kafkaDataStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "kafka-source-" + topic
            );

            RuntimeTableSchema schema = runtimeTableSchemaResolver.resolve(topicInfo);
            Schema tableSchema = schema.getSchema();

            SingleOutputStreamOperator<Row> rowStream = kafkaDataStream
                    .map(message -> OracleCdcRowMapper.parseKafkaMessage(message, schema.getColumns()))
                    .returns(schema.getRowTypeInfo())
                    .name("cdc-row-mapper-" + topic);

            String viewName = buildUniqueViewName(topicInfo);
            tableEnv.createTemporaryView(
                    viewName,
                    tableEnv.fromChangelogStream(rowStream, tableSchema, ChangelogMode.all())
            );

            aliasRegistry.register(
                    topicInfo.getBusinessDataSourceId(),
                    topicInfo.getSchemaName(),
                    topicInfo.getTableName(),
                    viewName
            );

            logger.info("registered incremental source view. topic={}, viewName={}, schema={}, table={}",
                    topic,
                    viewName,
                    topicInfo.getSchemaName(),
                    topicInfo.getTableName());
        }
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
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }

    private String buildUniqueViewName(ServerTopicInfo topicInfo) {
        String raw = String.join("_",
                safe(topicInfo.getBusinessDataSourceId()),
                safe(topicInfo.getSchemaName()),
                safe(topicInfo.getTableName())
        ).toLowerCase(Locale.ROOT);

        String sanitized = raw.replaceAll("[^a-z0-9_]", "_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "v_" + sanitized;
        }
        return sanitized;
    }

    private String safe(String raw) {
        return raw == null ? "null" : raw.trim();
    }

    @FunctionalInterface
    public interface RuntimeTableSchemaResolver {
        RuntimeTableSchema resolve(ServerTopicInfo topicInfo);
    }

    @FunctionalInterface
    public interface EnvironmentConfigurer {
        void configure(Configuration configuration, StreamExecutionEnvironment env) throws Exception;

        static EnvironmentConfigurer noop() {
            return (configuration, env) -> {
            };
        }
    }

    @Override
    public void run(List<RulePlan> plans, List<PipelinePlanRunState> states) throws Exception {
        // 校验输入
        validate(plans, states);

        // 创建 incremental job 配置
        Configuration configuration = buildConfiguration();

        // 每次增量提交创建自己的 env，只用于构建 JobGraph
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 应用 KafkaPipeline 注入的 checkpoint / state backend 等环境配置
        environmentConfigurer.configure(configuration, env);

        // 创建当前 job 的 TableEnv
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 SQL 表别名注册表
        SqlScopeAliasRegistry aliasRegistry = new SqlScopeAliasRegistry();

        // 先按所有 plan 的最小 snapshotScn 做 Kafka source 粗过滤
        Long minSnapshotScn = resolveMinSnapshotScn(states);

        // 注册 Kafka source 对应的临时视图
        registerSourceViews(env, tableEnv, aliasRegistry, minSnapshotScn);

        // 将所有增量 plan 装配进同一个 incremental job
        for (int i = 0; i < plans.size(); i++) {
            RulePlan plan = plans.get(i);
            PipelinePlanRunState state = states.get(i);

            // 构建当前 plan 的增量上下文
            PlanContext context = planContextFactory.createIncrementalContext(
                    PlanContextFactory.IncrementalContextSpec.builder()
                            .env(env)
                            .tableEnv(tableEnv)
                            .kafkaBootstrapServers(kafkaBootstrapServers)
                            .planId(state.getPlanId())
                            .runId(state.getRunId())
                            .snapshotScn(state.getSnapshotScn())
                            .sqlScopeAliasRegistry(aliasRegistry)
                            .sqlNodeSchemaRegistry(sqlNodeSchemaRegistry)
                            .build()
            );

            // 构建增量节点工厂
            PlanNodeFactory factory = PipelinePlanNodeFactoryBuilder.build(context);

            // 执行 plan，实际是构建当前 env 的 JobGraph
            PlanExecutor executor = new PlanExecutor(factory);
            executor.execute(plan, context);
        }

        // 构建增量 job 名称
        String jobName = buildJobName(plans, states);

        // 增量是长跑 job，只提交，不等待完成
        logger.info("submit incremental flink job to incremental MiniCluster, planCount={}, jobName={}",
                plans.size(), jobName);

        // 提交后立即返回，让 coordinator 可以更新 INCREMENTAL_RUNNING
        miniClusterManager.submit(env, jobName);
    }

    /**
     * 校验总 incremental job 的输入。
     */
    private void validate(List<RulePlan> plans, List<PipelinePlanRunState> states) {
        if (plans == null || plans.isEmpty()) {
            throw new IllegalArgumentException("plans is empty");
        }
        if (states == null || states.isEmpty()) {
            throw new IllegalArgumentException("states is empty");
        }
        if (plans.size() != states.size()) {
            throw new IllegalArgumentException(
                    "plans and states size mismatch, plans=" + plans.size() + ", states=" + states.size()
            );
        }

        for (int i = 0; i < plans.size(); i++) {
            RulePlan plan = plans.get(i);
            PipelinePlanRunState state = states.get(i);

            Objects.requireNonNull(plan, "plan");
            Objects.requireNonNull(state, "state");

            requireText(plan.getPlanId(), "plan.planId");
            requireText(state.getPlanId(), "state.planId");
            requireText(state.getRunId(), "state.runId");

            if (!plan.getPlanId().trim().equals(state.getPlanId().trim())) {
                throw new IllegalArgumentException(
                        "planId mismatch, plan.planId=" + plan.getPlanId()
                                + ", state.planId=" + state.getPlanId()
                );
            }

            if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
                throw new IllegalArgumentException("plan nodes is empty, planId=" + plan.getPlanId());
            }

            if (state.getSnapshotScn() == null) {
                throw new IllegalArgumentException("snapshotScn is null, planId=" + state.getPlanId());
            }
        }
    }

    /**
     * 为总 incremental job 选择一个统一的 source 过滤下界。
     *
     * 当前先取所有 state 中最小的 snapshotScn。
     * 这样可以保证不会把某些新 plan 需要的数据提前过滤掉。
     */
    private Long resolveMinSnapshotScn(List<PipelinePlanRunState> states) {
        if (states == null || states.isEmpty()) {
            throw new IllegalArgumentException("states is empty");
        }

        Long minScn = null;
        for (PipelinePlanRunState state : states) {
            if (state == null || state.getSnapshotScn() == null) {
                continue;
            }

            if (minScn == null || state.getSnapshotScn() < minScn) {
                minScn = state.getSnapshotScn();
            }
        }

        if (minScn == null) {
            throw new IllegalArgumentException("cannot resolve min snapshotScn from states");
        }

        return minScn;
    }

    /**
     * 构造总 incremental job 名称。
     *
     * 这里不再把所有 planId 全拼进去，避免作业名过长。
     * 只保留 plan 数量和第一个 planId 作为标识。
     */
    protected String buildJobName(List<RulePlan> plans, List<PipelinePlanRunState> states) {
        String firstPlanId = "unknown";
        if (plans != null && !plans.isEmpty() && plans.get(0) != null) {
            String value = plans.get(0).getPlanId();
            if (value != null && !value.trim().isEmpty()) {
                firstPlanId = safe(value);
            }
        }

        return jobNamePrefix + "-" + plans.size() + "plans-" + firstPlanId;
    }
}
