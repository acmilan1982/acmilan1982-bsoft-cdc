package com.bsoft.pipeline.k2k;


import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.dataSource.DataSourceFactory;
import com.bsoft.common.dbmeta.*;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.common.service.TopicInfoService;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import com.bsoft.pipeline.api.SyncClientHTTPClient;
import com.bsoft.pipeline.flink.PipelineMiniClusterManager;
import com.bsoft.pipeline.plan.runtime.*;
import com.bsoft.pipeline.plan.scn.OracleSnapshotScnProvider;

import com.bsoft.pipeline.plan.state.JdbcPipelinePlanRunStateRepository;

import com.bsoft.pipeline.plan.state.PipelinePlanRunStateRepository;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;

import java.time.Duration;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;


/**
 * KafkaPipeline 是 sync-pipeline 模块的服务端总入口。
 *
 * 当前版本的职责分为两层：
 * 1. 启动前准备：加载配置、构建执行环境、预取表结构和 SQL 节点 schema。
 * 2. 执行编排：把 RulePlan 交给新的 coordinator / runner / context 主链执行。
 *
 * 注意：
 * - 本类不再自己维护 snapshot / incremental 的状态机。
 * - plan phase 状态统一交给 PlanExecutionCoordinator 管理。
 * - plan 运行状态通过 JdbcPipelinePlanRunStateRepository 持久化到配置库。
 */
@Component

public class KafkaPipeline extends CDCJob {

    private static final Logger logger = LogManager.getLogger(KafkaPipeline.class);

    // Pipeline 配置
    @Autowired
    protected PipeLineInfo pipeLineInfo;
    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;

    // Kafka 工具类
    @Autowired
    private KafkaUtils kafkaUtils;

    // Flink checkpoint / state backend 相关配置
    @Autowired
    private StateBackendProperties stateBackendProperties;

    // 所有业务库配置
    private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;

    // 所有目标库配置
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

//    // 订阅关系注册表
    private SubscriberMappingRegistry subscriberMappingRegistry;

    // 用于向探针端请求表结构、SQL schema
    private SyncClientHTTPClient syncClientHTTPClient;

    // 缓存业务表字段结构
    // key: clientId + businessDataSourceId + schemaName
    // value: 表字段元数据，比如类型长度等
    private final Map<String, OracleColumnsQueryResult> tableSchemaCache = new ConcurrentHashMap<>();

    // 缓存规则计划
    private List<RulePlan> rulePlans = Collections.emptyList();

    // 缓存 SQL 节点返回的字段结构
    // key: planId|nodeId
    private final Map<String, OracleColumnsQueryResult> sqlNodeSchemaCache = new ConcurrentHashMap<>();

    private SqlNodeSchemaRegistry sqlNodeSchemaRegistry;

    // plan 运行状态仓库
    private PipelinePlanRunStateRepository runStateRepository;

    // 统一构建 snapshot / incremental 场景下的 PlanContext
    private PlanContextFactory planContextFactory;

    // snapshot SCN 提供器
    private OracleSnapshotScnProvider snapshotScnProvider;

    @Autowired
    private TopicInfoService topicInfoService;

    // snapshot 阶段专用 MiniCluster
    private PipelineMiniClusterManager snapshotMiniClusterManager;

    // incremental 阶段专用 MiniCluster
    private PipelineMiniClusterManager incrementalMiniClusterManager;

    /**
     * 初始化 KafkaPipeline 这个服务端入口对象本身。
     *
     * 这个方法只负责初始化“与本次 JVM 生命周期相关、但不依赖配置库运行态数据”的基础组件。
     *
     * 这里应该初始化的内容：
     * 1. 面向 client 端的 HTTP 调用客户端；
     * 2. PlanContextFactory；
     * 3. 若干运行期字段的初始状态。
     *
     * 这里不应该初始化的内容：
     * - 不读取配置库中的 RulePlan；
     * - 不创建 runStateRepository；
     * - 不构建 coordinator；
     * - 不生成 executionPlan；
     * - 不拉取表结构；
     * - 不启动任何 Flink 作业。
     *
     * 为什么要这样收敛职责：
     * - init() 是对象级初始化，不是“开始运行”；
     * - 一旦把运行态逻辑放进 init()，后面 prepare / initServerExecutionEnvironment 的边界就会混乱；
     * - 同时也更利于后续支持 reload、重试、测试注入。
     *
     * 重要说明：
     * 为了让每一轮执行都能拿到干净的 SQL 节点 schema 注册表，
     * 建议把字段
     *
     *     private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry = new SqlNodeSchemaRegistry();
     *
     * 改成
     *
     *     private SqlNodeSchemaRegistry sqlNodeSchemaRegistry;
     *
     * 然后在这里或 prepareForJobExecution() 里重建。
     *
     * 否则同一个 Spring 单例对象跨多轮执行时，registry 容易残留旧 plan 的元数据。
     */
    public void init() throws Exception {
        logger.info("初始化 kafka pipeline 基础组件, pipelineId={}",
                pipeLineInfo == null ? null : pipeLineInfo.getPipelineId());

        /*
         * 第 1 步：初始化面向 client 端的 HTTP 客户端。
         *
         * 这个客户端后续会用于：
         * - 拉取业务表 schema；
         * - 拉取 SQL 节点输出 schema；
         * - 其他可能的探针/元数据请求。
         *
         * 这里选择在 init() 阶段完成，是因为：
         * - 它是 KafkaPipeline 的基础设施依赖；
         * - 它不依赖 prepare 阶段加载出的 RulePlan / topic / registry；
         * - 后面 initServerExecutionEnvironment() 可以直接复用。
         */
        this.syncClientHTTPClient = new SyncClientHTTPClient(this.pipeLineInfo);
        this.syncClientHTTPClient.init();

        /*
         * 第 2 步：初始化 PlanContextFactory。
         *
         * 这个工厂负责在 snapshot / incremental 两种场景下，
         * 统一构建 PlanContext，并注入运行时参数和服务对象。
         *
         * 它本身是无状态装配器，因此适合作为 KafkaPipeline 的基础组件长期持有。
         */
        this.planContextFactory = new PlanContextFactory();

        /*
         * 第 3 步：初始化本轮执行相关字段的默认值。
         *
         * 这里不是做“真实执行前准备”，只是把对象状态收敛到一个可预期的初始态，
         * 避免外部调用者在 init() 之后立刻访问到脏状态。
         *
         * 注意：
         * 真正每轮执行前的清理和重建，仍然应该在 prepareForJobExecution() 里做；
         * 这里更多是对象首次初始化时的默认兜底。
         */
        this.rulePlans = Collections.emptyList();
        this.runStateRepository = null;
        this.snapshotScnProvider = null;

        /*
         * topicInfoRegistry 和 executionPlan 都来自 CDCJob 父类。
         * 因为 KafkaPipeline 是单例，显式重建能减少“对象刚初始化时残留旧值”的风险。
         */
        this.topicInfoRegistry = new TopicInfoRegistry();
        this.executionPlan = null;

        /*
         * 各类 registry / cache 都属于“运行期可重建状态”，
         * 在 init() 里先置空或清空，保持对象初始状态明确。
         */
        this.businessDataSourceConfigRegistry = null;
        this.targetDataSourceConfigRegistry = null;
//        this.subscriberMappingRegistry = null;
        this.tableSchemaCache.clear();
        this.sqlNodeSchemaCache.clear();

        /*
         * SQL 节点 schema 注册表建议不要做成 final 常驻对象，
         * 否则多轮执行之间可能残留旧 plan 的 schema。
         *
         * 如果你接受前面说的字段修改，这里直接重建一份最干净的 registry。
         */
        this.sqlNodeSchemaRegistry = new SqlNodeSchemaRegistry();

        logger.info("kafka pipeline 基础组件初始化完成");
    }




    /**
     * 启动整个 sync-pipeline 服务端任务。
     *
     * 这版的设计目标是把职责收敛清楚：
     * 1. 这里只负责“启动总流程”；
     * 2. 启动前的配置加载、topic/schema 预热，交给 prepare / init 方法；
     * 3. snapshot / incremental 的 phase 状态推进，全部交给 PlanExecutionCoordinator；
     * 4. 本方法不再直接介入单个 RulePlan 的执行细节。
     *
     * 为什么要这样改：
     * - 当前 KafkaPipeline 里还残留了一套旧的 executePlan/runSnapshot/runIncremental 逻辑，
     *   和新的 PlanExecutionCoordinator 是重复且冲突的；
     * - startJob 作为总入口，应该只做“编排”，不应该再承担 phase 状态机职责；
     * - 这样后续你改 snapshot / incremental 策略时，只需要改 coordinator / runner，
     *   不会再回头改 KafkaPipeline。
     */
    public void startJob() {
        try {
            // 第一步：加载配置库中的运行时配置，并刷新本地缓存。
            // 这里会重新读取 RulePlan、业务库配置、目标库配置、订阅关系等。
            prepareForJobExecution();

            // 第二步：基于刚刚加载的配置，构造本次运行真正需要的执行环境。
            // 包括 topic 注册、等待 topic 就绪、向 client 端拉取表结构和 SQL schema。
            initServerExecutionEnvironment();

            // 第三步：如果当前根本没有有效 RulePlan，直接结束，不继续往下跑。
            // 这里不抛错，因为“没有计划可执行”本身不一定是异常场景。
            if (rulePlans == null || rulePlans.isEmpty()) {
                logger.info("no rule plans found, skip pipeline execution");
                return;
            }

            // 第四步：构建统一的 plan 执行协调器。
            PlanExecutionCoordinator coordinator = buildPlanExecutionCoordinator();

            // 第五步：统一交给 coordinator 执行全部 RulePlan。
            // 后续如果要做：
            // - 逐 plan 隔离失败
            // - snapshot 串行 / incremental 异步提交
            // - 更细粒度的状态持久化
            // 也都应该在 coordinator 层处理，而不是在这里展开。
            coordinator.executeAll(rulePlans);

            logger.info("pipeline execution finished, totalPlans={}", rulePlans.size());
        } catch (Exception e) {
            // 这里保留一层总兜底，把顶层启动异常转成明确的运行失败。
            // 不建议只 e.printStackTrace()，要带上上下文日志，方便排查。
            logger.error("failed to start sync pipeline, pipelineId={}, serverId={}",
                    pipeLineInfo == null ? null : pipeLineInfo.getPipelineId(),
                    serverInfo == null ? null : serverInfo.getServerId(),
                    e);
            throw new RuntimeException("failed to start sync pipeline", e);
        }
    }


    /**
     * 构建当前这轮执行所使用的 PlanExecutionCoordinator。
     *
     * 这个方法是 KafkaPipeline 和新执行主链之间的“装配入口”。
     * 它的职责非常明确：
     * 1. 创建状态仓库；
     * 2. 创建 snapshot SCN 提供器；
     * 3. 创建 snapshot runner；
     * 4. 创建 incremental runner；
     * 5. 把这些组件组装成统一的 coordinator 返回。
     *
     * 为什么要保留这样一个组装方法：
     * - KafkaPipeline 是服务端入口，掌握最完整的运行期上下文：
     *   serverInfo、kafkaUtils、schema cache、topicInfoRegistry、stateBackendProperties 等；
     * - coordinator 自己不应该知道这些对象是从哪里来的；
     * - runner 也不应该自己去全局拿依赖，而应该由这里统一注入。
     *
     * 这版相比旧实现，重点解决三个问题：
     * 1. 明确这是“本轮执行时临时组装”的对象，而不是常驻全局单例；
     * 2. 在组装前做依赖校验，避免把 null 依赖一路传到更深层；
     * 3. incremental runner 明确只消费“本轮预热后得到的 topic 列表和 schema 缓存”。
     */
    private PlanExecutionCoordinator buildPlanExecutionCoordinator() {


        /*
         * 第 1 步：创建运行状态仓库。
         *
         * 这里选择每轮执行重新构建 repository，而不是在 init() 时提前创建，
         * 原因很简单：
         * - runStateRepository 依赖配置库 DataSource；
         * - 配置库连接能力属于“运行期依赖”，不是 bean 构造期常量；
         * - 如果以后支持配置源切换或 reload，这里重建会更稳妥。
         *
         * 同时把它挂回字段，便于 KafkaPipeline 其他辅助方法复用。
         */
        this.runStateRepository = buildRunStateRepository();








        /*
         * 第 2 步：创建 snapshot SCN provider。
         *
         * 这个组件的职责是：
         * - 在 snapshot 阶段开始前，针对某个 RulePlan 固定一个一致性的基线 SCN；
         * - coordinator 只关心“需要时调用”，不关心底层怎么采集。
         *
         * 这里也选择“按本轮执行重建”，避免持有过期业务源配置。
         */
        this.snapshotScnProvider = new OracleSnapshotScnProvider(businessDataSourceConfigRegistry);




         // 创建 snapshot 阶段专用 MiniCluster
        this.snapshotMiniClusterManager = new PipelineMiniClusterManager(
                "pipeline-snapshot",
                pipeLineInfo.getSnapshotFlinkUiPort(),
                4,
                8
        );

        // 启动 snapshot MiniCluster
        try {
            this.snapshotMiniClusterManager.start();
        } catch (Exception e) {
            throw new IllegalStateException("failed to start pipeline snapshot MiniCluster", e);
        }

         // 创建 snapshot runner
        SnapshotPhaseRunner snapshotPhaseRunner = new DefaultSnapshotPhaseRunner(
                planContextFactory,
                businessDataSourceConfigRegistry,
                sqlNodeSchemaRegistry,
                this.kafkaUtils.getKafkaBootstrapServers(),
                snapshotMiniClusterManager
        );

        // 创建 incremental 阶段专用 MiniCluster
        this.incrementalMiniClusterManager = new PipelineMiniClusterManager(
                "pipeline-incremental",
                pipeLineInfo.getIncrementalFlinkUiPort(),
                1,
                8
        );

        // 启动 incremental MiniCluster
        try {
            this.incrementalMiniClusterManager.start();
        } catch (Exception e) {
            // incremental 启动失败时关闭已启动的 snapshot MiniCluster
            closeQuietly(this.snapshotMiniClusterManager, "snapshot MiniCluster");
            throw new IllegalStateException("failed to start pipeline incremental MiniCluster", e);
        }

        // 准备 incremental runner 需要的 topic 列表
        List<ServerTopicInfo> incrementalTopicInfos = topicInfoRegistry.getServerTopicInfoList().stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // 创建 incremental runner
        IncrementalPhaseRunner incrementalPhaseRunner = new DefaultIncrementalPhaseRunner(
                planContextFactory,
                sqlNodeSchemaRegistry,
                incrementalTopicInfos,
                new DefaultRuntimeTableSchemaResolver(tableSchemaCache),
                kafkaUtils.getKafkaBootstrapServers(),
                this.pipeLineInfo.getPipelineId(),
                incrementalMiniClusterManager,
                this::configureIncrementalEnvironment,
                "sync-pipeline-incremental"
        );

        // 组装 coordinator
        return new PlanExecutionCoordinator(
                runStateRepository,
                snapshotScnProvider,
                snapshotPhaseRunner,
                incrementalPhaseRunner,
                snapshotMiniClusterManager
        );
    }


    /**
     * 构建当前服务端所使用的 plan 运行状态仓库。
     *
     * 当前实现选择 JDBC 作为持久化介质，目的是把每个 RulePlan 的运行状态
     * 持久化到配置库中，而不是只放在内存里。
     *
     * 为什么必须持久化：
     * 1. 服务端重启后，仍然需要知道某个 plan 上次跑到了哪个 phase；
     * 2. snapshot SCN 一旦采集完成，后续重启时不能随便重新采，否则会破坏快照/增量衔接；
     * 3. 如果 snapshot 或 incremental 失败，需要保留最后一次错误信息供排查；
     * 4. 后续如果做管理页面或运维查询，也需要一个稳定的状态来源。
     *
     * 为什么 repository 在这里构建，而不是在 init() 时构建：
     * - runStateRepository 依赖 configDataSource；
     * - configDataSource 属于运行期配置，应该在 prepare 之后再使用；
     * - 这样也便于后面扩展为不同实现，例如：
     *   JdbcPipelinePlanRunStateRepository / InMemoryPipelinePlanRunStateRepository / Redis 版等。
     */
    private PipelinePlanRunStateRepository buildRunStateRepository() {


        /*
         * 第 2 步：基于配置库配置创建 DataSource。
         *
         * 这里的 DataSourceFactory 负责把抽象的数据源配置转换成真正可用的 JDBC DataSource。
         * 注意，这里只是创建 repository 所需的数据访问入口，
         * 并不意味着此时已经执行了具体的 SQL。
         */
        DataSource dataSource = DataSourceFactory.getDataSource(this.pipeLineInfo.getConfigDataSourceConfig());
        if (dataSource == null) {
            throw new IllegalStateException("failed to create config DataSource for plan run state repository");
        }

        /*
         * 第 3 步：创建 JDBC 版 run-state repository。
         *
         * 当前默认表名由 JdbcPipelinePlanRunStateRepository 内部决定，
         * 一般是 CDC_PIPELINE_RUN_STATE。
         *
         * 如果后面你想做得更灵活，建议再往前走一步：
         * - 通过 application.yml 或 serverInfo 配置状态表名；
         * - 这里读取配置后传入 repository 构造器；
         * - 这样不同环境可以更方便隔离状态表。
         */
        PipelinePlanRunStateRepository repository = new JdbcPipelinePlanRunStateRepository(dataSource);

        logger.info("plan run state repository created, repositoryType={}, dataSourceType={}",
                repository.getClass().getSimpleName(),
                dataSource.getClass().getName());

        return repository;
    }

    /**
     * 为本次服务端任务执行做“运行前准备”。
     *
     * 这个方法只做两类事情：
     * 1. 从配置库重新加载本次运行依赖的静态/半静态配置；
     * 2. 重置当前 KafkaPipeline 实例里和“本轮执行上下文”相关的内存状态。
     *
     * 不在这里做的事情：
     * - 不创建 Flink env / tableEnv；
     * - 不启动 snapshot / incremental；
     * - 不等待 Kafka topic；
     * - 不向 client 拉取表结构；
     * - 不构建 PlanExecutionCoordinator。
     *
     * 为什么要这样拆：
     * - prepareForJobExecution() 的职责应该是“准备数据”，而不是“开始执行”；
     * - 这样调用顺序会非常清晰：
     *   prepareForJobExecution()
     *   -> initServerExecutionEnvironment()
     *   -> buildPlanExecutionCoordinator()
     *   -> coordinator.executeAll(...)
     *
     * 相比当前旧实现，这个版本额外解决了两个问题：
     * 1. 避免复用旧缓存
     *    当前 KafkaPipeline 是 Spring 单例 Bean，如果以后支持 reload / 重试 / 定时重启，
     *    那么 rulePlans、schemaCache、topicInfoRegistry 这些状态如果不先清空，会发生脏数据叠加；
     * 2. 明确“配置加载阶段”的失败边界
     *    一旦配置库缺数据、配置非法，应该在这里就尽早失败，而不是等到后面的 runner 才暴露。
     */
    public void prepareForJobExecution() throws Exception {

        this.rulePlans = Collections.emptyList();
        this.tableSchemaCache.clear();
        this.sqlNodeSchemaCache.clear();
        this.topicInfoRegistry = new TopicInfoRegistry();

        this.sqlNodeSchemaRegistry = new SqlNodeSchemaRegistry();

        // 1.向配置库注册当前server
        configFetcher.registerPipeLineIfAbsent(this.pipeLineInfo);

        // 刷新活跃 client 缓存。
        configFetcher.refreshActiveClientCache();

        // 从配置库重新加载 RulePlan 列表。
        List<RulePlan> loadedPlans = configFetcher.loadRulePlans();

        // 3.读取目标库的相关连接信息, 设置到serverInfo，注意，现在不能创建datasource，等算子分配到集群后，才能创建serverInfo
        // 一同处理DATA_TO_SOURCE_ID中逗号分隔 DATA_TO_SOURCE_ID，并去重，保证每个目标库只有一个 TargetDataSourceConfig
        // 只读取 有效订阅关系中 的目标数据库，
        targetDataSourceConfigRegistry = configFetcher.generateTargetDataSourceConfigRegistry();


        // 4.读取所有订阅关系,最终都解析成 一对一 的关系
        subscriberMappingRegistry = configFetcher.generateSubscriberMappingRegistry();

        // 5.读取所有 有效的业务库 配置
        businessDataSourceConfigRegistry = configFetcher.generateBusinessDataSourceConfigRegistry();


        this.rulePlans = loadedPlans == null ? Collections.emptyList() : loadedPlans;



    }

    /**
     * 基于 prepareForJobExecution() 已加载好的配置，初始化“本轮真正要执行”的服务端运行环境。
     *
     * 这个方法的职责是把“配置态信息”进一步加工成“执行态信息”：
     * 1. 把订阅关系、业务源、目标源之间的引用关系串起来；
     * 2. 生成当前服务端可执行的 executionPlan；
     * 3. 生成本轮需要消费的 Kafka topic 列表；
     * 4. 等待这些 topic 在 Kafka 中实际可用；
     * 5. 向 client 端预拉取表结构和 SQL 节点 schema，填充本地缓存；
     * 6. 标记服务端启动时间，便于后续日志/探针观测。
     *
     * 不在这里做的事情：
     * - 不直接执行 RulePlan；
     * - 不直接推进 snapshot / incremental phase；
     * - 不创建 PlanExecutionCoordinator；
     * - 不提交 Flink 作业。
     *
     * 设计原则：
     * - 这里是“执行前预热阶段”；
     * - 所有后面 runner 一定会用到、而且一旦缺失就会在运行时炸掉的元数据，
     *   都应该尽量在这里预先准备好；
     * - 这样能把错误尽量提前暴露，避免 Flink job 提交后才发现 schema 缺失。
     */
    public void initServerExecutionEnvironment() throws Exception {

        // 1.通过 订阅关系，把所有对象关联起来，并进行必要的初始化
        postProcessSubscriberMappingRegistry();

        // 2.生成当前 服务端 的执行计划，
        executionPlan = configFetcher.batchGenerateTableExecutionPlan(subscriberMappingRegistry);

//        // 3. 初始化serverController，该类用于外部通信，比如建表
//        serverController.init(businessDataSourceConfigRegistry,targetDataSourceConfigRegistry);
//        serverController.startWorkers();
//        serverController.buildDorisBizAttrBySqlNodeId(rulePlans);


        // 4. 生成需要读取主题信息，包括每个主题的数据最终写入哪个目标库的哪个表
        topicInfoRegistry.registerServerTopicInfo(
                topicInfoService.generateServerTopicInfo(
                        executionPlan,
                        businessDataSourceConfigRegistry,
                        targetDataSourceConfigRegistry
                )
        );




        // 5. 检查需要监听的主题是否已经创建，还没创建则等待创建
        List<String> topics = topicInfoRegistry.getServerTopicInfoList().stream()
                .map(ClientTopicInfo::getTopic)
                .collect(Collectors.toList());
        logger.info("topics-->{}",topics.size());
        if(kafkaUtils == null){
            logger.info("kafkaUtils is null");
        }else{
            logger.info("kafkaUtils is  not null");
        }

        kafkaUtils.waitUntilTopicExists(topics);


//        // 6. 按需创建 日志，监控 主题
//        kafkaUtils.createKafkaTopicsIfNotExists(Arrays.asList(this.serverInfo.getServerLogTopic(), this.serverInfo.getMonitorMetricTopic()));



        // 8. 注册flink sql对应的主题
//        this.pipelineSqlSinkTopicResolver = new PipelineSqlSinkTopicResolver(configFetcher,targetDataSourceConfigRegistry,tableNameGenerator);
//        List<ServerTopicInfo> sqlSinkTopics = pipelineSqlSinkTopicResolver.resolve(this.rulePlans);
//        topicInfoRegistry.registerServerTopicInfo(sqlSinkTopics);

        // final. 设置一个启动时间
        this.pipeLineInfo.notifyClientStart();


        logger.info("当前 job 启动,启动时间: {}", this.pipeLineInfo.printStartTime());


        // 向 client 端预拉取源表结构，并放入 tableSchemaCache
        requestTableSchemaFromClient();

        // 向 client 端预拉取 SQL 节点的结果 schema。
        requestSqlNodeSchemaFromClient();

        /*
         * 第 7 步：通知 serverInfo 记录启动时间。
         *
         * 这属于观测和状态记录，不参与执行逻辑本身。
         * 但保留这一步有价值：
         * - 日志里能看到本轮服务端实际启动时间；
         * - 下游如果有探针/状态页，也可以读取这个时间。
         */
        pipeLineInfo.notifyClientStart();

    }

    /**
     * 把 prepare 阶段加载到的各类注册表真正“串起来”，
     * 让 serverInfo、businessDataSourceConfig、targetDataSourceConfig 之间形成完整的运行时对象关系。
     *
     * 这个方法的核心作用不是做业务执行，而是做“对象图装配”：
     * 1. 把订阅关系挂到 serverInfo 上；
     * 2. 把订阅关系挂到每个业务源配置上；
     * 3. 把目标库注册表挂到每个业务源配置上；
     * 4. 调用每个业务源配置的 init()，完成表级元数据初始化。
     *
     * 为什么这个方法必须单独存在：
     * - prepareForJobExecution() 只负责从配置库“读数据”；
     * - 这里负责把这些数据“连起来”；
     * - 这样职责更清晰，也方便以后把装配逻辑进一步下沉到 assembler / builder 中。
     *
     * 当前版本额外做了几件旧实现没有明确处理的事：
     * - 前置依赖检查，避免深层 NPE；
     * - 对业务源列表做空值保护；
     * - 避免把 null 配置对象继续向下传播；
     * - 日志中记录初始化数量，便于排查配置是否完整。
     */
    private void postProcessSubscriberMappingRegistry() {
//        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);


        // 为每个 业务库，配置对应的订阅关系，一个业务库，可能有多个 一对一 的订阅关系
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {
            // 每个业务源，可能有多个 一对一 的订阅关系
            businessDataSourceConfig.setSubscriberMappingList(this.subscriberMappingRegistry.findSubscriberMappingByDataFromSourceId(businessDataSourceConfig));

            // 每个业务源，可能有多个 目标库
//            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 读取业务库中每个需要同步的表的元数据，创建对应的 TableMappingConfig
//            businessDataSourceConfig.init();
        }
//
//        /*
//         * 第 1 步：把订阅关系注册表挂到 serverInfo。
//         *
//         * 这样做的目的是让服务端全局配置对象能够感知当前有哪些订阅关系，
//         * 便于后续 executionPlan 生成、状态展示、调试打印等统一从 serverInfo 读取。
//         *
//         * 这一步本质上是“全局级别装配”。
//         */
//        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);
//
//        /*
//         * 第 2 步：取出所有业务源配置，逐个完成装配与初始化。
//         *
//         * 注意这里不要默认认为 list 一定非空。
//         * 如果配置库里当前没有有效业务源，应该允许平稳返回，
//         * 而不是在 for 循环里直接抛 NPE。
//         */
//        List<BusinessDataSourceConfig> businessConfigs =
//                businessDataSourceConfigRegistry.getBusinessDataSourceConfigList();
//
//        if (businessConfigs == null || businessConfigs.isEmpty()) {
//            logger.warn("no business data source configs found when post-processing subscriber mappings");
//            return;
//        }
//
//        int initializedCount = 0;
//
//        /*
//         * 第 3 步：逐个 businessDataSourceConfig 做运行时装配。
//         *
//         * 每个业务源需要知道两件事：
//         * 1. 它自己关联哪些订阅关系；
//         * 2. 它可能会同步到哪些目标库。
//         *
//         * 只有这两块信息都挂进去，它的 init() 才能正确构建表级映射元数据。
//         */
//        for (BusinessDataSourceConfig businessDataSourceConfig : businessConfigs) {
//            /*
//             * 配置列表里理论上不该出现 null，但实际系统里最好不要盲目信任外部数据。
//             * 这里选择跳过并记录 warn，而不是整批失败。
//             *
//             * 原因：
//             * - 单个脏配置不一定应该把整个服务端初始化直接打死；
//             * - 先把问题暴露在日志里，便于排查配置库脏数据。
//             *
//             * 如果你后面希望更严格，也可以改成直接抛异常。
//             */
//            if (businessDataSourceConfig == null) {
//                logger.warn("skip null businessDataSourceConfig while post-processing subscriber mappings");
//                continue;
//            }
//
////            /*
////             * 3.1 注入订阅关系注册表。
////             *
////             * 一个业务源可能对应多个一对一订阅关系，
////             * 所以后面它在初始化表映射时，需要能够从 subscriberMappingRegistry 中查到自己相关的订阅。
////             */
////            businessDataSourceConfig.setSubscriberMappingRegistry(subscriberMappingRegistry);
////
////            /*
////             * 3.2 注入目标库注册表。
////             *
////             * 一个业务源可能同步到多个目标库，
////             * 所以后续初始化表级映射时，也需要知道目标端有哪些可选配置。
////             */
////            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
//
//            /*
//             * 3.3 执行业务源自身初始化。
//             *
//             * 这里通常会做：
//             * - 扫描该业务源下需要同步的表；
//             * - 构建 table mapping；
//             * - 补全后续 executionPlan 需要的元数据。
//             *
//             * 这是整个服务端运行前非常关键的一步。
//             */
//            businessDataSourceConfig.init();
//            initializedCount++;
//        }
//
//        logger.info("post processed subscriber mappings finished, businessDataSourceCount={}, initializedCount={}",
//                businessConfigs.size(),
//                initializedCount);
    }








    /**
     * 为 incremental 阶段的 Flink 作业补充运行时环境配置。
     *
     * 这个方法的职责非常聚焦：
     * 1. 为本服务端生成稳定的 Flink JobId；
     * 2. 配置增量作业的基础执行参数；
     * 3. 尝试从最近一次可用 checkpoint 恢复；
     * 4. 配置 checkpoint 和状态后端相关参数。
     *
     * 这个方法不负责：
     * - 不创建 env；
     * - 不创建 tableEnv；
     * - 不注册 Kafka source；
     * - 不执行 RulePlan；
     * - 不推进 plan phase。
     *
     * 为什么要单独抽出来：
     * - DefaultIncrementalPhaseRunner 只知道“这里有一个环境配置回调”，
     *   它不应该知道 KafkaPipeline 里关于固定 jobId、checkpoint 目录、恢复策略的细节；
     * - KafkaPipeline 作为服务端入口，掌握 pipeLineInfo、serverInfo、stateBackendProperties，
     *   更适合统一定义增量作业的恢复语义。
     *
     * 当前设计的核心意图：
     * - 增量作业应尽量具备“固定身份 + 可恢复”的特性；
     * - 只要是同一个服务端实例，就尽量复用固定 jobId；
     * - 重启后优先从最近 checkpoint 恢复，避免重复消费或状态丢失。
     */
    private void configureIncrementalEnvironment(Configuration configuration,
                                                 StreamExecutionEnvironment env) throws Exception {

        /*
         * 第 2 步：读取 pipelineId，并生成固定的 Flink JobId。
         *
         * 为什么要固定 jobId：
         * - 对同一个服务端实例来说，增量任务本质上是“同一条长跑作业”；
         * - 固定 jobId 有利于 checkpoint 目录稳定归档；
         * - 重启时也更容易按 jobId 找到历史 checkpoint。
         *
         * 这里使用 pipelineId，而不是随机 UUID，
         * 是因为随机值会让每次重启都变成“一个全新的 Flink 任务身份”，
         * 恢复链路就不稳定了。
         */
        String pipelineId = pipeLineInfo.getPipelineId();
        if (pipelineId == null || pipelineId.trim().isEmpty()) {
            throw new IllegalStateException("pipelineId is blank");
        }

        String jobIdHex = JobIdUtil.generateFixedJobId(pipelineId.trim());

        /*
         * 第 3 步：把固定 JobId 写入 Flink Configuration。
         *
         * 这里使用内部 job-id 配置项，让 Flink 在提交作业时使用稳定身份。
         */
        configuration.setString("$internal.pipeline.job-id", jobIdHex);

        /*
         * 第 4 步：设置 incremental 作业的基础运行参数。
         *
         * 这里保留 parallelism=1 的设计，原因通常是：
         * - 当前链路更偏向强顺序、低并发、便于恢复的 CDC 处理；
         * - 同时很多状态恢复和 topic 消费语义也默认建立在单并发基础上。
         *
         * 后续如果你要提升并行度，这里不能只改成 >1，
         * 还要一起审查：
         * - Kafka topic 分区策略；
         * - SQL / sink 是否允许并行；
         * - checkpoint 恢复语义；
         * - RowKind 和主键更新语义。
         */
        configuration.setString("parallelism.default", "1");

        /*
         * 使用服务端配置中的 Flink UI 端口。
         * 这样本地 Web UI 端口不会和其他作业默认值冲突。
         */
        configuration.setInteger(RestOptions.PORT, serverInfo.getFlinkUiPort());

        /*
         * 第 5 步：配置网络缓冲区相关参数。
         *
         * 这部分是对 Flink 运行内存的补充配置。
         * 保持单独方法封装，便于后续独立调整。
         */
        configureNetworkBufferPool(configuration);

        /*
         * 第 6 步：读取 checkpoint 根目录，并尝试定位当前 jobId 最近一次可恢复的 checkpoint。
         *
         * 恢复策略：
         * - 如果找到了最近的有效 checkpoint（存在 _metadata），则从该路径恢复；
         * - 如果没找到，则按 source 自己的起点策略启动。
         *
         * 注意：
         * 这里恢复的是“增量作业状态”，不是 snapshot 阶段状态；
         * snapshotScn 的控制仍由 run-state repository 和 coordinator 负责。
         */
        String checkpointBasePath = stateBackendProperties.getCheckpointPath();
        if (checkpointBasePath == null || checkpointBasePath.trim().isEmpty()) {
            throw new IllegalStateException("checkpointPath is blank");
        }

        String latestCheckpointPathWithMetadata = findLatestCheckpointPathWithMetadata(
                checkpointBasePath,
                jobIdHex
        );

        /*
         * 第 7 步：如果存在可恢复 checkpoint，则把恢复路径写入配置。
         *
         * 这里沿用 execution.savepoint.path 作为恢复入口，
         * 让 Flink 在提交作业时从最近状态继续运行。
         *
         * 如果没有 checkpoint，不视为错误。
         * 那通常意味着：
         * - 首次启动；
         * - 历史 checkpoint 已清理；
         * - 之前作业未成功留下可恢复状态。
         */
        if (latestCheckpointPathWithMetadata != null && !latestCheckpointPathWithMetadata.trim().isEmpty()) {
            configuration.setString("execution.savepoint.path", latestCheckpointPathWithMetadata);
            logger.info("restore incremental job from checkpoint, pipelineId={}, jobId={}, checkpointPath={}",
                    pipelineId,
                    jobIdHex,
                    latestCheckpointPathWithMetadata);
        } else {
            logger.info("no checkpoint found for incremental job, pipelineId={}, jobId={}, checkpointBasePath={}",
                    pipelineId,
                    jobIdHex,
                    checkpointBasePath);
        }

        /*
         * 第 8 步：把 checkpoint 相关配置真正应用到 env。
         *
         * 这里包括：
         * - checkpoint 周期
         * - state backend
         * - checkpoint storage
         * - timeout / 并发数 / 最小间隔
         * - 非对齐 checkpoint 等
         *
         * 这些属于增量长跑作业的核心容灾参数。
         */
        configureCheckpointing(env);

        logger.info("incremental environment configured, pipelineId={}, jobId={}, flinkUiPort={}",
                pipelineId,
                jobIdHex,
                serverInfo.getFlinkUiPort());
    }

    private void validatePlan(RulePlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan is null");
        }
        if (plan.getPlanId() == null || plan.getPlanId().trim().isEmpty()) {
            throw new IllegalArgumentException("planId is blank");
        }
        if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
            throw new IllegalArgumentException("plan nodes is empty, planId=" + plan.getPlanId());
        }
    }

    private String safeTrim(String value, int maxLength) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.length() <= maxLength) {
            return trimmed;
        }
        return trimmed.substring(0, maxLength);
    }



    /**
     * 配置 Flink 作业的网络内存和进程内存参数。
     *
     * 这个方法的目标不是做“完整的资源管理策略”，
     * 而是给当前 sync-pipeline 这类本地 WebUI 启动方式的 Flink 作业提供一套可预测的基础内存配置。
     *
     * 为什么单独抽这个方法：
     * - 网络内存、进程总内存属于环境级配置，不应该散落在 incremental runner 主逻辑里；
     * - 后续如果要做不同环境下的差异化配置，只需要替换这里；
     * - 同时方法名也能明确表达：这里配的是 Flink runtime 资源，不是业务逻辑。
     *
     * 当前配置策略偏保守：
     * - taskmanager.memory.process.size = 2048m
     * - taskmanager.memory.network.min = 256mb
     * - taskmanager.memory.network.max = 256mb
     *
     * 这代表：
     * 1. 给 TaskManager 进程一个固定总内存预算；
     * 2. 把网络内存钉死为 256MB，避免自动推导在不同环境下波动过大；
     * 3. 优先追求运行稳定和可预测，而不是动态最优。
     *
     * 注意：
     * 这不是一个“放之四海而皆准”的配置。
     * 如果后面 topic 数量、消息大小、并发度明显提升，
     * 这里很可能需要和 checkpoint / state backend / source 并发一起重新评估。
     */
    public void configureNetworkBufferPool(Configuration configuration) throws IOException {
        /*
         * 第 0 步：前置校验。
         *
         * configuration 是外部传入并将用于构建 Flink env 的配置对象。
         * 如果为空，说明调用链本身不成立。
         */
        if (configuration == null) {
            throw new IllegalArgumentException("configuration is null");
        }

        /*
         * 第 1 步：设置 TaskManager 进程总内存。
         *
         * 这里配的是整个进程级别的总内存预算，而不是某个单独组件的内存。
         * 选择 2048m 的意图通常是：
         * - 当前链路大概率运行在单并发、单 JVM、本地 WebUI 模式下；
         * - 2GB 是一个相对保守但不至于太紧的起点；
         * - 对中小规模 CDC 任务通常够用，且便于排查。
         *
         * 如果后续你把 RocksDB state、并发度、topic 数量都拉高，
         * 这个值可能需要同步调大。
         */
        configuration.setString("taskmanager.memory.process.size", "2048m");

        /*
         * 第 2 步：固定网络内存下限和上限。
         *
         * 把 min 和 max 都设置为同一个值，等价于把网络内存“钉死”。
         *
         * 这么做的优点：
         * - 不依赖 Flink 自动推导；
         * - 不同机器/环境下结果更稳定；
         * - 对当前这类较固定的单作业场景更容易排查问题。
         *
         * 这么做的代价：
         * - 灵活性较差；
         * - 如果数据吞吐、消息体积、并发度发生明显变化，可能过大或过小。
         */
        configuration.setString("taskmanager.memory.network.min", "256mb");
        configuration.setString("taskmanager.memory.network.max", "256mb");

        /*
         * 第 3 步：记录日志，方便后续排查环境配置问题。
         *
         * 尤其是在多个环境下跑同一套 pipeline 时，
         * 明确打印出最终配置值会比后面去猜环境参数可靠得多。
         */
        logger.info("configured flink network memory, processSize=2048m, networkMin=256mb, networkMax=256mb");
    }

//    public void configureStatisticsWindows(SingleOutputStreamOperator<OperationResult> operationResults ,
//                                           WindowTimeUnit windowTimeUnit, // 绐楀彛闀垮害
//                                           Time time)                     // 绐楀彛姝ラ暱
//            throws IOException {
//
//        // 鍩轰簬鎿嶄綔缁撴灉杩涜绐楀彛缁熻
//        SingleOutputStreamOperator<TableOperationResultStats> statistics = operationResults
//                .keyBy(result -> result.getTableName()) // 鎸夎〃鍚嶅垎缁?
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowTimeUnit.getSecondsValue()), time))  //婊戝姩绐楀彛锛岄暱搴?姝ラ暱鍧囩敱鍙傛暟鎸囧畾
//                .aggregate(
//                        new OperationResultStatsAggregator(windowTimeUnit.getName()),  // 鑱氬悎绐楀彛鍑芥暟
//                        new OperationResultWindowFunction()    // 鍏ㄥ眬绐楀彛鍑芥暟
//                );
//
//
//        //  杈撳嚭缁熻缁撴灉鍒版帶鍒跺彴锛堜繚鐣欏師鏈夊姛鑳斤級;
//        statistics.addSink(new OperationResultStatisticsSink());
//
//        // 杈撳嚭缁熻鏁版嵁鍒皕k锛屾湁闂锛屾殏鏃跺厛涓嶈緭鍑?
////        statistics.addSink(new ZooKeeperStatisticsSink(serverInfo,windowTimeUnit));
//    }

    /**
     * 配置 incremental 长跑作业所需的 Flink checkpoint 策略。
     *
     * 这个方法的目标是给当前 sync-pipeline 的增量任务提供“可恢复、偏稳态”的容灾配置。
     *
     * 为什么这个方法重要：
     * - 当前 incremental job 是 CDC 长跑任务，不是一次性批处理；
     * - 一旦进程重启、网络抖动、机器故障，checkpoint 是恢复链路的核心；
     * - 前面的 configureIncrementalEnvironment() 负责“找到上一次 checkpoint 并决定是否恢复”，
     *   而这个方法负责“当前这次运行如何持续产出新的 checkpoint”。
     *
     * 当前策略大体是：
     * 1. 开启 EXACTLY_ONCE checkpoint；
     * 2. 指定 checkpoint storage；
     * 3. 控制超时、并发、最小间隔；
     * 4. 保留外部化 checkpoint；
     * 5. 容忍一定数量的 checkpoint 失败；
     * 6. 开启非对齐 checkpoint，提高背压场景下的可用性。
     *
     * 注意：
     * 这套参数是“偏保守 + 偏容灾”的取向，不一定是吞吐最优解。
     * 如果后面你追求更高吞吐、更短恢复时间，参数需要结合实际负载重新调。
     */
    public void configureCheckpointing(StreamExecutionEnvironment env) throws IOException {
        /*
         * 第 0 步：前置校验。
         *
         * env 是实际要应用 checkpoint 配置的执行环境。
         * 如果为空，说明调用链本身已经不成立。
         */
        if (env == null) {
            throw new IllegalArgumentException("env is null");
        }

        /*
         * 第 1 步：校验状态后端配置是否可用。
         *
         * checkpoint storage 路径来自 stateBackendProperties，
         * 缺失时不应该继续，因为 checkpoint 会变成“名义开启，实际不可恢复”。
         */
        if (stateBackendProperties == null) {
            throw new IllegalStateException("stateBackendProperties is null");
        }

        String checkpointPath = stateBackendProperties.getCheckpointPath();
        if (checkpointPath == null || checkpointPath.trim().isEmpty()) {
            throw new IllegalStateException("checkpointPath is blank");
        }

        logger.info("start configuring checkpointing, checkpointPath={}", checkpointPath);

        /*
         * 第 2 步：开启 checkpoint，并指定语义为 EXACTLY_ONCE。
         *
         * 这里选择 EXACTLY_ONCE 的原因：
         * - 当前链路本质上在做 CDC 增量同步，恢复后尽量减少重复和丢失；
         * - 非对齐 checkpoint 也要求在 EXACTLY_ONCE 下使用。
         *
         * 当前周期设置为 3 小时。
         * 这是一个明显偏大的周期，反映的是“降低 checkpoint 频率、优先稳态运行”的思路。
         *
         * 这意味着：
         * - 优点：checkpoint 压力小，对作业影响较小；
         * - 缺点：故障恢复时，回退窗口可能较大。
         *
         * 如果你后面希望恢复更及时，可以把周期明显调短。
         */
        env.enableCheckpointing(3 * 60 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);

        /*
         * 第 3 步：拿到 checkpoint 配置对象，后续细项都从这里设置。
         */
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        /*
         * 第 4 步：配置 state backend 和 checkpoint storage。
         *
         * 当前代码使用 HashMapStateBackend + 外部 checkpoint storage 路径。
         *
         * 这套组合的含义是：
         * - 作业内存态由 HashMapStateBackend 管理；
         * - checkpoint 实际持久化到外部存储路径。
         *
         * 注意：
         * 这并不等价于 RocksDB 增量状态后端。
         * 如果后面状态规模明显变大，HashMapStateBackend 可能不再合适。
         */
        env.setStateBackend(new HashMapStateBackend());
        checkpointConfig.setCheckpointStorage(checkpointPath);

        /*
         * 第 5 步：设置 checkpoint 超时时间。
         *
         * 当前配置为 1 小时。
         * 含义是：
         * - 如果某次 checkpoint 超过 1 小时仍未完成，则认定失败。
         *
         * 这个值明显偏大，适合“宁可慢，不轻易判失败”的策略。
         * 但如果你的目标是快速发现阻塞问题，这个值可以再收紧。
         */
        checkpointConfig.setCheckpointTimeout(1 * 60 * 60 * 1000L);

        /*
         * 第 6 步：限制同时只允许 1 个 checkpoint 在运行。
         *
         * 这样做的原因：
         * - 配合非对齐 checkpoint，更容易保持语义和资源占用稳定；
         * - 对当前单并发、长跑 CDC 作业来说，通常也足够；
         * - 避免多个 checkpoint 同时进行带来的资源竞争和排查复杂度。
         */
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        /*
         * 第 7 步：设置最小暂停间隔。
         *
         * 当前设置为 5 分钟，表示：
         * - 上一次 checkpoint 完成之后，至少等待 5 分钟，才允许开始下一次。
         *
         * 这进一步强化了“低频 checkpoint、减轻运行压力”的策略。
         * 和 3 小时周期叠加后，说明当前系统更偏向长稳态，而不是高频恢复点。
         */
        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 60 * 1000L);

        /*
         * 第 8 步：保留外部化 checkpoint。
         *
         * 选择 RETAIN_ON_CANCELLATION 的原因：
         * - 主动 cancel 作业时，也希望保留已有 checkpoint；
         * - 这样后续重启/人工恢复时，仍然有恢复入口；
         * - 对长跑 CDC 任务，这通常比 DELETE_ON_CANCELLATION 更合理。
         *
         * 代价是：
         * - 外部存储会持续积累 checkpoint 文件；
         * - 需要你后续有明确的清理策略。
         */
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        /*
         * 第 9 步：允许一定数量的 checkpoint 连续失败。
         *
         * 当前值是 100，容忍度非常高。
         * 这反映出当前策略更倾向于：
         * - 不因为偶发 checkpoint 异常就直接杀掉作业；
         * - 优先保证长跑任务继续活着。
         *
         * 但这也有明显代价：
         * - 如果 checkpoint 长期持续失败，问题可能会被拖很久才暴露；
         * - 作业看起来还活着，但实际上已经失去可靠恢复能力。
         *
         * 所以这个值虽然我保留，但我建议后续你认真评估是否要调低。
         */
        checkpointConfig.setTolerableCheckpointFailureNumber(100);

        /*
         * 第 10 步：启用非对齐 checkpoint。
         *
         * 非对齐 checkpoint 在存在背压时更有机会顺利完成，
         * 对流式长跑任务很有价值。
         *
         * 开启前提：
         * - 必须是 EXACTLY_ONCE；
         * - 并发 checkpoint 数量通常应控制为 1。
         *
         * 当前这两个条件前面都已经满足。
         */
        checkpointConfig.enableUnalignedCheckpoints();

        /*
         * 第 11 步：设置对齐 checkpoint 的超时阈值。
         *
         * 当前值 10 秒，含义是：
         * - checkpoint 开始时先尝试走普通对齐路径；
         * - 如果对齐耗时超过 10 秒，则切换到非对齐模式。
         *
         * 这是一个比较折中的选择：
         * - 正常情况下仍优先用对齐 checkpoint；
         * - 一旦背压明显，就退到非对齐，提高可完成性。
         */
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));

        logger.info("checkpointing configured successfully, mode={}, intervalMs={}, timeoutMs={}, minPauseMs={}, checkpointPath={}",
                CheckpointingMode.EXACTLY_ONCE,
                3 * 60 * 60 * 1000L,
                1 * 60 * 60 * 1000L,
                5 * 60 * 1000L,
                checkpointPath);
    }


    /**
     * 在给定的 checkpoint 根目录下，查找某个 Flink JobId 最近一次“真正可恢复”的 checkpoint 目录。
     *
     * 返回值不是 `_metadata` 文件本身，而是该 `_metadata` 所在的 `chk-*` 目录 URI，
     * 以便后续直接作为恢复路径传给 Flink。
     *
     * 为什么不能简单地取“名字最大的目录”：
     * - checkpoint 目录通常是 `chk-1`、`chk-2`、`chk-10` 这种格式；
     * - 如果直接按字符串倒序排序，`chk-9` 可能会排在 `chk-10` 前面；
     * - 所以必须按 checkpoint 序号的数值大小排序，而不是字典序。
     *
     * 为什么还要检查 `_metadata` 文件：
     * - 不是所有 `chk-*` 目录都一定是完整可恢复的；
     * - 某些 checkpoint 可能只创建了一半，或者文件不完整；
     * - `_metadata` 是判断该 checkpoint 是否具备恢复意义的关键标志之一。
     *
     * 这个方法的职责：
     * 1. 将 Flink checkpoint URI 转成本地文件路径；
     * 2. 定位 jobId 对应目录；
     * 3. 找出所有 `chk-*` 子目录；
     * 4. 按 checkpoint 编号从大到小排序；
     * 5. 返回第一个存在 `_metadata` 文件的 checkpoint 目录 URI。
     */
    public static String findLatestCheckpointPathWithMetadata(String checkpointBasePath,
                                                              String jobIdHex) throws Exception {
        /*
         * 第 0 步：校验入参。
         *
         * checkpointBasePath 是 checkpoint 根目录，
         * jobIdHex 是当前增量作业的稳定 jobId。
         * 两者缺一不可。
         */
        if (checkpointBasePath == null || checkpointBasePath.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpointBasePath is blank");
        }
        if (jobIdHex == null || jobIdHex.trim().isEmpty()) {
            throw new IllegalArgumentException("jobIdHex is blank");
        }

        /*
         * 第 1 步：把 Flink 风格的路径转成当前 JVM 能识别的本地文件路径。
         *
         * 例如：
         *   file:///F:/flink-checkpoints/demo
         * 会被转换成：
         *   /F:/flink-checkpoints/demo
         * 或当前平台可识别的本地路径形式。
         *
         * 注意：
         * 这个方法当前本质上只适用于“checkpoint 落在本地文件系统”的场景。
         * 如果后面你把 checkpoint 存到 HDFS / S3 / OSS，这个方法就不够用了，
         * 到时应改成基于 Flink FileSystem API 读取，而不是 java.io.File。
         */
        String localBasePath = new Path(checkpointBasePath.trim()).toUri().getPath();
        if (localBasePath == null || localBasePath.trim().isEmpty()) {
            throw new IllegalStateException("failed to resolve local checkpoint base path from " + checkpointBasePath);
        }

        /*
         * 第 2 步：定位当前 jobId 对应的 checkpoint 目录。
         *
         * Flink 通常会在 checkpoint 根目录下，以 jobId 作为一级子目录。
         */
        File jobDir = new File(localBasePath, jobIdHex.trim());

        /*
         * 如果 job 目录不存在，说明：
         * - 当前 job 从未成功产生过 checkpoint；
         * - 或 checkpoint 已被清理；
         * - 或路径配置不正确。
         *
         * 这里返回 null，表示“没有可恢复 checkpoint”，不视为异常。
         */
        if (!jobDir.exists() || !jobDir.isDirectory()) {
            return null;
        }

        /*
         * 第 3 步：找出该 job 目录下所有候选 checkpoint 目录。
         *
         * 只接受命名以 `chk-` 开头的目录。
         */
        File[] checkpointDirs = jobDir.listFiles(file ->
                file != null
                        && file.isDirectory()
                        && file.getName() != null
                        && file.getName().startsWith("chk-")
        );

        if (checkpointDirs == null || checkpointDirs.length == 0) {
            return null;
        }

        /*
         * 第 4 步：按 checkpoint 序号做“数值降序”排序，而不是字符串降序。
         *
         * 例如：
         * - chk-10 应该排在 chk-9 前面；
         * - chk-100 应该排在 chk-99 前面。
         *
         * 如果目录名格式异常、无法解析序号，则把它视为较小值，放到后面。
         */
        Arrays.sort(checkpointDirs, (left, right) -> {
            long rightSeq = extractCheckpointSequence(right.getName());
            long leftSeq = extractCheckpointSequence(left.getName());
            return Long.compare(rightSeq, leftSeq);
        });

        /*
         * 第 5 步：从最新的 checkpoint 开始往回找，
         * 返回第一个真正存在 `_metadata` 文件的目录。
         *
         * 注意：
         * 不是最新目录就一定有效，所以必须逐个验证。
         */
        for (File checkpointDir : checkpointDirs) {
            File metadataFile = new File(checkpointDir, "_metadata");
            if (metadataFile.exists() && metadataFile.isFile()) {
                return checkpointDir.toURI().toString();
            }
        }

        /*
         * 第 6 步：所有候选目录里都没有 `_metadata`，说明没有真正可恢复的 checkpoint。
         */
        return null;
    }


    /**
     * 从 checkpoint 目录名中提取序号。
     *
     * 例如：
     * - chk-1   -> 1
     * - chk-10  -> 10
     * - chk-999 -> 999
     *
     * 如果目录名不符合预期格式，则返回 -1，
     * 让调用方在排序时把异常目录放到后面。
     */
    private static long extractCheckpointSequence(String checkpointDirName) {
        if (checkpointDirName == null || checkpointDirName.trim().isEmpty()) {
            return -1L;
        }

        String name = checkpointDirName.trim();
        if (!name.startsWith("chk-")) {
            return -1L;
        }

        String suffix = name.substring("chk-".length()).trim();
        if (suffix.isEmpty()) {
            return -1L;
        }

        try {
            return Long.parseLong(suffix);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /**
     * 按“clientId + schemaName”分组，批量向 client 端拉取源表结构，
     * 并将结果缓存到 tableSchemaCache 中，供后续 incremental 阶段构建 RuntimeTableSchema 使用。
     *
     * 这个方法的输入来源是 topicInfoRegistry 中的 ServerTopicInfo 列表。
     * 原因是当前服务端真正需要消费哪些业务表，最终是由 topicInfo 决定的，
     * 所以按 topicInfo 反推需要哪些表结构是最直接、也最贴近执行态的做法。
     *
     * 这个方法的职责：
     * 1. 从 topicInfoRegistry 中提取出所有需要的 client/schema/table；
     * 2. 按 (clientId, schemaName) 聚合，减少远程 HTTP 调用次数；
     * 3. 调用 client 接口批量拉取 schema；
     * 4. 对返回结果做必要的完整性校验；
     * 5. 将结果缓存到 tableSchemaCache。
     *
     * 这个方法不负责：
     * - 不把 schema 转成 Flink DataType；
     * - 不构建 RuntimeTableSchema；
     * - 不解析 Kafka 消息；
     * - 不注册 temporary view。
     *
     * 为什么这里要“严格校验”：
     * - 表结构元数据属于后续 Flink 执行的关键前提；
     * - 如果这里静默接受不完整数据，后面增量作业很可能在更深层以难以排查的方式失败；
     * - 因此这里宁可尽早失败，也不要把脏 schema 带入后续执行链。
     */
    private void requestTableSchemaFromClient() throws Exception {
        // 读取当前服务端要消费的 topic 信息
        List<ServerTopicInfo> topicInfos =  topicInfoRegistry.getServerTopicInfoList();



        /*
         * 第 1 步：按照 (clientId, schemaName) 聚合请求。
         *
         * 之所以不按单表逐个请求，是因为：
         * - 同一个 client + schema 下通常有多张表；
         * - 合并请求可以显著减少 HTTP 往返次数；
         * - 对 client 侧探针和网络开销都更友好。
         *
         * requestMap:
         *   key   -> clientId_schemaName
         *   value -> 对应一次 HTTP 请求对象
         *
         * groupedTableNames:
         *   记录每个 key 下实际需要的去重表名集合
         */
        // key: clientId + businessDataSourceId + schemaName
        Map<String, OracleColumnsQueryRequest> requestMap = new LinkedHashMap<>();
        // key: clientId + businessDataSourceId + schemaName
        // value: 当前schema下所有的表名
        Map<String, LinkedHashSet<String>> groupedTableNames = new LinkedHashMap<>();

        for (ServerTopicInfo topicInfo : topicInfos) {
            String clientId =  topicInfo.getClientId().trim();
            String businessDataSourceId =  topicInfo.getBusinessDataSourceId().trim();
            String schemaName =  topicInfo.getSchemaName().trim();
            String tableName =  topicInfo.getTableName().trim();


            String normalizedSchemaName = schemaName.toUpperCase(Locale.ROOT);
            String normalizedTableName = tableName.toUpperCase(Locale.ROOT);

            // clientId + businessDataSourceId + schemaName
            String cacheKey = buildTableSchemaCacheKey(clientId, businessDataSourceId,normalizedSchemaName);


            // 构造请求对象骨架,tableNames 稍后统一从 groupedTableNames 中灌进去
            requestMap.computeIfAbsent(cacheKey, key -> {
                OracleColumnsQueryRequest request = new OracleColumnsQueryRequest();
                request.setClientId(clientId);
                request.setBusinessDataSourceId(businessDataSourceId);
                request.setOwner(normalizedSchemaName);
                request.setTableNames(new ArrayList<>());
                return request;
            });

            /*
             * 使用 LinkedHashSet 做去重，同时保持原始遍历顺序，
             * 这样日志里输出的 tableNames 顺序也更稳定。
             */
            groupedTableNames
                    .computeIfAbsent(cacheKey, key -> new LinkedHashSet<>())
                    .add(normalizedTableName);
        } // topic 处理结束



        // 逐组发起远程请求，并校验结果
        for (Map.Entry<String, OracleColumnsQueryRequest> entry : requestMap.entrySet()) {
            String cacheKey = entry.getKey();
            OracleColumnsQueryRequest oracleColumnsQueryRequest = entry.getValue();

            // 当前schema下所有的表
            List<String> requestedTables = new ArrayList<>(
                    groupedTableNames.getOrDefault(cacheKey, new LinkedHashSet<>())
            );


            // 当个请求中设置表
            oracleColumnsQueryRequest.setTableNames(requestedTables);


            // 真正调用 client 接口批量拉取 schema。
            OracleColumnsQueryResult result = syncClientHTTPClient.getTableSchema(oracleColumnsQueryRequest);

            /*
             * 第 3 步：做返回值完整性校验。
             *
             * 必须校验的原因：
             * - 后续 DefaultRuntimeTableSchemaResolver 会直接依赖缓存；
             * - 如果这里缓存了空结果，后面异常位置会离根因很远；
             * - 提前失败更容易定位是“client schema 拉取问题”。
             */
            if (result == null) {
                throw new IllegalStateException(
                        "客户端返回的 schema result 结果为 null, clientId=" + oracleColumnsQueryRequest.getClientId()
                                + ", schemaName=" + oracleColumnsQueryRequest.getOwner()
                                + ", tables=" + requestedTables
                );
            }

            if (result.getTableColumns() == null || result.getTableColumns().isEmpty()) {
                throw new IllegalStateException(
                        "客户端返回的 tableColumns 结果为 null,empty tableColumns, clientId=" + oracleColumnsQueryRequest.getClientId()
                                + ", schemaName=" + oracleColumnsQueryRequest.getOwner()
                                + ", tables=" + requestedTables
                );
            }

            /*
             * 第 4 步：校验每一张请求的表都确实返回了列定义。
             *
             * 这是这类方法最容易遗漏的点：
             * 不是“结果不为空”就够了，
             * 而是“我请求的每张表都必须有列定义”。
             *
             * 否则后面某个 topic 在真正构建 RuntimeTableSchema 时才会炸。
             */
            for (String requestedTable : requestedTables) {
                List<OracleColumnMetadata> columns = result.getTableColumns().get(requestedTable);
                if (columns == null || columns.isEmpty()) {
                    throw new IllegalStateException(
                            "missing columns in table schema result, clientId=" + oracleColumnsQueryRequest.getClientId()
                                    + ", schemaName=" + oracleColumnsQueryRequest.getOwner()
                                    + ", tableName=" + requestedTable
                    );
                }
            }

            /*
             * 第 5 步：通过校验后再写入缓存。
             *
             * 缓存粒度仍然保持为 clientId + schemaName。
             * 后续具体按表取列时，再去 result.getTableColumns().get(tableUpper)。
             */
            this.tableSchemaCache.put(cacheKey, result);

            /*
             * 打印拉取成功日志，便于排查：
             * - client/schema 是否正确；
             * - 请求了多少张表；
             * - 总列数大致多少。
             */
            int totalColumnCount = 0;
            for (String requestedTable : requestedTables) {
                List<OracleColumnMetadata> columns = result.getTableColumns().get(requestedTable);
                totalColumnCount += columns == null ? 0 : columns.size();
            }

            logger.info("table schema loaded successfully, clientId={}, schemaName={}, tableCount={}, totalColumnCount={}",
                    oracleColumnsQueryRequest.getClientId(),
                    oracleColumnsQueryRequest.getOwner(),
                    requestedTables.size(),
                    totalColumnCount);
        }
    }

    /**
     * 基于 topic 对应的表结构缓存，构建运行期使用的 RuntimeTableSchema。
     *
     * 这个方法的作用是把“数据库元数据层”的列定义，
     * 转换成 Flink 运行时真正需要的一组结构描述，包括：
     * - 原始列定义列表
     * - 字段名数组
     * - TypeInformation 数组
     * - RowTypeInfo
     * - Flink Table Schema
     *
     * 为什么要有这一层：
     * - requestTableSchemaFromClient() / resolveTableColumns() 拿到的只是 Oracle 元数据；
     * - Flink 在 fromChangelogStream / map / returns 等链路里，需要的是运行时 schema；
     * - 所以这里承担“元数据 -> Flink 运行时结构”的转换职责。
     *
     * 这版实现建议直接复用 RuntimeTableSchemaBuilder，
     * 而不是继续在 KafkaPipeline 里手工维护一套字段映射逻辑。
     *
     * 这样改的原因：
     * 1. 当前项目里已经有 RuntimeTableSchemaBuilder 专门做这件事；
     * 2. KafkaPipeline 里再保留一套 oracleToTypeInfo / oracleToDataType 的实现，会产生双份逻辑；
     * 3. 一旦两套映射规则未来不一致，运行结果会非常难排查；
     * 4. schema 构建规则应该集中，不应该散落在入口类里。
     */
    private RuntimeTableSchema buildRuntimeTableSchema(ServerTopicInfo topicInfo) {
        /*
         * 第 0 步：先通过 topicInfo 解析出目标表的列定义。
         *
         * resolveTableColumns() 已经保证：
         * - topicInfo 必填字段完整；
         * - 对应 schema cache 必须命中；
         * - 对应表的列定义必须非空。
         *
         * 所以这里拿到的 columns 可以被视为一个可信输入。
         */
        List<OracleColumnMetadata> columns = resolveTableColumns(topicInfo);

        /*
         * 第 1 步：调用统一的 RuntimeTableSchemaBuilder 完成构建。
         *
         * 这样可以把：
         * - 字段名规范化
         * - Oracle 类型到 Flink 类型映射
         * - RowTypeInfo 构建
         * - Schema 构建
         * 都集中在同一处维护。
         *
         * 这比在 KafkaPipeline 中重复写一套类型转换逻辑更合理。
         */
        RuntimeTableSchema runtimeTableSchema = RuntimeTableSchemaBuilder.build(columns);

        /*
         * 第 2 步：做结果兜底校验。
         *
         * 虽然 RuntimeTableSchemaBuilder 理论上应该保证结果完整，
         * 但这里作为入口层仍然建议补一层防御式检查，
         * 这样一旦 builder 后续被改坏，问题会在这里尽早暴露。
         */
        if (runtimeTableSchema == null) {
            throw new IllegalStateException(
                    "RuntimeTableSchemaBuilder returned null, topic=" + topicInfo.getTopic()
                            + ", clientId=" + topicInfo.getClientId()
                            + ", schemaName=" + topicInfo.getSchemaName()
                            + ", tableName=" + topicInfo.getTableName()
            );
        }

        if (runtimeTableSchema.getColumns() == null || runtimeTableSchema.getColumns().isEmpty()) {
            throw new IllegalStateException(
                    "runtimeTableSchema.columns is empty, topic=" + topicInfo.getTopic()
                            + ", clientId=" + topicInfo.getClientId()
                            + ", schemaName=" + topicInfo.getSchemaName()
                            + ", tableName=" + topicInfo.getTableName()
            );
        }

        if (runtimeTableSchema.getRowTypeInfo() == null) {
            throw new IllegalStateException(
                    "runtimeTableSchema.rowTypeInfo is null, topic=" + topicInfo.getTopic()
                            + ", clientId=" + topicInfo.getClientId()
                            + ", schemaName=" + topicInfo.getSchemaName()
                            + ", tableName=" + topicInfo.getTableName()
            );
        }

        if (runtimeTableSchema.getSchema() == null) {
            throw new IllegalStateException(
                    "runtimeTableSchema.schema is null, topic=" + topicInfo.getTopic()
                            + ", clientId=" + topicInfo.getClientId()
                            + ", schemaName=" + topicInfo.getSchemaName()
                            + ", tableName=" + topicInfo.getTableName()
            );
        }

        /*
         * 第 3 步：返回运行时 schema。
         */
        return runtimeTableSchema;
    }

    /**
     * 根据一个 ServerTopicInfo，从本地 tableSchemaCache 中解析出该表对应的列定义。
     *
     * 这个方法是“表结构缓存读取层”的核心入口，后续 RuntimeTableSchema 构建会直接依赖它。
     *
     * 输入：
     * - topicInfo：描述当前 topic 对应哪一个 client/schema/table
     *
     * 输出：
     * - 该表在 Oracle 元数据中的列定义列表
     *
     * 为什么这个方法必须严格校验：
     * - 这里已经不再做远程调用，只是读缓存；
     * - 如果这里还拿不到列定义，说明前面的 requestTableSchemaFromClient()
     *   要么没有拉到，要么缓存键不一致，要么 client 返回结果不完整；
     * - 这属于“执行前置条件失败”，应该明确抛异常，而不是返回空列表让后续逻辑继续跑。
     *
     * 这个方法的职责：
     * 1. 校验 topicInfo 的关键字段；
     * 2. 生成缓存键；
     * 3. 从 tableSchemaCache 读取对应 schema 级别结果；
     * 4. 从结果中取出目标表的列定义；
     * 5. 返回非空列定义列表。
     */
    private List<OracleColumnMetadata> resolveTableColumns(ServerTopicInfo topicInfo) {
        /*
         * 第 0 步：校验 topicInfo 本身。
         *
         * 这个方法是运行期关键路径，不能接受 null 输入。
         */
        if (topicInfo == null) {
            throw new IllegalArgumentException("topicInfo is null");
        }

        /*
         * 第 1 步：提取并校验构成缓存定位所需的关键字段。
         *
         * 这里至少需要：
         * - clientId
         * - schemaName
         * - tableName
         *
         * 任意一个缺失，都无法准确定位缓存和表结构。
         */
        String clientId = topicInfo.getClientId();
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("topicInfo.clientId is blank, topic=" + topicInfo.getTopic());
        }

        String schemaName = topicInfo.getSchemaName();
        if (schemaName == null || schemaName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "topicInfo.schemaName is blank, topic=" + topicInfo.getTopic()
                            + ", clientId=" + clientId
            );
        }

        String tableName = topicInfo.getTableName();
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "topicInfo.tableName is blank, topic=" + topicInfo.getTopic()
                            + ", clientId=" + clientId
                            + ", schemaName=" + schemaName
            );
        }

        /*
         * 第 2 步：构造 schema 级缓存键。
         *
         * tableSchemaCache 当前的缓存粒度是：
         *   clientId + "_" + schemaName(UPPER)
         */
        String cacheKey = buildTableSchemaCacheKey(clientId, topicInfo.getBusinessDataSourceId(),schemaName);

        /*
         * 第 3 步：读取 schema 级缓存结果。
         *
         * 如果缓存 miss，说明有几种可能：
         * - requestTableSchemaFromClient() 没有被调用；
         * - topicInfo 与拉取 schema 时的 client/schema 不一致；
         * - prepare / init 阶段清理了缓存但未重新预热；
         * - 缓存键规则前后不一致。
         *
         * 这里直接失败，因为这不是一个可以在运行期静默兜底的状态。
         */
        OracleColumnsQueryResult result = tableSchemaCache.get(cacheKey);
        if (result == null) {
            throw new IllegalStateException(
                    "table schema cache miss, cacheKey=" + cacheKey
                            + ", topic=" + topicInfo.getTopic()
                            + ", clientId=" + clientId
                            + ", schemaName=" + schemaName
                            + ", tableName=" + tableName
            );
        }

        /*
         * 第 4 步：校验缓存结果的 tableColumns 是否完整。
         *
         * 这里不是只关心 result 不为 null，
         * 还要求其内部 tableColumns 结构必须可用。
         */
        if (result.getTableColumns() == null || result.getTableColumns().isEmpty()) {
            throw new IllegalStateException(
                    "table schema cache contains empty tableColumns, cacheKey=" + cacheKey
                            + ", topic=" + topicInfo.getTopic()
                            + ", clientId=" + clientId
                            + ", schemaName=" + schemaName
            );
        }

        /*
         * 第 5 步：按 Oracle 元数据习惯，把表名统一转大写后查找。
         *
         * 这样能尽量避免因为 tableName 大小写不一致导致缓存取不到。
         */
        String normalizedTableName = tableName.trim().toUpperCase(Locale.ROOT);

        List<OracleColumnMetadata> columns = result.getTableColumns().get(normalizedTableName);

        /*
         * 第 6 步：校验目标表的列定义是否存在且非空。
         *
         * 这说明：
         * - 要么 client 端没有返回该表的结构；
         * - 要么 topicInfo 指向了一张未被正确拉取的表；
         * - 要么 schema 请求结果和实际表名不一致。
         *
         * 这里的异常信息一定要把 topic/client/schema/table 带齐，
         * 否则线上排查会非常痛苦。
         */
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(
                    "no columns found in schema cache, cacheKey=" + cacheKey
                            + ", topic=" + topicInfo.getTopic()
                            + ", clientId=" + clientId
                            + ", schemaName=" + schemaName
                            + ", tableName=" + normalizedTableName
            );
        }

        /*
         * 第 7 步：返回列定义。
         *
         * 返回的列表约定为：
         * - 非 null
         * - 非 empty
         *
         * 后续 buildRuntimeTableSchema() 可以建立在这个前提上继续处理。
         */
        return columns;
    }






    /**
     * 为某个业务表对应的临时视图生成一个稳定、可读、尽量唯一的名称。
     *
     * 这个名称主要用于 incremental 阶段：
     * - 把 Kafka CDC 流注册成 Flink temporary view；
     * - 后续 SQL 节点通过 aliasRegistry 间接引用这些 view。
     *
     * 命名目标有三个：
     * 1. 稳定：同一个业务表在同一套规则下，每次生成的名字应一致；
     * 2. 可读：日志里看到 viewName 时，能大致反推出来源；
     * 3. 安全：只包含适合 Flink SQL / temporary view 的字符。
     *
     * 为什么这里不能随便拼接：
     * - 原始 businessDataSourceId/schemaName/tableName 可能包含空格、点号、横线、中文或其他特殊字符；
     * - 这些字符不一定适合直接作为 temporary view 名称；
     * - 如果不做标准化，后续 SQL 引用、调试、日志排查都会不稳定。
     *
     * 注意：
     * 这个方法追求的是“尽量唯一”，不是绝对唯一。
     * 当前规则使用：
     *   businessDataSourceId + "_" + schemaName + "_" + tableName
     *
     * 在大多数业务场景下已经足够。
     * 如果你未来真的出现重名冲突，可以再追加 clientId 或 topic 作为补充维度。
     */
    private String buildUniqueViewName(ServerTopicInfo topicInfo) {
        /*
         * 第 0 步：校验 topicInfo 本身。
         *
         * 这是生成视图名的输入对象，如果为空，说明调用方本身已经有问题。
         */
        if (topicInfo == null) {
            throw new IllegalArgumentException("topicInfo is null");
        }

        /*
         * 第 1 步：提取组成 viewName 的核心字段。
         *
         * 这里选择：
         * - businessDataSourceId
         * - schemaName
         * - tableName
         *
         * 原因是这三个字段最能表达“这张表来自哪里”。
         *
         * 这里继续沿用 safe(...) 做标准化兜底：
         * - null -> "null"
         * - 前后空白 trim 掉
         */
        String raw = String.join("_",
                safe(topicInfo.getBusinessDataSourceId()),
                safe(topicInfo.getSchemaName()),
                safe(topicInfo.getTableName())
        ).toLowerCase(Locale.ROOT);

        /*
         * 第 2 步：把不安全字符统一替换成下划线。
         *
         * 允许保留的字符集：
         * - a-z
         * - 0-9
         * - _
         *
         * 这样做的好处：
         * - 生成出来的名字更适合作为 SQL temporary view 名；
         * - 避免空格、点号、横线等字符在 SQL 引用中带来歧义；
         * - 不同来源的非法字符被收敛到同一种形式。
         */
        String sanitized = raw.replaceAll("[^a-z0-9_]", "_");

        /*
         * 第 3 步：压缩连续下划线，避免名字过于难看。
         *
         * 例如：
         *   a---b...c
         * 经过替换可能会变成：
         *   a___b___c
         * 再压缩后会更整洁：
         *   a_b_c
         */
        sanitized = sanitized.replaceAll("_+", "_");

        /*
         * 第 4 步：去掉首尾下划线，避免命名边界很脏。
         */
        sanitized = sanitized.replaceAll("^_+", "").replaceAll("_+$", "");

        /*
         * 第 5 步：防止名字为空，或者以数字开头。
         *
         * 以数字开头的名字在很多 SQL 标识符语义里都不够稳妥，
         * 所以统一补一个前缀。
         */
        if (sanitized.isEmpty()) {
            sanitized = "v_unknown";
        } else if (Character.isDigit(sanitized.charAt(0))) {
            sanitized = "v_" + sanitized;
        }

        /*
         * 第 6 步：增加固定前缀，强化这是“系统生成的临时视图名”。
         *
         * 这样做的好处：
         * - 日志中更容易识别；
         * - 避免和用户自定义的其他临时视图重名；
         * - 语义更明确。
         *
         * 如果你不想改现有命名风格，也可以把这一层拿掉。
         * 但从长期可维护性来说，我建议保留一个系统前缀。
         */
        return "vw_" + sanitized;
    }
    /**
     * 对字符串做最基础的空值兜底和空白清理。
     *
     * 这个方法的目标非常简单：
     * - 如果输入为 null，返回一个可参与后续拼接的占位值；
     * - 如果输入不为 null，去掉前后空白；
     * - 如果 trim 后为空，也返回占位值。
     *
     * 为什么需要这样一个小方法：
     * - 当前 KafkaPipeline 中很多地方需要把业务字段拼进日志、缓存键、viewName；
     * - 这些字段可能来自配置库或远程请求，不能完全信任其非空性；
     * - 单独抽一个方法能让字符串标准化行为保持一致。
     *
     * 注意：
     * - 这个方法适合“日志 / 命名 / 辅助拼接”场景；
     * - 不适合用于“业务主键 / 严格参数校验”场景；
     * - 真正的必填参数仍应走显式校验逻辑，而不是靠 safe() 兜底。
     */
    private String safe(String value) {
        /*
         * 第 0 步：null 直接返回统一占位值。
         */
        if (value == null) {
            return "null";
        }

        /*
         * 第 1 步：去掉前后空白。
         */
        String trimmed = value.trim();

        /*
         * 第 2 步：如果 trim 后为空，仍然返回统一占位值。
         *
         * 这样可以避免：
         * - 日志里出现空串；
         * - 拼出来的 viewName / 辅助标识中出现连续分隔符但看不出缺了哪一段。
         */
        if (trimmed.isEmpty()) {
            return "null";
        }

        /*
         * 第 3 步：返回标准化后的字符串。
         */
        return trimmed;
    }









    /**
     * 遍历当前已加载的 RulePlan，找到其中需要“预先解析结果列结构”的 SQL 节点，
     * 调用 client 端接口拉取 SQL 输出 schema，并缓存到 sqlNodeSchemaCache / sqlNodeSchemaRegistry。
     *
     * 这个方法主要服务于 snapshot SQL 场景。
     *
     * 为什么 SQL 节点需要提前拉 schema：
     * - 普通表 CDC 流的字段结构可以从表元数据中获得；
     * - 但 SQL 节点的输出列是“查询结果列”，不一定等于源表结构；
     * - SnapshotSqlPlanNode 在运行时要把 JDBC 查询结果映射成 Flink Row，
     *   必须提前知道列名、列类型、精度、scale 等元数据；
     * - 因此这里要先让 client 端数据库去解释 SQL，再把列结构带回来。
     *
     * 这个方法的职责：
     * 1. 遍历 rulePlans；
     * 2. 识别需要预热 schema 的 SQL 节点；
     * 3. 从节点参数中提取 sql/schema/data_source_id；
     * 4. 解析 data_source_id 对应的 clientId；
     * 5. 调用 client 端接口拉取 SQL 列结构；
     * 6. 对返回结果做完整性校验；
     * 7. 写入本地缓存和 registry。
     *
     * 这个方法不负责：
     * - 不执行 SQL；
     * - 不创建临时表；
     * - 不构建 RuntimeTableSchema；
     * - 不推进 plan phase。
     */
    private void requestSqlNodeSchemaFromClient() throws Exception {
        /*
         * 第 0 步：如果当前没有 RulePlan，直接结束。
         *
         * 这是正常场景，不需要抛错。
         */
        if (rulePlans == null || rulePlans.isEmpty()) {
            logger.info("no rule plans found, skip requesting SQL node schema from client");
            return;
        }

        /*
         * 统计一下到底处理了多少个 SQL 节点，方便日志观察。
         */
        int sqlNodeCount = 0;

        /*
         * 第 1 步：逐个 plan 遍历。
         *
         * 每个 plan 里可能包含多个节点，但并不是每个节点都需要远程 schema。
         */
        for (RulePlan plan : rulePlans) {
            if (plan == null) {
                logger.warn("skip null rule plan while requesting SQL node schema");
                continue;
            }

            String planId = plan.getPlanId();
            if (planId == null || planId.trim().isEmpty()) {
                throw new IllegalStateException("encountered rule plan with blank planId");
            }

            if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
                logger.info("plan has no nodes, skip requesting SQL schema, planId={}", planId);
                continue;
            }

            /*
             * 第 2 步：遍历 plan 下的节点，只处理 SQL 节点。
             *
             * 当前设计里，真正需要提前从数据库解释列结构的是 PlanNodeType.SQL。
             * 因为它的输出列不是固定表结构，而是 SQL 查询结果。
             *
             * 注意：
             * 如果未来你的 SQL 类节点细分成 SNAPSHOT_SQL / INCREMENTAL_SQL / LOOKUP_SQL，
             * 这里建议再做更细粒度筛选，而不是笼统判断 SQL。
             */
            for (PlanNodeSpec node : plan.getNodes()) {
                if (node == null) {
                    logger.warn("skip null node, planId={}", planId);
                    continue;
                }

                if (node.getType() != PlanNodeType.SQL) {
                    continue;
                }

                sqlNodeCount++;

                /*
                 * 第 3 步：提取 SQL 节点必需参数。
                 *
                 * 这里必须 fail-fast：
                 * - sql 缺失：根本无法让 client 端解析；
                 * - schema 缺失：client 端无法在正确 schema 语境下解释 SQL；
                 * - data_source_id 解析不到 clientId：无法定位请求该发给哪个 client。
                 */
                String nodeId = node.getId();
                String sql = extractRequiredStringParam(node, "sql");
                String businessDataSourceId = extractRequiredStringParam(node, "data_source_id");
                String schema = extractRequiredStringParam(node, "schema");
                String clientId = resolveClientIdFromNode(node);

                if (clientId == null || clientId.trim().isEmpty()) {
                    throw new IllegalStateException(
                            "resolved blank clientId for SQL node, planId=" + planId
                                    + ", nodeId=" + nodeId
                    );
                }

                /*
                 * schema 在数据库元数据语义里统一转大写，
                 * 这样和 Oracle 侧对象名处理保持一致。
                 */
                String normalizedSchema = schema.trim().toUpperCase(Locale.ROOT);

                OracleSqlColumnsQueryRequest request = new OracleSqlColumnsQueryRequest();
                request.setClientId(clientId.trim());
                request.setBusinessDataSourceId(businessDataSourceId.trim());
                request.setSql(sql);
                request.setSchema(normalizedSchema);

                logger.info("requesting SQL node schema from client, planId={}, nodeId={}, clientId={}, schema={}",
                        planId,
                        nodeId,
                        clientId,
                        normalizedSchema);

                /*
                 * 第 4 步：调用 client 端接口解析 SQL 输出列结构。
                 *
                 * 这个接口本质上是在 client 侧数据库上做“SQL 元数据解析”，
                 * 而不是执行完整业务计算。
                 */
                OracleColumnsQueryResult result = syncClientHTTPClient.getSqlColumnsSchema(clientId, request);

                /*
                 * 第 5 步：校验返回结果。
                 *
                 * 这里不能只判断 result != null，
                 * 还必须确保 columns 非空。
                 *
                 * 否则 SnapshotSqlPlanNode 在真正构建 RuntimeTableSchema 时，
                 * 会因为列定义为空而在运行期失败，定位起来更费劲。
                 */
                if (result == null) {
                    throw new IllegalStateException(
                            "client returned null SQL schema result, planId=" + planId
                                    + ", nodeId=" + nodeId
                                    + ", clientId=" + clientId
                    );
                }

                if (result.getColumns() == null || result.getColumns().isEmpty()) {
                    throw new IllegalStateException(
                            "client returned empty SQL schema columns, planId=" + planId
                                    + ", nodeId=" + nodeId
                                    + ", clientId=" + clientId
                    );
                }

                /*
                 * 第 6 步：写入两层缓存。
                 *
                 * sqlNodeSchemaCache：
                 * - KafkaPipeline 本地缓存，偏向入口层和调试层使用；
                 *
                 * sqlNodeSchemaRegistry：
                 * - 供后续 PlanContext / SnapshotSqlPlanNode 通过 planId + nodeId 查找；
                 * - 是新执行链真正依赖的 registry。
                 */
                String cacheKey = buildSqlNodeCacheKey(planId, nodeId);
                this.sqlNodeSchemaCache.put(cacheKey, result);
                this.sqlNodeSchemaRegistry.register(planId, nodeId, result);

                logger.info("SQL node schema loaded successfully, planId={}, nodeId={}, clientId={}, columnCount={}",
                        planId,
                        nodeId,
                        clientId,
                        result.getColumns().size());
            }
        }

        logger.info("finish requesting SQL node schema from client, sqlNodeCount={}", sqlNodeCount);
    }


    /**
     * 从指定的 PlanNodeSpec 参数中提取一个“必填字符串参数”。
     *
     * 这个方法的设计目标是做一层统一的参数读取与校验，避免每个调用点都重复写：
     * - node 判空；
     * - params 判空；
     * - key 判空；
     * - value 判空；
     * - value trim 后判空；
     * - 错误信息拼装。
     *
     * 为什么这个方法很重要：
     * - 当前 KafkaPipeline 中大量逻辑依赖节点参数，例如：
     *   sql / schema / data_source_id / targetTableName 等；
     * - 如果参数读取逻辑不统一，就会出现有的地方严格校验、有的地方静默接受空值；
     * - 最终会导致错误在深层执行时才暴露，而且报错信息非常不一致。
     *
     * 这里采用 fail-fast 策略：
     * - 只要调用方声明“这是必填参数”，那这里就必须保证返回的是非空白字符串；
     * - 否则立即抛异常，并带上 nodeId / paramKey，便于快速定位配置问题。
     */
    private String extractRequiredStringParam(PlanNodeSpec node, String paramKey) {
        /*
         * 第 0 步：校验 node 本身。
         *
         * 如果连 node 都是 null，说明调用链上游已经有明显问题，
         * 不应该继续做任何参数读取。
         */
        if (node == null) {
            throw new IllegalArgumentException("node is null");
        }

        /*
         * 第 1 步：校验参数名。
         *
         * paramKey 是调用方传进来的“我要读取哪个参数”的声明。
         * 如果这个 key 自己就是空的，属于编程错误，不是配置错误。
         */
        if (paramKey == null || paramKey.trim().isEmpty()) {
            throw new IllegalArgumentException("paramKey is blank, nodeId=" + node.getId());
        }

        /*
         * 第 2 步：校验 params 容器。
         *
         * 如果节点压根没有 params，说明这个节点配置本身就不完整。
         */
        Map<String, Object> params = node.getParams();
        if (params == null || params.isEmpty()) {
            throw new IllegalArgumentException("node params is empty, nodeId=" + node.getId());
        }

        /*
         * 第 3 步：取值并做标准化。
         *
         * 这里统一转成 String，再 trim。
         * 这样上层无论传的是字符串、数字，还是其他可 stringify 的对象，
         * 最终都按“非空文本参数”处理。
         */
        Object rawValue = params.get(paramKey);
        if (rawValue == null) {
            throw new IllegalArgumentException(
                    "missing required param `" + paramKey + "`, nodeId=" + node.getId()
            );
        }

        String value = String.valueOf(rawValue).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException(
                    "required param `" + paramKey + "` is blank, nodeId=" + node.getId()
            );
        }

        /*
         * 第 4 步：返回标准化后的参数值。
         *
         * 所有调用方都可以默认：
         * - 返回值非 null；
         * - 返回值非空串；
         * - 返回值已经 trim。
         */
        return value;
    }
    /**
     * 根据节点中的数据源参数，解析出对应的 clientId。
     *
     * 这个方法主要用于 SQL 节点等“需要向某个 client 端发起远程请求”的场景。
     * 因为服务端真正调用 HTTP 接口时，目标是某个 client，
     * 而节点配置里通常保存的是 data_source_id，不是 clientId。
     *
     * 所以这里做的事情本质上是：
     * 1. 从节点参数中读取 data_source_id；
     * 2. 通过 configFetcher 把 data_source_id 映射成 clientId；
     * 3. 对结果做严格校验；
     * 4. 返回一个可直接用于远程调用的 clientId。
     *
     * 为什么这里必须严格校验：
     * - 如果 data_source_id 配错或映射关系缺失，
     *   后续 requestSqlNodeSchemaFromClient() 会在更深处调用 client HTTP 接口时报错；
     * - 那时问题已经被“包装”成远程调用失败，不如在这里直接暴露“映射失败”更清楚；
     * - 同时这里也是配置问题和运行问题的分界点，适合做 fail-fast。
     */
    private String resolveClientIdFromNode(PlanNodeSpec node) {
        /*
         * 第 0 步：校验 node 本身。
         *
         * 这是最基础的防御式检查。
         */
        if (node == null) {
            throw new IllegalArgumentException("node is null");
        }

        /*
         * 第 1 步：检查 configFetcher 是否可用。
         *
         * 这个方法依赖 configFetcher 中维护的配置查询能力。
         * 如果它不存在，说明 KafkaPipeline 还没被正确初始化，或者 Spring 注入异常。
         */
        if (configFetcher == null) {
            throw new IllegalStateException("configFetcher is null");
        }

        /*
         * 第 2 步：读取节点参数中的 data_source_id。
         *
         * 当前节点参数命名可能存在历史兼容问题，
         * 所以这里同时兼容：
         * - data_source_id
         * - dataFromSourceId
         *
         * 如果你后面准备统一参数命名，建议最终只保留一个字段，
         * 避免节点配置风格长期分裂。
         */
        String dataSourceId = null;

        Map<String, Object> params = node.getParams();
        if (params != null && !params.isEmpty()) {
            Object value = params.get("data_source_id");
            if (value == null || String.valueOf(value).trim().isEmpty()) {
                value = params.get("dataFromSourceId");
            }
            if (value != null) {
                dataSourceId = String.valueOf(value).trim();
            }
        }

        /*
         * data_source_id 是这个解析流程的输入根基。
         * 如果它本身缺失，就不用继续查 clientId 了。
         */
        if (dataSourceId == null || dataSourceId.isEmpty()) {
            throw new IllegalArgumentException(
                    "missing required param `data_source_id`, nodeId=" + node.getId()
            );
        }

        /*
         * 第 3 步：通过配置查询器把 data_source_id 解析为 clientId。
         *
         * 这一步依赖配置库中已经存在有效的映射关系。
         */
        String clientId = configFetcher.resolveClientIdByDataSourceId(dataSourceId);

        /*
         * 第 4 步：严格校验解析结果。
         *
         * 如果查不到 clientId，说明出现了以下任一问题：
         * - 节点配置中的 data_source_id 写错；
         * - 配置库中的数据源记录缺失；
         * - active client 缓存未刷新；
         * - client 与 data_source_id 的绑定关系异常。
         *
         * 这里直接抛异常，并把 nodeId / dataSourceId 带出来，
         * 能最快帮助定位是哪个节点、哪个数据源配置有问题。
         */
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalStateException(
                    "failed to resolve clientId from dataSourceId, nodeId=" + node.getId()
                            + ", dataSourceId=" + dataSourceId
            );
        }

        /*
         * 第 5 步：返回标准化后的 clientId。
         */
        return clientId.trim();
    }
    /**
     * 构造 SQL 节点 schema 缓存键。
     *
     * 当前缓存粒度定义为：
     *   planId + "|" + nodeId
     *
     * 这样设计的原因是：
     * - SQL 节点的输出 schema 不是按表名缓存，而是按“某个计划中的某个节点”缓存；
     * - 同一个 nodeId 在不同 plan 中可能语义不同，所以不能只用 nodeId；
     * - 同一个 plan 下不同 SQL 节点也可能输出完全不同的列结构，所以必须把 nodeId 带上。
     *
     * 为什么这个方法要单独封装：
     * - 避免各处手写字符串拼接，减少键格式不一致的风险；
     * - 一旦以后想更换缓存键格式，只需要改这里；
     * - 日志、registry、cache 都能共用同一套键规则。
     */
    private String buildSqlNodeCacheKey(String planId, String nodeId) {
        /*
         * 第 0 步：严格校验入参。
         *
         * 这里不允许 silently 容忍空 planId / nodeId，
         * 因为一旦用空值参与拼接，缓存键会变得不可预测，
         * 后续命中/覆盖问题会很难排查。
         */
        if (planId == null || planId.trim().isEmpty()) {
            throw new IllegalArgumentException("planId is blank when building SQL node cache key");
        }
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeId is blank when building SQL node cache key, planId=" + planId);
        }

        /*
         * 第 1 步：统一 trim，去掉配置中的前后空白。
         *
         * 这里不转大小写。
         * 原因是：
         * - planId / nodeId 通常是逻辑标识符，不一定适合强行大小写归一；
         * - 只做空白标准化即可。
         */
        String normalizedPlanId = planId.trim();
        String normalizedNodeId = nodeId.trim();

        /*
         * 第 2 步：拼接缓存键并返回。
         */
        return normalizedPlanId + "|" + normalizedNodeId;
    }

    // 构造业务表 schema 缓存键
    private String buildTableSchemaCacheKey(String clientId,String businessDataSourceId, String schemaName) {

        String normalizedClientId = clientId.trim();
        String normalizedBusinessDataSourceId = businessDataSourceId.trim();
        String normalizedSchemaName = schemaName.trim().toUpperCase(Locale.ROOT);

        /*
         * 第 2 步：拼接缓存键并返回。
         */
        return normalizedClientId + "_" + normalizedBusinessDataSourceId + "_" + normalizedSchemaName;
    }








    private String resolveTargetTableNameForSqlNode(RulePlan plan, PlanNodeSpec node) {
        Map<String, Object> params = node.getParams();
        if (params != null) {
            Object v = params.get("targetTableName");
            if (v == null || String.valueOf(v).trim().isEmpty()) v = params.get("tableName");
            if (v == null || String.valueOf(v).trim().isEmpty()) v = params.get("outputTableName");
            if (v != null && !String.valueOf(v).trim().isEmpty()) return String.valueOf(v).trim();
        }
        return ("SQL_RESULT_" + plan.getPlanId() + "_" + node.getId()).toUpperCase(Locale.ROOT);
    }

    private DatabaseType resolveTargetDatabaseTypeForSqlNode(PlanNodeSpec node) {
        Map<String, Object> params = node.getParams();
        if (params != null) {
            Object v = params.get("targetDatabaseType");
            if (v == null || String.valueOf(v).trim().isEmpty()) v = params.get("databaseType");
            if (v == null || String.valueOf(v).trim().isEmpty()) v = params.get("targetDbType");
            if (v != null && !String.valueOf(v).trim().isEmpty()) {
                return DatabaseType.valueOf(String.valueOf(v).trim().toUpperCase(Locale.ROOT));
            }
        }
        return DatabaseType.ORACLE;
    }

    /**
     * 判断当前已加载的 RulePlan 中，是否存在需要 SQL 处理链参与的计划节点。
     *
     * 这个方法的用途是做“能力预判”或“流程开关判断”，例如：
     * - 是否需要预热 SQL 节点 schema；
     * - 是否需要执行 SQL 相关的 snapshot 逻辑；
     * - 是否要在日志中提示当前 plan 包含 SQL 链路。
     *
     * 当前判断规则：
     * - 只要任一 RulePlan 中出现 `PlanNodeType.SQL` 或 `PlanNodeType.SINK_TABLE`，
     *   就认为当前存在 SQL 相关计划。
     *
     * 为什么把 `SINK_TABLE` 也算进去：
     * - 在当前项目结构里，`SINK_TABLE` 往往不是孤立存在，
     *   它通常代表一条表结果已经经过 SQL/转换处理、最终准备落地；
     * - 从运行链路角度看，它属于 SQL 处理链的一部分。
     *
     * 当然，这个判断是偏“工程语义”的，不是纯类型学定义。
     * 如果你后续把 plan 节点体系拆得更清楚，这里可以收紧为只判断真正需要 SQL 预热的节点类型。
     */
    private boolean hasSqlPlans() {
        /*
         * 第 0 步：如果当前没有加载任何 RulePlan，直接返回 false。
         *
         * 这属于正常场景，不需要抛异常。
         */
        if (this.rulePlans == null || this.rulePlans.isEmpty()) {
            return false;
        }

        /*
         * 第 1 步：逐个 plan 遍历，寻找 SQL 相关节点。
         */
        for (RulePlan plan : this.rulePlans) {
            if (plan == null) {
                continue;
            }

            if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
                continue;
            }

            for (PlanNodeSpec node : plan.getNodes()) {
                if (node == null || node.getType() == null) {
                    continue;
                }

                /*
                 * 只要出现任一 SQL 相关节点，就可以提前返回 true。
                 * 这里采用短路判断，避免无意义继续遍历。
                 */
                if (node.getType() == PlanNodeType.SQL || node.getType() == PlanNodeType.SINK_TABLE) {
                    return true;
                }
            }
        }

        /*
         * 所有 plan 遍历结束后仍未发现 SQL 相关节点，返回 false。
         */
        return false;
    }


    private void closeQuietly(AutoCloseable closeable, String name) {
        // 空对象无需关闭
        if (closeable == null) {
            return;
        }

        try {
            // 尝试关闭资源
            closeable.close();
        } catch (Exception e) {
            // 关闭失败只记录日志
            logger.warn("failed to close {}", name, e);
        }
    }



}
