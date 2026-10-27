package com.bsoft.client.o2k;

import com.bsoft.client.api.ClientController;
import com.bsoft.client.dispatcher.ClientJobDispatcher;
import com.bsoft.client.flink.SharedMiniClusterManager;
import com.bsoft.client.function.OracleSourceLogRichFunction;

import com.bsoft.client.function.SideOutputProcessFunction;
import com.bsoft.client.safety.DangerousOperationCountdown;
import com.bsoft.client.signal.JobFailureZkReporter;
import com.bsoft.client.sink.OrderedKafkaRoutingSink;
import com.bsoft.client.sink.SnapshotCompleteJdbcSink;
import com.bsoft.client.state.DataSourceRunStateRepository;
import com.bsoft.client.state.JdbcDataSourceRunStateRepository;
import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.*;
import com.bsoft.common.service.TopicInfoService;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import com.bsoft.common.zk.service.ClientJobZkService;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.OracleSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Oracle CDC 到 Kafka 的数据同步组件
 *
 * 当前版本设计目标：
 * 1. 一个 sync-client 进程可以采集多个业务库
 * 2. 每个业务库对应一个独立 Flink Job
 * 3. 每个 Job 都拥有自己独立的：
 *    - JobClient
 *    - checkpoint / savepoint 恢复
 *    - topic 视图
 *    - stop signal
 *    - scn 路径
 *    - snapshot stop monitor
 *
 * 核心思路：
 * 1. 从配置库中读出当前客户端负责的所有 BusinessDataSourceConfig
 * 2. 生成客户端全局执行计划
 * 3. 再按 fromDataSourceId 过滤成每个业务库自己的 scopedExecutionPlan
 * 4. 每个业务库构造成一个 ClientJobRuntime
 * 5. 最后逐个启动独立 Flink Job
 */
@Component
public class OracleToKafka extends CDCJob {

    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;

    // Kafka 工具类
    @Autowired
    private KafkaUtils kafkaUtils;

    // 用于初始化客户端端的 Oracle 元数据读取器
    @Autowired
    private ClientController clientController;

//    // 状态后端配置
//    @Autowired
//    private StateBackendProperties stateBackendProperties;

    // 当前客户端分配到的所有业务库
    private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;

    // 当前客户端涉及的所有目标库
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 当前客户端下所有业务库的订阅关系
    private SubscriberMappingRegistry subscriberMappingRegistry;



    /**
     * 多 job 运行时上下文
     *
     * key: businessDataSourceId
     * value: 当前业务库对应的运行时上下文
     */
    private final Map<String, ClientJobRuntime> runtimeByDataSourceId = new LinkedHashMap<>();

    /**
     * 调度器引用
     *
     * 主要用于 snapshot 完成后回调重启指定业务库 job。
     */
    private ClientJobDispatcher clientJobDispatcher;


//    /**
//     * client 进程级 ZK 服务。
//     *
//     * 负责：
//     * 1. client 进程级节点初始化
//     * 2. client 进程级状态更新
//     */
//    private ClientProcessZkService clientProcessZkService;


    @Autowired
    private TopicInfoService topicInfoService;


    private static final long PREREQUISITE_RETRY_INTERVAL_MS = 60_000L;

//    // 收集所有 topic offset 提交事件侧输出流
//    List<DataStream<DataSourceScnCommitEvent>> allDataSourceScnCommitStreams = new ArrayList<>();

//    // scn 持久化仓储
//    private transient DataSourceScnRepository dataSourceScnRepository;

    // 业务库 snapshot 运行状态仓库
    // 用于读取 CDC_DATA_SOURCE_RUN_STATE，判断当前业务库 snapshot 是否已经完成
    private transient DataSourceRunStateRepository dataSourceRunStateRepository;


    // Flink job 失败上报器
    private JobFailureZkReporter jobFailureZkReporter;


    public void init(ClientJobDispatcher clientJobDispatcher) throws Exception {
        // 保存调度器引用
        this.clientJobDispatcher = clientJobDispatcher;

        // 初始化 业务库运行状态 仓库
        // 后续启动模式判断依赖 CDC_DATA_SOURCE_RUN_STATE
        this.dataSourceRunStateRepository =
                new JdbcDataSourceRunStateRepository(this.configFetcher.getConfigDataSource());

        // 初始化 job 失败上报器，并把失败事件交给 dispatcher 处理。
        this.jobFailureZkReporter = new JobFailureZkReporter(clientInfo, clientJobDispatcher);
    }

    // 启动作业总入口
    public void startJob() throws Exception {

        // 1. 初始化客户端 ZK 根节点
//        clientProcessZkService.updateStatus(JobStatus.INITIALIZING);

        // 1.注册当前 client，并从配置库回填 DATA_SOURCE_ID 等绑定信息
        registerCurrentClient();

        // 2. 第一阶段前置检查：只依赖配置库和 clientInfo，不依赖运行时对象
        verifyClientConfigPrerequisites();

        // 3. 加载运行配置
        prepareForJobExecution();

        // 4. 第二阶段前置检查：依赖已经初始化好的业务库和订阅关系
        verifyRuntimeSourcePrerequisites();

        // 5. 初始化客户端执行环境
        initClientExecutionEnvironment();

        // 6.分析配置内容，制定执行计划，
        buildClientExecutionPlan();

        // 7. 为每个业务库，创建独立的 ClientJobRuntime
        buildRuntimes();



        // 10. 启动所有业务库对应的 Flink Job
        startAllDataSourceJobs();
    }

    /**
     * 注册当前客户端。
     */
    private void registerCurrentClient() throws Exception {
        // 如果 client 已存在，会把配置库中的 DATA_SOURCE_ID 回填到 clientInfo
        this.configFetcher.registerClientIfAbsent(clientInfo);
    }

    /**
     * 构建当前客户端的执行计划。
     */
    private void buildClientExecutionPlan() throws Exception {
        // 根据订阅关系生成或刷新当前客户端的执行计划
        this.executionPlan = configFetcher.buildClientExecutionPlan(this.subscriberMappingRegistry);
    }
    /**
     * 第一阶段前置检查。
     * 只依赖配置库和 clientInfo，适合放在 registerClientIfAbsent(clientInfo) 之后立即执行。
     */
    public void verifyClientConfigPrerequisites() throws Exception {
        if (!clientInfo.isOracleCdcPreRequisites()) {
            return;
        }

        int round = 1;

        while (true) {
            logger.info("开始执行客户端配置前置检查，第 {} 轮", round++);

            boolean passed = true;

            // 读取 CDC_CLIENT_MULTIPLE 中所有激活 client
            List<SimpleClientInfo> activeClientRecords = configFetcher.loadActiveSimpleClientInfos();

            // 检查激活记录中的 CLIENT_ID 是否重复
            if (!checkActiveClientIdUniqueness(activeClientRecords)) {
                passed = false;
            }

            // 检查不同激活 client 之间 DATA_SOURCE_ID 是否重复
            if (!checkActiveDataSourceIdUniqueness(activeClientRecords)) {
                passed = false;
            }

            // 检查当前 client 是否已经拿到了有效业务库配置，同时检查执行计划归属冲突
            if (!checkExecutionPlanSourceOwnership(activeClientRecords)) {
                passed = false;
            }

            if (passed) {
                logger.info("客户端配置前置检查通过，继续启动 CDC 作业");
                return;
            }

            logger.warn("客户端配置前置检查未通过，{} 秒后重新检查", PREREQUISITE_RETRY_INTERVAL_MS/1000);
            Thread.sleep(PREREQUISITE_RETRY_INTERVAL_MS);
        }
    }

    /**
     * 第二阶段前置检查。
     * 依赖 prepareForJobExecution() 和 initClientExecutionEnvironment() 已经初始化好的对象。
     */
    public void verifyRuntimeSourcePrerequisites() throws Exception {
        if (!clientInfo.isOracleCdcPreRequisites()) {
            return;
        }

        int round = 1;

        while (true) {
            logger.info("开始执行运行时源库前置检查，第 {} 轮", round++);

            boolean passed = true;

            // 检查当前业务库账号是否能访问所有订阅源表
            if (!checkSourceTableSelectPrivileges()) {
                passed = false;
            }

            if (passed) {
                logger.info("运行时源库前置检查通过，继续启动 CDC 作业");
                return;
            }

            logger.warn("运行时源库前置检查未通过，{} 毫秒后重新检查", PREREQUISITE_RETRY_INTERVAL_MS);
            Thread.sleep(PREREQUISITE_RETRY_INTERVAL_MS);
        }
    }

    /**
     * 前提一：
     * FG_ACTIVE='1' 的记录中，CLIENT_ID 不能重复。
     */
    private boolean checkActiveClientIdUniqueness(List<SimpleClientInfo> activeClientRecords) {
        Map<String, Long> countByClientId = activeClientRecords.stream()
                .map(SimpleClientInfo::getClientId)
                .map(this::normalizeText)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.groupingBy(
                        Function.identity(),
                        LinkedHashMap::new,
                        Collectors.counting()
                ));

        boolean passed = true;

        for (Map.Entry<String, Long> entry : countByClientId.entrySet()) {
            if (entry.getValue() > 1) {
                passed = false;
                logger.warn(
                        "前置检查未通过：CDC_CLIENT_MULTIPLE 表中存在重复的激活 CLIENT_ID，CLIENT_ID={}，重复记录数={}",
                        entry.getKey(),
                        entry.getValue()
                );
            }
        }

        return passed;
    }

    /**
     * 前提二：
     * 不同激活记录之间的 DATA_SOURCE_ID 不能重复。
     *
     * 说明：
     * 1. DATA_SOURCE_ID 为逗号分隔字符串
     * 2. 使用 DatasourceConfigFetcher.parseCommaSeparatedIds(...) 解析
     * 3. 只要同一个 DATA_SOURCE_ID 出现在多条激活记录中，就视为前置检查失败
     */
    private boolean checkActiveDataSourceIdUniqueness(List<SimpleClientInfo> activeClientRecords) {
        Map<String, List<String>> ownersByDataSourceId = new LinkedHashMap<>();

        for (SimpleClientInfo record : activeClientRecords) {
            String clientId = normalizeText(record.getClientId());
            if (clientId.isEmpty()) {
                continue;
            }

            // 单行的DATA_SOURCE_ID，根据逗号分隔，解析出List
            List<String> dataSourceIds = configFetcher.parseCommaSeparatedIds(record.getDataSourceIds());
            for (String dataSourceId : dataSourceIds) {
                String normalizedDataSourceId = normalizeText(dataSourceId);
                if (normalizedDataSourceId.isEmpty()) {
                    continue;
                }

                ownersByDataSourceId
                        .computeIfAbsent(normalizedDataSourceId, k -> new ArrayList<>())
                        .add(clientId);
            }
        }

        boolean passed = true;

        for (Map.Entry<String, List<String>> entry : ownersByDataSourceId.entrySet()) {
            List<String> owners = entry.getValue();

            if (owners.size() > 1) {
                passed = false;
                logger.warn(
                        "前置检查未通过：CDC_CLIENT_MULTIPLE 表中存在重复的 DATA_SOURCE_ID，DATA_SOURCE_ID={}，涉及 CLIENT_ID={}",
                        entry.getKey(),
                        owners
                );
            }
        }

        return passed;
    }

    /**
     * 前提三：
     * 当前 client 负责的业务库，不能已经被其他 sync-client 占用执行计划。
     *
     * 规则：
     * 1. 检查 CDC_EXECUTION_PLAN.FROM_DATA_SOURCE_ID
     * 2. 如果存在 CLIENT_ID != 当前 clientId 的记录，则视为冲突
     */
    private boolean checkExecutionPlanSourceOwnership(List<SimpleClientInfo> activeClientRecords) throws Exception {
        String currentClientId = normalizeText(clientInfo.getClientId());
        if (currentClientId.isEmpty()) {
            logger.warn("前置检查未通过：当前 ClientInfo.CLIENT_ID 为空，无法检查执行计划归属。");
            return false;
        }

        // 只关注当前 client 自己在 CDC_CLIENT_MULTIPLE 中激活的业务库
        Set<String> currentClientDataSourceIds = activeClientRecords.stream()
                .filter(record -> currentClientId.equals(normalizeText(record.getClientId())))
                .flatMap(record -> configFetcher.parseCommaSeparatedIds(record.getDataSourceIds()).stream())
                .map(this::normalizeText)
                .filter(id -> !id.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (currentClientDataSourceIds.isEmpty()) {
            logger.warn("前置检查未通过：当前 CLIENT_ID={} 在 CDC_CLIENT_MULTIPLE 中没有配置 有效的业务库", currentClientId);
            return false;
        }

        Map<String, Set<String>> conflicts = configFetcher.findExecutionPlanSourceOwnershipConflicts(
                currentClientId,
                new ArrayList<>(currentClientDataSourceIds)
        );

        if (conflicts.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, Set<String>> entry : conflicts.entrySet()) {
            logger.warn(
                    "前置检查未通过：业务库 DATA_SOURCE_ID={} 已被其他 sync-client 占用执行计划，当前 CLIENT_ID={}，占用方 CLIENT_ID={}",
                    entry.getKey(),
                    currentClientId,
                    entry.getValue()
            );
        }

        return false;
    }

    private boolean checkSourceTableSelectPrivileges() throws Exception {
        // subscriberMappingRegistry 未初始化时，无法继续检查
        if (subscriberMappingRegistry == null || subscriberMappingRegistry.isEmpty()) {
            logger.warn("前置检查未通过：subscriberMappingRegistry 尚未初始化，无法检查源表访问权限。");
            return false;
        }

        // businessDataSourceConfigRegistry 未初始化时，无法继续检查
        if (businessDataSourceConfigRegistry == null || businessDataSourceConfigRegistry.isEmpty()) {
            logger.warn("前置检查未通过：businessDataSourceConfigRegistry 尚未初始化，无法检查源表访问权限。");
            return false;
        }

        // 按业务库 dataSourceId 分组收集需要检查的源表
        Map<String, Set<String>> sourceTablesByDataSourceId = new LinkedHashMap<>();

        // 遍历 fromDataSourceId -> SubscriberMapping 列表
        for (Map.Entry<String, List<SubscriberMapping>> entry
                : subscriberMappingRegistry.getMappingMapByFromDataSourceId().entrySet()) {

            // 当前业务库 ID
            String dataSourceId = normalizeText(entry.getKey());
            if (dataSourceId.isEmpty()) {
                continue;
            }

            // 当前业务库对应的所有订阅关系
            List<SubscriberMapping> subscriberMappings = entry.getValue();
            if (subscriberMappings == null || subscriberMappings.isEmpty()) {
                continue;
            }

            // 使用 Set 对当前业务库下的源表去重
            Set<String> fullyQualifiedTables = new LinkedHashSet<>();

            // 遍历当前业务库下的所有订阅关系
            for (SubscriberMapping subscriberMapping : subscriberMappings) {
                if (subscriberMapping == null || subscriberMapping.getSourceTableNameList() == null) {
                    continue;
                }

                // 收集当前订阅关系下的所有源表
                for (Table table : subscriberMapping.getSourceTableNameList()) {
                    if (table == null) {
                        continue;
                    }

                    // 规范化 schema 和 table 名称
                    String schemaName = normalizeText(table.getSchemaName());
                    String tableName = normalizeText(table.getTableName());

                    // schema 或 table 为空时忽略
                    if (schemaName.isEmpty() || tableName.isEmpty()) {
                        continue;
                    }

                    // 组装为 SCHEMA.TABLE 形式，供后续权限检查使用
                    fullyQualifiedTables.add(schemaName + "." + tableName);
                }
            }

            // 当前业务库存在待检查源表时才加入检查列表
            if (!fullyQualifiedTables.isEmpty()) {
                sourceTablesByDataSourceId.put(dataSourceId, fullyQualifiedTables);
            }
        }

        // 调用 DatasourceConfigFetcher 执行业务库表访问权限检查
        Map<String, Set<String>> unreadableTables =
                configFetcher.findUnreadableSourceTables(businessDataSourceConfigRegistry, sourceTablesByDataSourceId);

        // 没有不可访问表，说明检查通过
        if (unreadableTables.isEmpty()) {
            return true;
        }

        // 打印每个业务库下不可访问的表
        for (Map.Entry<String, Set<String>> entry : unreadableTables.entrySet()) {
            logger.warn(
                    "前置检查未通过：业务库 DATA_SOURCE_ID={} 对以下源表没有查询权限，tables={}",
                    entry.getKey(),
                    entry.getValue()
            );
        }

        // 存在任意不可访问表，则检查失败
        return false;
    }
//    /**
//     * 加载作业执行前的必要配置
//     */
//    public void prepareForJobExecution() throws Exception {
//        // 1.启动时，注册客户端，便于配置端分配数据源/读取已经配置的数据源
//        configFetcher.registerClientIfAbsent(clientInfo);
//
//        // 2.从配置库读取 业务库数据源，订阅关系，所有目标库
//        do {
//            // 短路检查：任何一个配置获取失败都立即重试
//            if (loadAllConfigurations()) {  // 如果所有配置都加载成功
//                break;                      // 跳出循环
//            }
//        } while (!checkPreconditions());  // 检查必要的配置
//    }
//
//

//
//    /**
//     * 加载所有必要配置
//     *
//     * @return 配置是否已完整加载
//     */
//    private boolean loadAllConfigurations() throws Exception {
//        // 1. 读取当前客户端负责的所有业务库，每个业务库对应一个BusinessDataSourceConfig
//        this.businessDataSourceConfigRegistry = configFetcher.getClientBasicBusinessDataSourceConfig();
//        if (this.businessDataSourceConfigRegistry.isEmpty()) {
//            logger.warn("当前客户端未被配置 任何业务库数据源，将在10秒后重试");
//            this.jobStatus = JobStatus.CONFIG_MISSING.withDetailInfo("当前客户端未被配置 任何业务库数据源，将在10秒后重试");
//            clientProcessZkService.updateStatus(this.jobStatus);
//            Thread.sleep(10000);
//            return false;  // 短路返回
//        }
//
//        // .读取当前业务库的订阅关系,为了安全起见，把所有 当前业务库 的订阅关系都读出来,
//        // 订阅表中可能存在的一对一，一对多，多对多等乱七八糟的关系，这里全部解析成一对一的关系，最后还是合并成一个SubscriberMappingRegistry，其持有多个SubscriberMapping
//        this.subscriberMappingRegistry = configFetcher.getSubscriberMapping();
//        if (this.subscriberMappingRegistry.isEmpty()) {
//            logger.warn("订阅关系配置缺失，将在10秒后重试");
//            this.jobStatus = JobStatus.CONFIG_MISSING.withDetailInfo("当订阅关系配置缺失，将在10秒后重试");
//            clientProcessZkService.updateStatus(this.jobStatus);
//            Thread.sleep(10000);
//            return false;
//        }
//
//        // 3. 读取所有目标库
//        this.targetDataSourceConfigRegistry = configFetcher.getTargetDataSourceConfigRegistry();
//        if (this.targetDataSourceConfigRegistry.isEmpty()) {
//            logger.warn("目标数据源配置缺失，将在10秒后重试");
//            this.jobStatus = JobStatus.CONFIG_MISSING.withDetailInfo("目标数据源配置缺失，将在10秒后重试");
//            clientProcessZkService.updateStatus(this.jobStatus);
//            Thread.sleep(10000);
//            return false;
//        }
//
//        return true;
//    }



    /**
     * 加载作业执行前的必要配置
     *
     * 执行流程：
     * 1. 启动时先注册当前 client，便于配置端分配数据源
     * 2. 循环尝试加载所有必要配置
     * 3. 如果配置不完整，则把“缺失类型 + 详细说明”交给上层处理
     * 4. 间隔 10 秒继续重试，直到配置完整为止
     *
     * 设计说明：
     * 1. 当前方法不再直接写 client 进程级状态
     * 2. 当前方法只负责：
     *    - 调用 loadAllConfigurations()
     *    - 根据结果决定是否重试
     * 3. client 进程级状态由上层 ClientJobDispatcher 根据 LoadConfigResult 决定如何写入
     */
    public void prepareForJobExecution() throws Exception {


        // 2. 循环加载配置，直到成功
        while (true) {
            LoadConfigResult result = loadAllConfigurations();

            // 3. 所有必要配置都已加载完成，结束重试
            if (result.isSuccess()) {
                return;
            }

            // 4. 将当前“配置缺失结果”交给上层处理
            //
            // 说明：
            // 1. OracleToKafka 不再直接写 /clients/{clientId}/status
            // 2. 由 ClientJobDispatcher 决定如何将 LoadConfigResult 映射为 ClientProcessStatus
            if (clientJobDispatcher != null) {
                clientJobDispatcher.handleLoadConfigResult(result);
            }

            // 5. 间隔 10 秒后继续重试
            Thread.sleep(10000);
        }
    }


    /**
     * 加载所有必要配置
     *
     * 返回值说明：
     * 1. success()：所有必要配置已加载完成
     * 2. BUSINESS_DATA_SOURCE_MISSING：当前 client 未分配任何业务库
     * 3. SUBSCRIBER_MAPPING_MISSING：订阅关系缺失
     * 4. TARGET_DATA_SOURCE_MISSING：目标数据源配置缺失
     *
     * 设计说明：
     * 1. 当前方法只负责“判断并返回结果”
     * 2. 不再直接写 client 进程级状态
     * 3. 不再通过 this.jobStatus 表达配置缺失
     * 4. 具体 detailInfo 由 OracleToKafka 在这里生成
     * 5. 上层 ClientJobDispatcher 再根据 LoadConfigResult 映射到 ClientProcessStatus
     */
    private LoadConfigResult loadAllConfigurations() throws Exception {
        // 1. 读取当前客户端负责的所有业务库
        //
        // 说明：
        // 每个业务库对应一个 BusinessDataSourceConfig。
        this.businessDataSourceConfigRegistry = configFetcher.getClientBasicBusinessDataSourceConfig();
        if (this.businessDataSourceConfigRegistry.isEmpty()) {
            String detailInfo = "当前客户端未被分配任何业务库数据源，将在10秒后重试";
            logger.warn(detailInfo);

            return LoadConfigResult.of(
                    LoadConfigResultType.BUSINESS_DATA_SOURCE_MISSING,
                    detailInfo
            );
        }

        // 2. 读取当前所有业务库的订阅关系
        //
        // 说明：
        // 这里会把当前 client 所涉及到的全部订阅关系读出来，
        // 最终统一整理成 SubscriberMappingRegistry。
        this.subscriberMappingRegistry = configFetcher.getSubscriberMapping();
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {
            if (this.subscriberMappingRegistry.findSubscriberMappingByDataFromSourceId(businessDataSourceConfig) == null) {
                String detailInfo = businessDataSourceConfig.getDataSourceId() + " -> " + businessDataSourceConfig.getDataSourceOrg() +
                                   "订阅关系配置缺失，将在10秒后重试";
                logger.warn(detailInfo);

                return LoadConfigResult.of(
                        LoadConfigResultType.SUBSCRIBER_MAPPING_MISSING,
                        detailInfo
                );
            }
        }


        // 3. 读取当前所有业务库涉及到的目标数据源配置
        this.targetDataSourceConfigRegistry = configFetcher.getTargetDataSourceConfigRegistry();
        if (this.targetDataSourceConfigRegistry.isEmpty()) {
            String detailInfo = "目标数据源配置缺失，将在10秒后重试";
            logger.warn(detailInfo);

            return LoadConfigResult.of(
                    LoadConfigResultType.TARGET_DATA_SOURCE_MISSING,
                    detailInfo
            );
        }





        // 4. 所有必要配置加载成功
        return LoadConfigResult.success();
    }

    /**
     * 检查必要条件
     *
     * 当前保持占位实现。
     */
    public boolean checkPreconditions() {
        return false;
    }

    /**
     * 初始化客户端执行环境
     *
     * 包括：
     * 1. 构建业务库 -> 订阅关系 -> 目标库之间的关联
     * 2. 为每个业务库构建表配置并加载主键
     * 3. 初始化 ClientController 的元数据读取器
     */
    public void initClientExecutionEnvironment() throws Exception {
        // 1. 处理业务库侧的表配置与主键
        postProcessBusinessDataSource();

        // 2. 初始化 Oracle 元数据 reader
        clientController.initializeMetadataReader(this.businessDataSourceConfigRegistry);

        // 3. 记录客户端启动时间
        clientInfo.notifyClientStart();

        logger.info("Client started at {}", clientInfo.printStartTime());
    }

    /**
     * 后处理业务库配置
     *
     * 对每个业务库：
     * 1. 注入订阅关系
     * 2. 注入目标库注册表
     * 3. 初始化表映射
     * 4. 为表分配主键/唯一键
     */
    private void postProcessBusinessDataSource() throws Exception {

        for (BusinessDataSourceConfig businessDataSourceConfig : this.businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {

            // 1.建立 生产库，订阅关系，目标库之间的关系
            businessDataSourceConfig.setSubscriberMappingList(this.subscriberMappingRegistry.findSubscriberMappingByDataFromSourceId(businessDataSourceConfig));
//            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);

            // 2.为每个源表，创建对应的 TableMappingConfig
            businessDataSourceConfig.init();
            // 3.表的主键设置到每一个 TableMappingConfig 中
            businessDataSourceConfig.assignPrimaryKeysOrUniqueKeysForAllTables();
        }
    }

//    /**
//     * 初始化 Kafka Topics
//     *
//     * 注意：
//     * 这里仍按“客户端全局执行计划”生成所有 topic。
//     * 每个业务库自己的 job 启动时，只会使用自己 scoped plan 对应的 topic 子集。
//     */
//    public void initKafkaTopics() {
//        // 1.万事俱备，生成 客户端 必须的主题名
//        topicInfoRegistry.registerClientTopicInfo(this.executionPlan.generateClientTopicInfo(clientInfo));
//        // 2.自动创建每个需要同步的表对应的Kafka Topic，一个源表可能对应主题
//        kafkaUtils.createKafkaTopicsIfNotExists(topicInfoRegistry.getClientTopicInfoList());
//    }

    // 为每个业务库构建运行时上下文
    private void buildRuntimes() throws Exception {
        // 清空当前进程内已有 runtime，避免重复启动时残留旧状态
        runtimeByDataSourceId.clear();

        // 遍历当前 client 负责的所有业务库
        for (BusinessDataSourceConfig businessDataSourceConfig
                : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {

            // 当前业务库 ID
            String dataSourceId = businessDataSourceConfig.getDataSourceId();

            // 从全局执行计划中过滤出当前业务库自己的执行计划
            ExecutionPlan scopedExecutionPlan = filterExecutionPlanByDataSource(dataSourceId);

            // 如果当前业务库没有任何需要同步的表，则跳过
            if (scopedExecutionPlan.getTableExecutionPlans().isEmpty()) {
                logger.warn("数据源：dataSourceId={},{}，未配置任何需要同步的表，忽略当前数据源!!",
                        dataSourceId,
                        businessDataSourceConfig.getDataSourceOrg());
                continue;
            }

            // 从 CDC_DATA_SOURCE_RUN_STATE 判断当前业务库应该以 SNAPSHOT 还是 NORMAL 模式启动
            JobStartupMode startupMode = determineStartupMode(businessDataSourceConfig);

            // 生成当前业务库的全部 Kafka topic 信息
            // 注意：这里不再根据 CDC_EXECUTION_PLAN.TABLE_STATUS 过滤 snapshot 表
            // 因为当前恢复策略已经改为业务库级 snapshot 状态
            List<ClientTopicInfo> topicInfos =
                    topicInfoService.generateClientTopicInfo(scopedExecutionPlan.getTableExecutionPlans());



//             // SNAPSHOT 模式且启用自动清理时，执行删除并重建 topic 的完整流程。
//            if (startupMode == JobStartupMode.SNAPSHOT && clientInfo.isCleanTopicsBeforeSnapshot()) {
//                // 记录当前业务库即将执行 Kafka topic 重建。
//                logger.warn("SNAPSHOT 模式且启用自动清理，将删除并重建 Kafka topics. clientId={}, dataSourceId={}, topicCount={}",
//                        clientInfo.getClientId(),
//                        dataSourceId,
//                        topicInfos.size());
//
//                // 危险操作执行前倒计时。
//                // 倒计时期间只输出日志，不做交互式确认；如果发现配置或目标 topic 异常，可以手动终止当前进程。
//                DangerousOperationCountdown.beforeRecreateKafkaTopics(
//                        clientInfo,
//                        dataSourceId,
//                        topicInfos
//                );
//
//                // 删除当前业务库对应 topics，并持续重试直到这些 topics 可以重新创建。
//                kafkaUtils.recreateKafkaTopicsForSnapshot(
//                        topicInfos,
//                        clientInfo.getCleanTopicsBeforeSnapshotTimeoutMinutes(),
//                        clientInfo.getCleanTopicsBeforeSnapshotRetryIntervalSeconds()
//                );
//            } else {
//                // 非自动清理场景只保证 topic 存在。
//                kafkaUtils.createKafkaTopicsIfNotExists(topicInfos);
//            }


            // SNAPSHOT 模式且启用自动清理时，根据配置选择清理方式
            if (startupMode == JobStartupMode.SNAPSHOT && clientInfo.isCleanTopicsBeforeSnapshot()) {
                // 解析 topic 清理方式
                KafkaTopicCleanMode cleanMode = clientInfo.resolveCleanTopicsBeforeSnapshotMode();

                // 记录当前业务库即将执行 Kafka topic 清理
                logger.warn("SNAPSHOT模式，且启用自动清理 Kafka topics，clientId={}, dataSourceId={}, 清理模式={}, topicCount={}",
                        clientInfo.getClientId(),
                        dataSourceId,
                        cleanMode == KafkaTopicCleanMode.CLEAR_DATA ? "清空 Kafka topic 数据并保留 topics" : "只删除topic数据，不删除topic\"",
                        topicInfos.size());

                // 危险操作执行前倒计时
                DangerousOperationCountdown.beforeCleanKafkaTopics(
                        clientInfo,
                        dataSourceId,
                        topicInfos,
                        cleanMode
                );

                // 根据清理方式执行 topic 清理
                cleanKafkaTopicsBeforeSnapshot(topicInfos, cleanMode);
            } else {
                // 非自动清理场景只保证 topic 存在
                kafkaUtils.createKafkaTopicsIfNotExists(topicInfos);
            }


            // 构造当前业务库对应的运行时上下文
            ClientJobRuntime runtime = new ClientJobRuntime(
                    businessDataSourceConfig,
                    scopedExecutionPlan,
                    topicInfos,
                    startupMode
            );

            // 按 dataSourceId 保存 runtime
            runtimeByDataSourceId.put(dataSourceId, runtime);
        }
    }

    // SNAPSHOT 启动前按配置清理 Kafka topics
    private void cleanKafkaTopicsBeforeSnapshot(List<ClientTopicInfo> topicInfos,
                                                KafkaTopicCleanMode cleanMode) {
        // 删除并重建 topic
        if (cleanMode == KafkaTopicCleanMode.RECREATE_TOPIC) {
            kafkaUtils.recreateKafkaTopicsForSnapshot(
                    topicInfos,
                    clientInfo.getCleanTopicsBeforeSnapshotTimeoutMinutes(),
                    clientInfo.getCleanTopicsBeforeSnapshotRetryIntervalSeconds()
            );
            return;
        }

        // 保留 topic，仅清除 topic 数据
        if (cleanMode == KafkaTopicCleanMode.CLEAR_DATA) {
            kafkaUtils.clearKafkaTopicDataForSnapshot(
                    topicInfos,
                    clientInfo.getCleanTopicsBeforeSnapshotTimeoutMinutes(),
                    clientInfo.getCleanTopicsBeforeSnapshotRetryIntervalSeconds()
            );
            return;
        }

        // 未知清理方式直接失败
        throw new IllegalStateException("Unsupported Kafka topic clean mode: " + cleanMode);
    }

    /**
     * 从全局执行计划中，过滤出某个业务库自己的 scoped execution plan
     */
    private ExecutionPlan filterExecutionPlanByDataSource(String dataSourceId) {
        ExecutionPlan scoped = new ExecutionPlan();

        List<ExecutionPlan.TableExecutionPlan> filtered = this.executionPlan.getTableExecutionPlans()
                .stream()
                .filter(plan -> dataSourceId.equals(plan.getFromDataSourceId()))
                .collect(Collectors.toList());

        for (ExecutionPlan.TableExecutionPlan plan : filtered) {
            scoped.getAddedTableExecutionPlan().add(plan);
        }

        scoped.applyChanges();
        return scoped;
    }

    // 根据 CDC_DATA_SOURCE_RUN_STATE 判断当前业务库启动模式
    private JobStartupMode determineStartupMode(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
        String dataSourceId = businessDataSourceConfig.getDataSourceId();
        String dataSourceOrg = businessDataSourceConfig.getDataSourceOrg();

        // 启动时先确保 CDC_DATA_SOURCE_RUN_STATE 中存在当前 client + dataSource 的记录
        // 如果已经存在，ensureRunStateExists方法不做任何处理
        // 如果不存在，则插入一条默认 SNAPSHOT_RUNNING 记录
        dataSourceRunStateRepository.ensureRunStateExists(clientInfo.getClientId(), dataSourceId);

        // 查询当前业务库 snapshot 是否已经完成
        boolean snapshotCompleted =
                dataSourceRunStateRepository.isSnapshotCompleted(clientInfo.getClientId(), dataSourceId);

        // 如果 snapshot 已经完成，则使用 NORMAL 模式
        // 后续会允许从 Kafka 最新消息中读取 SCN
        if (snapshotCompleted) {
            logger.info("dataSourceId={}, dataSourceOrg={}，快照任务已经完成，执行 增量模式",
                    dataSourceId,
                    dataSourceOrg);
            return JobStartupMode.NORMAL;
        }

        // 如果 snapshot 未完成，则标记为 SNAPSHOT_RUNNING
        // 这表示本次启动会重新执行完整快照
        dataSourceRunStateRepository.markSnapshotRunning(clientInfo.getClientId(), dataSourceId);

        // 返回 SNAPSHOT 模式
        logger.info("dataSourceId={}, dataSourceOrg={}，快照任务未完成，执行 快照模式",
                dataSourceId,
                dataSourceOrg);
        return JobStartupMode.SNAPSHOT;
    }


    // 启动所有业务库对应的 Flink job。
    private void startAllDataSourceJobs() throws Exception {
        // 逐个启动当前客户端持有的 dataSource runtime。
        for (ClientJobRuntime runtime : runtimeByDataSourceId.values()) {
            // 首次启动需要清空历史 scn，避免旧运行态残留。
            startSingleDataSourceJob(runtime, JobLaunchMode.INITIAL_START);
        }
    }



    /**
     * 启动单个业务库对应的 Flink Job
     *
     * 这是多数据源版本的核心方法。
     *
     * 当前版本改造点：
     * 1. 不再为每个业务库单独创建 createLocalEnvironmentWithWebUI(...)
     * 2. 不再为每个业务库单独起一个本地 MiniCluster
     * 3. 改为：
     *    - 为当前业务库构建自己的 job graph
     *    - 再提交到 ClientJobDispatcher 持有的共享 MiniCluster
     *
     * 执行流程：
     * 1. 初始化当前业务库自己的 job 级 ZK 节点
     * 2. 生成当前业务库自己的 jobId
     * 3. 构建当前 job 的 Flink 配置
     * 4. 创建当前业务库自己的 StreamExecutionEnvironment
     * 5. 构建 Oracle CDC Source
     * 6. 挂载日志/SCN 更新逻辑
     * 7. 如为 snapshot 模式，则挂载 snapshot 控制信号处理
     * 8. 分流写入 Kafka
     * 9. 将当前 job 提交到共享 MiniCluster
     * 10. 提交成功后写入当前 job 的运行状态
     */
    public void startSingleDataSourceJob(ClientJobRuntime runtime, JobLaunchMode launchMode) throws Exception {
        // 启动场景不能为空，避免 scn 清理策略不明确。
        if (launchMode == null) {
            throw new IllegalArgumentException("JobLaunchMode cannot be null");
        }

        // 获取当前业务库配置。
        BusinessDataSourceConfig businessDataSourceConfig = runtime.getBusinessDataSourceConfig();

        // 获取当前业务库 ID。
        String dataSourceId = businessDataSourceConfig.getDataSourceId();

        // 获取当前业务库机构名称。
        String dataSourceOrg = businessDataSourceConfig.getDataSourceOrg();

        // 记录当前 job 启动场景。
        logger.info("数据源 -----> {},{} 开始采集, launchMode={}, clearScnBeforeStart={}",
                dataSourceId,
                dataSourceOrg,
                launchMode,
                launchMode.isClearScnBeforeStart());

        // 初始化当前业务库自己的 job 级 ZK 节点，并按启动场景决定是否清空 scn。
        initJobLevelZkNodes(dataSourceId, dataSourceOrg, launchMode);

        // 2. 生成当前业务库自己的 jobId
        String jobIdHex = buildJobId(dataSourceId);

        // 3. 构建当前 job 的 Flink 配置
        //
        // 注意：
        // 这里现在只构建“job 级配置”，不再混入 Web UI 端口这类 cluster 级配置。
        Configuration jobConfiguration = buildFlinkConfiguration(runtime, jobIdHex);

        // 4. 创建当前业务库自己的 StreamExecutionEnvironment
        //
        // 注意：
        // 1. 这里创建的是“job graph 构建环境”
        // 2. 不再使用 createLocalEnvironmentWithWebUI(...)
        // 3. 真正运行 job 的 cluster 是共享 MiniCluster
        StreamExecutionEnvironment env = createJobEnvironment(jobConfiguration);
        env.setParallelism(1);



        // 为当前业务库生成唯一的 Debezium logical server name
        //
        // 说明：
        // 1. 该值必须在当前 JVM 内唯一
        // 2. 这里使用 clientId + dataSourceId 组合，避免多个 job 的 metrics 名称冲突
        String logicalServerName = clientInfo.getClientId() + "_" + dataSourceId;



        // 快照模式/或 根据当前业务库 runtime 构造 Oracle CDC 启动参数(主要是断点)
        StartupOptions startupOptions = buildStartupOptions(runtime);


        // 获取从 Kafka 解析出来的 Oracle CDC 启动位点。
        OracleStartupOffset kafkaStartupOffset = runtime.getKafkaStartupOffset();

        // snapshot 控制消息侧输出标签
        OutputTag<String> snapshotControlOutputTag =
                new OutputTag<String>("snapshot-control-output-" + dataSourceId) {};

        // 6. 构建 Oracle CDC Source
        logger.info("Creating Oracle CDC Source, host={}, port={}, service={}, dataSourceId={}",
                businessDataSourceConfig.getDataSourceHost(),
                businessDataSourceConfig.getDataSourcePort(),
                businessDataSourceConfig.getDataSourceServiceName(),
                dataSourceId);

        for (int i = 5; i > 0; i--) {
            System.out.println("倒计时：" + i + " 秒");
            Thread.sleep(1000);
        }
        System.out.println("倒计时结束");

        SourceFunction<String> oracleSource = OracleSource.<String>builder()
                // 设置 Oracle 主机。
                .hostname(businessDataSourceConfig.getDataSourceHost())
                // 设置 Oracle 端口。
                .port(businessDataSourceConfig.getDataSourcePort())
                // 设置 Oracle 用户名。
                .username(businessDataSourceConfig.getDataSourceUserName())
                // 设置 Oracle 密码。
                .password(businessDataSourceConfig.getDataSourcePassword())
                // 设置 Oracle service name。
                .database(businessDataSourceConfig.getDataSourceServiceName())
                // 设置当前 job 需要同步的表。
                .tableList(getCurrentJobSyncTables(runtime))
                // 设置 Oracle JDBC URL。
                .url(businessDataSourceConfig.getUrl())
//                .startupOptions(StartupOptions.initial())
                // 设置启动模式。
                .startupOptions(startupOptions)

                // 设置 Oracle CDC 指定启动位点。
                .specificStartupOffset(
                        kafkaStartupOffset == null ? null : kafkaStartupOffset.getScn(),
                        kafkaStartupOffset == null ? null : kafkaStartupOffset.getCommitScn()
                )
                // 设置 Debezium 参数。
                .debeziumProperties(DebeziumProperties.getDebeziumProperties(logicalServerName))
                // 设置 Debezium 反序列化器。
                .deserializer(new CustomDebeziumDeserializationSchema(businessDataSourceConfig, clientInfo))
                // 构建 SourceFunction。
                .build();

        SingleOutputStreamOperator<String> oracleStream = env.addSource(oracleSource)
                .uid("oracle-cdc-source-" + dataSourceId)
                .process(new OracleSourceLogRichFunction(this.clientInfo, dataSourceId, dataSourceOrg,snapshotControlOutputTag))
                .uid("oracle-cdc-map-" + dataSourceId)
                .name("Oracle-CDC-Source-" + dataSourceId);


        // 从 OracleSourceLogRichFunction 中取出 snapshot 完成控制信号侧输出流
        DataStream<String> snapshotControlStream =
                oracleStream.getSideOutput(snapshotControlOutputTag);

         // 只有快照启动模式才需要处理 snapshot 完成标记
        if (runtime.getStartupMode() == JobStartupMode.SNAPSHOT) {
            configureSnapshotCompleteMarking(snapshotControlStream, dataSourceId);
        }

        // 8. 根据配置选择 Kafka 写入模式
        toKafkaSink(oracleStream, runtime);

        // 9. 将当前业务库 job 提交到共享 MiniCluster
        //
        // 注意：
        // 1. 不再调用 env.executeAsync(...)
        // 2. 这里显式提交到 ClientJobDispatcher 持有的共享 MiniCluster
        String jobName = "oracle-cdc-" + clientInfo.getClientId() + "-" + dataSourceId;
        // 提交当前业务库 job 到共享 MiniCluster
        JobClient jobClient = submitJobToSharedMiniCluster(env, jobName, jobIdHex);
        // 保存当前业务库 job client
        runtime.setJobClient(jobClient);

        // 注册当前业务库 job 的失败监听
        jobFailureZkReporter.watch(dataSourceId, jobClient);

        logger.info("Flink job submitted to shared MiniCluster, jobName={}, jobId={}, dataSourceId={}",
                jobName, jobClient.getJobID(), dataSourceId);



         // 根据启动模式写入当前 job 运行状态
        updateCurrentJobRunningStatus(dataSourceId, runtime.getStartupMode());
    }




    // 生成当前业务库本次 Flink 提交使用的 JobID。
    private String buildJobId(String dataSourceId) {
        // 每次提交都生成新的 Flink JobID，避免 MiniCluster 中已完成 job 的 JobID 与新提交冲突。
        String jobIdHex = JobIdUtil.generateRandomJobId();

        // dataSourceId 仍然是业务固定身份，Flink JobID 只是单次执行实例身份。
        logger.info("Generated new Flink JobID for dataSourceId={}, clientId={}, jobIdHex={}",
                dataSourceId,
                clientInfo.getClientId(),
                jobIdHex);

        // 返回本次提交使用的 JobID。
        return jobIdHex;
    }

    /**
     * 构建当前业务库 job 的 Flink 配置。
     *
     * 包括：
     * 1. 注入本次提交使用的 Flink JobID。
     * 2. 设置 job 级默认并行度。
     *
     * 说明：
     * 1. Flink JobID 是单次提交实例身份，每次提交都重新生成。
     * 2. dataSourceId 才是业务固定身份，用于 ZK、Kafka offset、日志和运行状态。
     * 3. 不能在同一个 MiniCluster 生命周期内重复提交相同 JobID，否则已完成 job 会触发 DuplicateJobSubmissionException。
     */
    private Configuration buildFlinkConfiguration(ClientJobRuntime runtime, String jobIdHex) throws Exception {
        Configuration configuration = new Configuration();

        // 1. 注入当前 job 的固定 JobID
        //
        // 说明：
        // 1. NORMAL 模式下通常是固定 JobID，用于 checkpoint 恢复
        // 2. SNAPSHOT 模式下通常是随机 JobID，用于一次性快照任务
        configuration.setString("$internal.pipeline.job-id", jobIdHex);

        // 2. 设置默认并行度
        configuration.setString("parallelism.default", "1");

//        // 3. NORMAL 模式下，检查是否存在可恢复的 checkpoint
//        //
//        // 说明：
//        // 1. 只有增量任务需要尝试从历史 checkpoint 恢复
//        // 2. snapshot 任务通常不走 checkpoint 恢复
//        if (runtime.getStartupMode() == JobStartupMode.NORMAL) {
//            String latestCheckpointPathWithMetadata = findLatestCheckpointPathWithMetadata(
//                    stateBackendProperties.getCheckpointPath(),
//                    jobIdHex
//            );
//
//            if (latestCheckpointPathWithMetadata != null) {
//                configuration.setString("execution.savepoint.path", latestCheckpointPathWithMetadata);
//
//                logger.info("从检查点恢复, dataSourceId={}, checkpoint={}",
//                        runtime.getDataSourceId(),
//                        latestCheckpointPathWithMetadata);
//            } else {
//                logger.info("没有找到检查点，将从头开始, dataSourceId={}",
//                        runtime.getDataSourceId());
//            }
//        }


        // 明确记录当前 job 不再使用 Flink checkpoint/savepoint 恢复
        logger.info("Flink checkpoint/savepoint recovery disabled, dataSourceId={}, jobIdHex={}",
                runtime.getDataSourceId(),
                jobIdHex);

        return configuration;
    }




     // 配置 snapshot 完成控制信号处理逻辑
    private void configureSnapshotCompleteMarking(DataStream<String> snapshotControlStream,
                                                  String dataSourceId) {
        // 控制信号理论上每个业务库只会出现一次，单并发即可
        snapshotControlStream
                .addSink(new SnapshotCompleteJdbcSink(
                        this.configDataSourceConfig,
                        this.clientInfo.getClientId(),
                        dataSourceId,
                        this.clientInfo.getSnapshotCompleteMarkDelaySeconds()
                ))
                .uid("snapshot-complete-jdbc-sink-" + dataSourceId)
                .name("Snapshot-Complete-Jdbc-Sink-" + dataSourceId)
                .setParallelism(1);
    }


    // 根据 client-info.kafka-write-mode 选择 Kafka 写入方式
    private void toKafkaSink(DataStream<String> oracleStream, ClientJobRuntime runtime) {
        // 解析 Kafka 写入模式
        KafkaWriteMode kafkaWriteMode = clientInfo.resolveKafkaWriteMode();

        // 记录当前写入模式
        logger.info("Kafka write mode resolved, clientId={}, dataSourceId={}, mode={}",
                clientInfo.getClientId(),
                runtime.getDataSourceId(),
                kafkaWriteMode);

        // 如果是顺序模式，则使用单一有序 sink
        if (kafkaWriteMode == KafkaWriteMode.ORDERED) {
            // 使用顺序路由 sink 写入 Kafka
            toOrderedKafkaSink(oracleStream, runtime);

            // 结束处理
            return;
        }

        // 高吞吐模式下提示恢复语义风险
        logger.warn("Kafka write mode is PARALLEL, cross-topic SCN order is not guaranteed. dataSourceId={}",
                runtime.getDataSourceId());

        // 使用原有 side output 并发写入模式
        toSinkWithSideOutput(oracleStream, runtime);
    }

    // 使用单一自定义 sink 按输入顺序写入 Kafka
    private void toOrderedKafkaSink(DataStream<String> oracleStream, ClientJobRuntime runtime) {
        // 构建 schema.table 到 topic 的路由表
        Map<String, String> topicByTable = buildTopicByTable(runtime);

        // 添加单一顺序 Kafka sink
        oracleStream
                .addSink(new OrderedKafkaRoutingSink(
                        kafkaUtils.getKafkaBootstrapServers(),
                        topicByTable
                ))
                .name("Ordered-Kafka-Routing-Sink-" + runtime.getDataSourceId())
                .uid("ordered-kafka-routing-sink-" + runtime.getDataSourceId())
                .setParallelism(1);
    }

    // 构建当前业务库的表名到 Kafka topic 的映射
    private Map<String, String> buildTopicByTable(ClientJobRuntime runtime) {
        // 创建有序映射，便于日志和排查
        Map<String, String> topicByTable = new LinkedHashMap<>();

        // 遍历当前业务库的 topic 配置
        for (ClientTopicInfo topicInfo : runtime.getTopicInfos()) {
            // 获取全限定表名
            String fullyQualifiedTableName = topicInfo.getFullyQualifiedTableName();

            // 获取 Kafka topic
            String topic = topicInfo.getTopic();

            // 校验全限定表名
            if (fullyQualifiedTableName == null || fullyQualifiedTableName.trim().isEmpty()) {
                throw new IllegalArgumentException("fullyQualifiedTableName is blank, dataSourceId=" + runtime.getDataSourceId());
            }

            // 校验 topic
            if (topic == null || topic.trim().isEmpty()) {
                throw new IllegalArgumentException("topic is blank, table=" + fullyQualifiedTableName);
            }

            // 写入路由表
            topicByTable.put(fullyQualifiedTableName, topic);
        }

        // 记录路由数量
        logger.info("Kafka topic route built, dataSourceId={}, routeCount={}",
                runtime.getDataSourceId(),
                topicByTable.size());

        // 返回路由表
        return topicByTable;
    }

    /**
     * 使用侧输出流方式将数据分流到不同 Kafka Topic
     *
     * 当前版本：
     * 1. 只使用当前 runtime 自己的 topicInfos
     * 2. 不使用其它业务库的 topic
     */
    public void toSinkWithSideOutput(DataStream<String> oracleStream, ClientJobRuntime runtime) {
        Map<String, List<OutputTag<String>>> outputTagMap =
                generateOutputTagMap(runtime.getTopicInfos());

        SingleOutputStreamOperator<String> mainStream = oracleStream
                .process(new SideOutputProcessFunction(outputTagMap))
                .name("SideOutput-Split-" + runtime.getDataSourceId());

        List<OutputTag<String>> outputTagList = outputTagMap.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        for (OutputTag<String> outputTag : outputTagList) {
            DataStream<String> tableStream = mainStream.getSideOutput(outputTag);
            String topic = outputTag.getId();

            tableStream
                    .sinkTo(KafkaSink.<String>builder()
                            .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(topic)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build())
                            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            .setProperty("batch.size", "262144")
                            .setProperty("linger.ms", "100")
                            .setProperty("buffer.memory", "67108864")
                            .setProperty("compression.type", "lz4")
                            .setProperty("retries", String.valueOf(Integer.MAX_VALUE))
                            .setProperty("retry.backoff.ms", "1000")
                            .setProperty("reconnect.backoff.ms", "1000")
                            .setProperty("reconnect.backoff.max.ms", "10000")
                            .setProperty("request.timeout.ms", "30000")
                            .setProperty("delivery.timeout.ms", String.valueOf(Integer.MAX_VALUE))
                            .setProperty("acks", "1")
                            .build())
                    .name("KafkaSink-" + runtime.getDataSourceId() + "-" + topic);
        }
    }

    /**
     * 根据当前 runtime 的 topicInfos 生成 outputTagMap
     *
     * key: fullyQualifiedTableName
     * value: 当前表对应的所有 OutputTag
     */
    private Map<String, List<OutputTag<String>>> generateOutputTagMap(List<ClientTopicInfo> topicInfos) {
        Map<String, List<OutputTag<String>>> outputTagMap = new HashMap<>();

        for (ClientTopicInfo clientTopicInfo : topicInfos) {
            String fullyQualifiedTableName = clientTopicInfo.getFullyQualifiedTableName();
            String topic = clientTopicInfo.getTopic();

            outputTagMap
                    .computeIfAbsent(fullyQualifiedTableName, k -> new ArrayList<>())
                    .add(new OutputTag<>(topic, Types.STRING));
        }

        return outputTagMap;
    }

    /**
     * 获取当前业务库 job 需要同步的表名数组
     */
    public String[] getCurrentJobSyncTables(ClientJobRuntime runtime) {
        return runtime.getTopicInfos()
                .stream()
                .map(ClientTopicInfo::getFullyQualifiedTableName)
                .distinct()
                .toArray(String[]::new);
    }









    /**
     * snapshot 完成后的后处理
     *
     * 逻辑：
     * 1. 只处理当前业务库下的 snapshot 表
     * 2. 将这些表的状态切换为 NORMAL
     * 3. 不影响其它业务库
     */
    public void afterSnapshotCompletion(String dataSourceId) throws Exception {
        List<ExecutionPlan.TableExecutionPlan> snapshotTableExecutionPlanList =
                this.executionPlan.getTableExecutionPlans()
                        .stream()
                        .filter(plan -> dataSourceId.equals(plan.getFromDataSourceId()))
                        .filter(plan -> TableStatus.SNAPSHOT == plan.getTableStatus())
                        .collect(Collectors.toList());

        configFetcher.batchUpdateExecutionPlans(snapshotTableExecutionPlanList);
    }


    /**
     * 为单个业务库 job 创建 StreamExecutionEnvironment
     *
     * 当前版本改造点：
     * 1. 这里创建的 env 只用于“构建当前 job 的 job graph”
     * 2. 不再使用 createLocalEnvironmentWithWebUI(...)
     * 3. 不再在这里隐式创建新的本地 MiniCluster
     * 4. 真正运行这些 job 的，是 ClientJobDispatcher 持有的共享 MiniCluster
     *
     * 说明：
     * 1. 这里仍然保留按 job 维度创建 env 的方式
     * 2. 但 cluster 不再按 job 维度创建
     * 3. 也就是说：一个 job 一个 env，多个 job 共享一个 MiniCluster
     */
    private StreamExecutionEnvironment createJobEnvironment(Configuration jobConfiguration) {
        // 直接基于当前 job 的配置创建执行环境
        //
        // 注意：
        // 这里的 env 只承担 job graph 构建职责，
        // 后续不会用 env.executeAsync() 直接起本地 cluster。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(jobConfiguration);

        logger.info("Created StreamExecutionEnvironment for job graph building on shared MiniCluster.");
        return env;
    }
    /**
     * 将当前业务库 job 提交到共享 MiniCluster
     *
     * 当前版本改造点：
     * 1. 不再使用 env.executeAsync(...)
     * 2. 改为显式获取共享 MiniCluster
     * 3. 先把 env 转成 StreamGraph
     * 4. 再把 StreamGraph 转成 JobGraph
     * 5. 显式指定 JobID，保证 NORMAL 模式下固定 jobId 生效
     * 6. 最后提交到共享 MiniCluster，并构造 JobClient 返回
     *
     * @param env 当前业务库 job 对应的执行环境
     * @param jobName 当前业务库 job 名称
     * @param jobIdHex 当前业务库 job 对应的十六进制 JobID
     * @return 当前 job 对应的 JobClient
     */
    private JobClient submitJobToSharedMiniCluster(StreamExecutionEnvironment env,
                                                   String jobName,
                                                   String jobIdHex) throws Exception {
        if (env == null) {
            throw new IllegalArgumentException("env must not be null");
        }
        if (jobName == null || jobName.trim().isEmpty()) {
            throw new IllegalArgumentException("jobName must not be blank");
        }
        if (jobIdHex == null || jobIdHex.trim().isEmpty()) {
            throw new IllegalArgumentException("jobIdHex must not be blank");
        }

        // 1. 从 dispatcher 拿到共享 MiniCluster 管理器
        SharedMiniClusterManager sharedMiniClusterManager = clientJobDispatcher.getSharedMiniClusterManager();
        if (sharedMiniClusterManager == null || !sharedMiniClusterManager.isStarted()) {
            throw new IllegalStateException("Shared MiniCluster is not started");
        }

        // 2. 获取共享 MiniCluster 实例
        MiniCluster miniCluster = sharedMiniClusterManager.getMiniCluster();

        // 3. 将 env 转成当前 job 的 StreamGraph
        StreamGraph streamGraph = env.getStreamGraph();

        // 4. 设置当前 job 名称
        streamGraph.setJobName(jobName);

        // 5. 显式指定 JobID，确保 NORMAL 模式固定 jobId 生效
        JobID jobId = JobID.fromHexString(jobIdHex);

        // 6. 再将 StreamGraph 转成 JobGraph
        JobGraph jobGraph = streamGraph.getJobGraph(
                Thread.currentThread().getContextClassLoader(),
                jobId
        );

        logger.info("Submitting job to shared MiniCluster, jobName={}, jobId={}", jobName, jobId);

        // 7. 提交到共享 MiniCluster
        JobSubmissionResult submissionResult = miniCluster.submitJob(jobGraph).get();


        logger.info("Job submitted to shared MiniCluster successfully, jobName={}, jobId={}",
                jobName, submissionResult.getJobID());

        // 8. 基于 MiniCluster 和 JobID 构造 JobClient
        //
        // 注意：
        // 这里使用 NOTHING，表示单个 job 结束时不关闭整个共享 MiniCluster。
        return new MiniClusterJobClient(
                submissionResult.getJobID(),
                miniCluster,
                Thread.currentThread().getContextClassLoader(),
                MiniClusterJobClient.JobFinalizationBehavior.NOTHING
        );
    }


//    private Map<String, Long> loadPersistedDataSourceScns(String clientId) throws Exception {
//        // 从 CDC_DATA_SOURCE_SCN 表中读取当前 client 的全部 DATA_SOURCE_ID scn
//        Map<String, Long> persisteScns = dataSourceScnRepository.loadNextScns(clientId);
//
//        // 防御性处理，避免 repository 返回 null
//        if (persisteScns == null) {
//            persisteScns = Collections.emptyMap();
//        }
//
//        // 记录启动时读取到的 offset 条数
//        logger.info("Loaded persisted dataSource scns, clientId={}, scnCount={}",
//                clientId, persisteScns.size());
//
//        return persisteScns;
//    }
//
//    private StartupOptions buildStartupOptions(BusinessDataSourceConfig businessDataSourceConfig) {
//
//        // 读取当前 dataSourceId 持久化的 scn
//        Long nextScn = this.persistsScns.get(businessDataSourceConfig.getDataSourceId());
//
//        // 如果没有持久化 offset，则兜底从最早开始消费
//        if (nextScn == null) {
//            logger.info("当前数据源 {} -> {}, 未找到被记录的SCN，因此从快照数据开始同步",
//                    businessDataSourceConfig.getDataSourceId(),
//                    businessDataSourceConfig.getDataSourceOrg());
//            return StartupOptions.initial(); // 首次快照
//        }
//
//        // 使用持久化的 scn 作为启动
//        logger.info("当前数据源 {} -> {}, 找到了被记录的SCN: {}",
//                businessDataSourceConfig.getDataSourceId(),
//                businessDataSourceConfig.getDataSourceOrg(),
//                nextScn);
//
//        return StartupOptions.specificOffset(String.valueOf(nextScn), 0);
//    }

    /**
     * 获取指定业务库对应的 JobClient
     */
    public JobClient getJobClient(String dataSourceId) {
        ClientJobRuntime runtime = runtimeByDataSourceId.get(dataSourceId);
        return runtime == null ? null : runtime.getJobClient();
    }

    /**
     * 获取指定业务库对应的 runtime
     *
     * 如果不存在则抛异常。
     */
    public ClientJobRuntime requireRuntime(String dataSourceId) {
        ClientJobRuntime runtime = runtimeByDataSourceId.get(dataSourceId);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found, dataSourceId=" + dataSourceId);
        }
        return runtime;
    }

    /**
     * 获取所有运行时上下文
     */
    public Collection<ClientJobRuntime> getAllRuntimes() {
        return runtimeByDataSourceId.values();
    }

    /**
     * 清理 OracleToKafka 持有的运行态资源。
     *
     * 当前方法职责：
     * 1. 逐个取消所有业务库对应的 Flink Job。
     * 2. 清理 runtime 中持有的运行态引用。
     * 3. 清空 runtimeByDataSourceId。
     *
     * 说明：
     * 1. 当前方法只清理 OracleToKafka 自己负责的 job 级资源。
     * 2. 不关闭 ClientProcessZkService。
     * 3. ClientProcessZkService 的生命周期由 ClientJobDispatcher 统一管理。
     */
    public void clearState() throws Exception {
        logger.info("Cleaning OracleToKafka state...");

        for (ClientJobRuntime runtime : runtimeByDataSourceId.values()) {
            String dataSourceId = runtime.getDataSourceId();

            // 2. 再取消当前业务库对应的 Flink Job
            try {
                if (runtime.getJobClient() != null) {
                    runtime.getJobClient().cancel();
                    runtime.setJobClient(null);
                }
            } catch (Exception e) {
                logger.warn("Failed to cancel job, dataSourceId={}", dataSourceId, e);
            }
        }

        // 3. 清空所有 runtime
        runtimeByDataSourceId.clear();

        logger.info("OracleToKafka state cleaned");
    }




//
//    // 为每个业务库 runtime 从 Kafka 最新消息中推断启动 SCN
//    private void loadStartupScnsFromKafka() {
//        // 遍历当前 client 下所有业务库 runtime
//        for (ClientJobRuntime runtime : runtimeByDataSourceId.values()) {
//
//            // 只有 snapshot 已完成的 NORMAL job 才允许从 Kafka 最新消息中推断启动 SCN
//            // 如果 snapshot 未完成，不能读 Kafka 中已有数据，否则半截快照会被误认为可恢复
//            if (runtime.getStartupMode() != JobStartupMode.NORMAL) {
//                // 清空启动 SCN
//                runtime.setKafkaStartupScn(null);
//
//                // 记录跳过原因
//                logger.info("Skip Kafka startup SCN loading because snapshot is not completed. dataSourceId={}, startupMode={}",
//                        runtime.getDataSourceId(),
//                        runtime.getStartupMode());
//
//                // 继续处理下一个业务库
//                continue;
//            }
//
//            // 收集当前业务库对应的所有 topic 名称
//            List<String> topics = runtime.getTopicInfos()
//                    .stream()
//                    .map(ClientTopicInfo::getTopic)
//                    .distinct()
//                    .collect(Collectors.toList());
//
//            // 从 Kafka 每个 topic 的最新消息中解析 SCN，并取最大值
//            OptionalLong maxScn =
//                    kafkaUtils.readMaxScnFromLatestMessages(topics, runtime.getDataSourceId());
//
//            // 如果 Kafka 中读到了可用 SCN，则保存到 runtime
//            if (maxScn.isPresent()) {
//                // 设置启动 SCN
//                runtime.setKafkaStartupScn(maxScn.getAsLong());
//
//                // 记录解析到的启动 SCN
//                logger.info("Startup SCN resolved from Kafka, dataSourceId={}, scn={}",
//                        runtime.getDataSourceId(),
//                        maxScn.getAsLong());
//
//                // 继续处理下一个业务库
//                continue;
//            }
//
//            // 如果 Kafka 中没有任何可用 SCN，则清空启动 SCN
//            runtime.setKafkaStartupScn(null);
//
//            // 记录回退 initial 的日志
//            logger.info("No startup SCN found from Kafka, fallback to initial startup, dataSourceId={}",
//                    runtime.getDataSourceId());
//        }
//    }

    // 根据当前业务库 runtime 构造 Oracle CDC 启动参数(主要是断点)
    private StartupOptions buildStartupOptions(ClientJobRuntime runtime) {
        // 获取当前业务库配置。
        BusinessDataSourceConfig businessDataSourceConfig = runtime.getBusinessDataSourceConfig();

        // SNAPSHOT 模式强制使用 initial。
        if (runtime.getStartupMode() == JobStartupMode.SNAPSHOT) {
            runtime.setKafkaStartupOffset(null);
            return StartupOptions.initial();
        }

        // NORMAL 模式从 Kafka 最新消息中解析 Oracle CDC 启动位点。
        Optional<OracleStartupOffset> kafkaStartupOffset = loadStartupOffsetFromKafka(runtime);

        // NORMAL 模式找不到启动位点时暂停启动。
        if (runtime.getStartupMode() == JobStartupMode.NORMAL && !kafkaStartupOffset.isPresent()) {
            while (true) {
                // 每 30 秒提示一次人工处理。
                logger.warn("NORMAL 模式下未从 Kafka 找到 Oracle 启动位点，程序暂停启动并等待人工处理。dataSourceId={}, org={}",
                        businessDataSourceConfig.getDataSourceId(),
                        businessDataSourceConfig.getDataSourceOrg());

                try {
                    // 等待 30 秒后继续提示。
                    Thread.sleep(30_000L);
                } catch (InterruptedException e) {
                    // 恢复中断标记。
                    Thread.currentThread().interrupt();

                    // 中断时退出等待。
                    throw new RuntimeException("等待 Kafka Oracle 启动位点时线程被中断", e);
                }
            }
        }

        // 非 NORMAL 模式且没有启动位点时兜底 initial。
        if (!kafkaStartupOffset.isPresent()) {
            runtime.setKafkaStartupOffset(null);

            logger.info("No Kafka startup offset found, use initial startup. dataSourceId={}, org={}",
                    businessDataSourceConfig.getDataSourceId(),
                    businessDataSourceConfig.getDataSourceOrg());

            return StartupOptions.initial();
        }

        // 保存 Kafka 启动位点到 runtime，供 OracleSource builder 使用。
        OracleStartupOffset offset = kafkaStartupOffset.get();
        runtime.setKafkaStartupOffset(offset);

        // 记录启动位点。
        logger.info("NORMAL 模式下，从 Kafka 恢复 Oracle 启动位点。dataSourceId={}, org={}, scn={}, commitScn={}",
                businessDataSourceConfig.getDataSourceId(),
                businessDataSourceConfig.getDataSourceOrg(),
                offset.getScn(),
                offset.getCommitScn());

        // 这里只进入增量路径，真正的 scn/commit_scn 由 OracleSource.specificStartupOffset 传入。
        return StartupOptions.latest();
    }

    /**
     * 从当前业务库对应的 Kafka topic 最新消息中推断 Oracle CDC 启动位点。
     */
    private Optional<OracleStartupOffset> loadStartupOffsetFromKafka(ClientJobRuntime runtime) {
        // 只有 NORMAL 模式才读取 Kafka 启动位点。
        if (runtime.getStartupMode() != JobStartupMode.NORMAL) {
            logger.info("Skip Kafka startup offset loading because startupMode is not NORMAL. dataSourceId={}, startupMode={}",
                    runtime.getDataSourceId(),
                    runtime.getStartupMode());
            return Optional.empty();
        }

        // 收集当前业务库对应的所有 topic 名称。
        List<String> topics = runtime.getTopicInfos()
                .stream()
                .map(ClientTopicInfo::getTopic)
                .distinct()
                .collect(Collectors.toList());

        // 没有 topic 时无法从 Kafka 恢复位点。
        if (topics.isEmpty()) {
            logger.info("No topics found for startup offset loading. dataSourceId={}",
                    runtime.getDataSourceId());
            return Optional.empty();
        }

        // 从 Kafka 最新消息中读取最大 Oracle 启动位点。
        Optional<OracleStartupOffset> startupOffset =
                kafkaUtils.readStartupOffsetFromLatestMessages(topics, runtime.getDataSourceId());

        // 记录读取结果。
        if (startupOffset.isPresent()) {
            logger.info("Startup offset resolved from Kafka, dataSourceId={}, offset={}",
                    runtime.getDataSourceId(),
                    startupOffset.get());
        } else {
            logger.info("No startup offset found from Kafka. dataSourceId={}",
                    runtime.getDataSourceId());
        }

        // 返回启动位点。
        return startupOffset;
    }

    // 初始化当前业务库对应的 job 级 ZK 运行态。
    private void initJobLevelZkNodes(String dataSourceId, String dataSourceOrg, JobLaunchMode launchMode) throws Exception {
        // 打开当前业务库 job 级 ZK 服务。
        ClientJobZkService jobZkService = ClientJobZkService.open(clientInfo, dataSourceId, dataSourceOrg);

        try {
            // 按启动场景准备 job 运行态节点，首次启动清空 scn，失败重启保留 scn。
            jobZkService.prepareJobRuntimeState(launchMode);
        } finally {
            // 释放 job 级 ZK 服务引用。
            jobZkService.close();
        }
    }



    /**
     * 根据启动模式更新当前业务库 job 的运行状态。
     */
    private void updateCurrentJobRunningStatus(String dataSourceId, JobStartupMode startupMode) throws Exception {
        // 打开当前业务库 job 级 ZK 服务。
        ClientJobZkService jobZkService = ClientJobZkService.open(clientInfo, dataSourceId);

        try {
            // 将启动模式映射为 job 运行状态并写入 ZK。
            jobZkService.updateRunningStatus(startupMode);
        } finally {
            // 释放 job 级 ZK 服务引用。
            jobZkService.close();
        }
    }






    // 清理失败 job 的单次启动运行态，保留 dataSourceId 对应 runtime 的稳定配置。
    public void cleanupFailedJobRuntime(String dataSourceId, String failedJobId) {
        // 根据 dataSourceId 查找 runtime。
        ClientJobRuntime runtime = runtimeByDataSourceId.get(dataSourceId);

        // runtime 不存在时直接返回，避免失败回调影响其它 job。
        if (runtime == null) {
            logger.warn("Skip failed job runtime cleanup because runtime not found. dataSourceId={}, failedJobId={}",
                    dataSourceId, failedJobId);
            return;
        }

        // 清理 Kafka 启动位点，确保下次启动重新从 Kafka 最新消息推导断点。
        runtime.setKafkaStartupOffset(null);

        // 只在当前 runtime 仍指向失败 job 时清理 jobClient，避免旧失败回调误清新 job。
        if (isRuntimeJobClientMatched(runtime, failedJobId)) {
            // 清理旧失败 job 的 JobClient 引用。
            runtime.setJobClient(null);

            // 记录清理成功日志。
            logger.info("Failed job client reference cleared. dataSourceId={}, failedJobId={}",
                    dataSourceId, failedJobId);
        } else {
            // 当前 runtime 已经指向其它 job 时不清理 jobClient。
            logger.info("Skip jobClient cleanup because runtime already points to another job. dataSourceId={}, failedJobId={}",
                    dataSourceId, failedJobId);
        }
    }

    // 判断 runtime 当前持有的 JobClient 是否就是本次失败的 job。
    private boolean isRuntimeJobClientMatched(ClientJobRuntime runtime, String failedJobId) {
        // 没有 failedJobId 时不能安全匹配。
        if (failedJobId == null || failedJobId.trim().isEmpty()) {
            return false;
        }

        // runtime 没有 JobClient 时无需清理。
        if (runtime.getJobClient() == null) {
            return false;
        }

        // 读取当前 runtime 持有的 jobId。
        String currentJobId = runtime.getJobClient().getJobID().toString();

        // 只有 jobId 一致时才允许清理。
        return failedJobId.equals(currentJobId);
    }




    // 失败自动重启前刷新 runtime 的启动模式，以 CDC_DATA_SOURCE_RUN_STATE 作为权威来源。
    public JobStartupMode refreshRuntimeStartupMode(ClientJobRuntime runtime) throws Exception {
        // runtime 不能为空，否则无法定位需要刷新的业务库。
        if (runtime == null) {
            throw new IllegalArgumentException("runtime must not be null");
        }

        // 读取当前 runtime 对应的业务库 ID。
        String dataSourceId = runtime.getDataSourceId();

        // 根据 dataSourceId 从当前 OracleToKafka 持有的业务库配置注册表中查找配置。
        BusinessDataSourceConfig businessDataSourceConfig = requireBusinessDataSourceConfig(dataSourceId);

        // 复用现有逻辑，从 CDC_DATA_SOURCE_RUN_STATE 判断最新启动模式。
        JobStartupMode refreshedStartupMode = determineStartupMode(businessDataSourceConfig);

        // 把最新启动模式写回进程内 runtime，保证后续 startSingleDataSourceJob 使用新值。
        runtime.setStartupMode(refreshedStartupMode);

        // 记录刷新结果，便于排查 snapshot 完成后自动重启是否切到 NORMAL。
        logger.info("Runtime startupMode refreshed before restart, clientId={}, dataSourceId={}, startupMode={}",
                clientInfo.getClientId(), dataSourceId, refreshedStartupMode);

        // 返回最新启动模式，调用方如需记录或判断可直接使用。
        return refreshedStartupMode;
    }

    // 根据 dataSourceId 获取业务库配置，供启动模式刷新逻辑复用。
    private BusinessDataSourceConfig requireBusinessDataSourceConfig(String dataSourceId) {
        // dataSourceId 不能为空，否则无法查询业务库配置。
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("dataSourceId must not be blank");
        }

        // 业务库配置注册表必须已经初始化。
        if (businessDataSourceConfigRegistry == null) {
            throw new IllegalStateException("businessDataSourceConfigRegistry is not initialized");
        }

        // 从注册表中按 dataSourceId 查找业务库配置。
        BusinessDataSourceConfig businessDataSourceConfig =
                businessDataSourceConfigRegistry.findBusinessDataSourceConfigByDataSourceId(dataSourceId);

        // 查不到配置时直接失败，避免用旧 runtime 状态继续启动错误 job。
        if (businessDataSourceConfig == null) {
            throw new IllegalStateException("BusinessDataSourceConfig not found, dataSourceId=" + dataSourceId);
        }

        // 返回当前业务库配置。
        return businessDataSourceConfig;
    }










}
