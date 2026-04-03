package com.bsoft.pipeline.k2k;


import com.bsoft.common.bean.*;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.dbmeta.*;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TopicSourceKind;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import com.bsoft.pipeline.api.SyncClientHTTPClient;
import com.bsoft.pipeline.api.SyncServerHTTPClient;
import com.bsoft.pipeline.functions.InjectOffsetIntoMessageSchema;
import com.bsoft.pipeline.functions.OracleCdcRowMapper;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanExecutor;
import com.bsoft.pipeline.plan.PlanNodeFactory;
import com.bsoft.pipeline.plan.runtime.CdcPipelinePlanNodeFactoryBuilder;
import com.bsoft.pipeline.plan.sql.SqlScopeAliasRegistry;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;

// 注册为 Spring 管理的组件
@Component
@EnableScheduling
public class KafkaPipeline extends CDCJob {


    private static final Logger logger = LogManager.getLogger(KafkaPipeline.class);

    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;


    // kafka工具类
    @Autowired
    private KafkaUtils kafkaUtils;


    // 检查点相关
    @Autowired
    private StateBackendProperties stateBackendProperties;

    // 所有业务库的配置，一般有多个业务库
    private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;

    // 所有目标库的配置，目标库可能有多个
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 订阅关系注册器，订阅关系可能有多个
    private SubscriberMappingRegistry subscriberMappingRegistry;


    private SyncClientHTTPClient syncClientHTTPClient;
    private SyncServerHTTPClient syncServerHTTPClient;

    // 缓存 业务库的表字段
    // key:clientId + "_" + schemaName
    private final Map<String, OracleColumnsQueryResult> tableSchemaCache = new ConcurrentHashMap<>();


    // 只加载一次 CDC_PIPELINE 计划，后续复用
    private List<RulePlan> rulePlans = Collections.emptyList();

    // 缓存 SQL 节点返回的结果集字段结构，key: planId|nodeId
    private final Map<String, OracleColumnsQueryResult> sqlNodeSchemaCache = new ConcurrentHashMap<>();
    private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry = new SqlNodeSchemaRegistry();


    private static final String EXECUTION_MODE_RUNTIME_ARG = "pipeline.execution.mode";
    private static final String SNAPSHOT_ENABLED_RUNTIME_ARG = "pipeline.snapshot.enabled";



    private SqlScopeAliasRegistry aliasRegistry = new SqlScopeAliasRegistry();



    public void init() throws Exception {
        // 1. 初始化 SyncClientHTTPClient 实例,用于向探针端请求表结构信息
        this.syncClientHTTPClient = new SyncClientHTTPClient(this.pipeLineInfo);
        this.syncClientHTTPClient.init();

        // 2. 初始化 SyncServerHTTPClient 实例,用于向服务端发送建表请求
        this.syncServerHTTPClient = new SyncServerHTTPClient(this.pipeLineInfo);
        this.syncServerHTTPClient.init();
    }

    public void startJob() {
        try {

            // 1. 初始化zk节点,ip+status，并注册当前服务端ip+port
//            initServerZKNode();

            // 2. 通知初始化开始
//            notifyJobStatusChange(JobStatus.INITIALIZING);

            // 3.从配置库读取各种配置
            prepareForJobExecution();

            // 4.根据配置库的内容，初始化当前执行所需的环境
            initServerExecutionEnvironment();

            if (shouldRunSqlSnapshotPhase()) {
                executeSqlSnapshotPhaseIfNeeded();
            } else {
                logger.info("skip SQL snapshot phase");
            }

            executeSqlIncrementalPhase();

//            // 5.是从 kafka 读取数据，写入目标库，可同时写入多个目标库
//            processKafkaStreamAndRepublish();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    // 从配置库读取各种配置
    public void prepareForJobExecution() throws Exception {

        // 1.从配置库中加载当前中心端的配置
        configFetcher.loadServerInfoConfig(this.serverInfo);

        // 2.从表中读取执行计划，每个RulePlan持有多个可执行PlanNode
        this.rulePlans = configFetcher.loadRulePlans();

        // 3.读取 CDC_CLIENT表数据，缓存
        configFetcher.refreshActiveClientCache();

        // 2.读取目标库的相关连接信息, 设置到serverInfo，注意，现在不能创建datasource，等算子分配到集群后，才能创建serverInfo
        // 一同处理DATA_TO_SOURCE_ID中逗号分隔 DATA_TO_SOURCE_ID，并去重，保证每个目标库只有一个 TargetDataSourceConfig
        // 只读取 有效订阅关系中 的目标数据库，
        targetDataSourceConfigRegistry = configFetcher.generateTargetDataSourceConfigRegistry();


        // 3.读取所有订阅关系,最终都解析成 一对一 的关系
        subscriberMappingRegistry = configFetcher.generateSubscriberMappingRegistry();

        // 4.读取所有 有效的业务库 配置
        businessDataSourceConfigRegistry = configFetcher.generateBusinessDataSourceConfigRegistry();

    }

    // 根据配置库的内容，初始化当前执行所需的环境
    public void initServerExecutionEnvironment() throws Exception {

        // 1.通过 订阅关系，把所有对象关联起来，并进行必要的初始化
        postProcessSubscriberMappingRegistry();

        // 2.生成当前 服务端 的执行计划，
        executionPlan = configFetcher.batchGenerateTableExecutionPlan(subscriberMappingRegistry);


        // 3. 生成需要读取主题信息，包括每个主题的数据最终写入哪个目标库的哪个表
        topicInfoRegistry.registerServerTopicInfo(executionPlan.generateServerTopicInfo(businessDataSourceConfigRegistry,
                targetDataSourceConfigRegistry,
                serverInfo,
                configFetcher));




        // 4. 检查需要监听的主题是否已经创建，还没创建则等待创建
        List<String> topics = topicInfoRegistry.getServerTopicInfoList().stream()
                .map(ClientTopicInfo::getTopic)
                .collect(Collectors.toList());
        kafkaUtils.waitUntilTopicExists(topics);


        // 5.从探针端获取每个表的表结构，并缓存
        requestTableSchemaFromClient();



        // 6.自定义SQL节点拉，取“SQL结果字段元数据”
        requestSqlNodeSchemaFromClient();



        // 6. 设置一个启动时间
        serverInfo.notifyClientStart();

        logger.info("当前 job 启动,启动时间: {}", serverInfo.printStartTime());
    }

    // 通过 订阅关系，把所有对象关联起来，并进行必要的初始化
    private void postProcessSubscriberMappingRegistry()   {
        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);


        // 为每个 业务库，配置对应的订阅关系，一个业务库，可能有多个 一对一 的订阅关系
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {
            // 每个业务源，可能有多个 一对一 的订阅关系
            businessDataSourceConfig.setSubscriberMappingRegistry(subscriberMappingRegistry);
            // 每个业务源，可能有多个 目标库
            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 读取业务库中每个需要同步的表的元数据，创建对应的 TableMappingConfig
            businessDataSourceConfig.init();
        }

    }






    private void executeSqlIncrementalPhase() throws Exception {
        processKafkaStreamAndRepublish();
    }

    private void executeSqlSnapshotPhaseIfNeeded() throws Exception {
        if (!hasSqlPlans()) {
            logger.info("no SQL plans found, skip snapshot phase");
            return;
        }

        logger.info("start SQL snapshot phase");

        Configuration configuration = new Configuration();
        configuration.setString("parallelism.default", "1");
        configuration.setInteger(RestOptions.PORT, serverInfo.getFlinkUiPort() + 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        PipelineInitContext initContext = PipelineInitContext.builder()
                .kafkaBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                .build();

        PlanContext context = new PlanContext(env, tableEnv);
        context.putRuntimeArg(EXECUTION_MODE_RUNTIME_ARG, PipelineExecutionMode.SNAPSHOT.name());
        context.putRuntimeArg(SNAPSHOT_ENABLED_RUNTIME_ARG, "true");

        context.registerService(
                BusinessDataSourceConfigRegistry.class.getName(),
                businessDataSourceConfigRegistry
        );
        context.registerService(
                SqlNodeSchemaRegistry.class.getName(),
                sqlNodeSchemaRegistry
        );

        // 创建PlanNodeFactory，注册各种 PlanNode 的 PlanNodeCreator
        PlanNodeFactory factory = CdcPipelinePlanNodeFactoryBuilder.buildSnapshotFactory(initContext);
        PlanExecutor executor = new PlanExecutor(factory);

        for (RulePlan plan : this.rulePlans) {
            executor.execute(plan, context);
        }

        env.execute("sync-pipeline-snapshot");
        logger.info("finish SQL snapshot phase");
    }


    public void processKafkaStreamAndRepublish() throws Exception {
        // 获取客户端id，为了生成固定的job id
        String serverId = pipeLineInfo.getPipelineId();
        // 生成固定的 job id
        String jobIdHex = JobIdUtil.generateFixedJobId(serverId);

        // 创建 执行环境 的 Configuration
        Configuration configuration = new Configuration();
        configuration.setString("$internal.pipeline.job-id", jobIdHex);
        configuration.setString("parallelism.default", "1");

        configureNetworkBufferPool(configuration);


        // 检查点设置
        String latestCheckpointPathWithMetadata = findLatestCheckpointPathWithMetadata(
                stateBackendProperties.getCheckpointPath(), jobIdHex);
        if (latestCheckpointPathWithMetadata != null) {
            configuration.setString("execution.savepoint.path", latestCheckpointPathWithMetadata);
            logger.info("从检查点恢复: {}", latestCheckpointPathWithMetadata);
        } else {
            logger.info(" 没有找到检查点，将从头开始");
        }


        // 为避免默认 8081 被占用，使用配置文件中指定的端口
        configuration.setInteger(RestOptions.PORT, serverInfo.getFlinkUiPort());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 应用检查点
        configureCheckpointing(env);




        // 读取每个主题的数据
        for (ClientTopicInfo topicInfo : topicInfoRegistry.getServerTopicInfoList()) {

            ServerTopicInfo serverTopicInfo = (ServerTopicInfo)topicInfo;
            // 主题名
            String kafkaTopic = serverTopicInfo.getTopic();


            // 稳定 groupId：serverId + topic，便于断点续读
            String groupId = "flink-k2db-" + serverInfo.getServerId() + "-" + kafkaTopic;

            // TODO 从Kafka读(消费者角色)： 新Source架构
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())   // 指定kafka节点的地址和端口
                    .setGroupId(groupId)  // 指定消费者组的id
                    .setTopics(serverTopicInfo.getTopic())                        // 指定消费的 Topic
                    .setDeserializer(new InjectOffsetIntoMessageSchema(serverTopicInfo.getTopic()))    // 指定 反序列化器，这个是反序列化value
                    .setStartingOffsets(OffsetsInitializer.earliest())                                 // flink消费kafka的策略
                    .build();


            // 创建数据流
            DataStreamSource<String> kafkaDataStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "kafka-source-" + kafkaTopic);

            // 动态表结构（来自 tableSchemaCache）
            RuntimeTableSchema rt = buildRuntimeTableSchema(serverTopicInfo);

            final List<OracleColumnMetadata> mapperColumns = new ArrayList<>(rt.getColumns());
            final Schema tableSchema = rt.getSchema();


            // 解析Kafka消息并处理数据库写入，获得操作结果
            SingleOutputStreamOperator<Row> rowStream = kafkaDataStream
                    .map(msg -> OracleCdcRowMapper.parseKafkaMessage(msg, mapperColumns))
                    .returns(rt.getRowTypeInfo());   // ← 关键：声明字段名

            kafkaDataStream.print("RAW-KAFKA");

            // TODO 3. 构建 Schema（全部字段使用 STRING 类型）
            // Oracle DATE 字段在 Kafka 消息中是毫秒时间戳字符串，统一用 STRING 接收，
            // 如需转换为时间类型，在 SQL 中使用：
            // TO_TIMESTAMP(FROM_UNIXTIME(CAST(clinic_date AS BIGINT) / 1000))
            // TODO 4. 将 DataStream<Row> 注册为临时视图
            // fromChangelogStream 会识别 Row 中的 RowKind（INSERT/UPDATE_AFTER/DELETE）
            String viewName = buildUniqueViewName(serverTopicInfo);
            tableEnv.createTemporaryView(
                    viewName,
                    tableEnv.fromChangelogStream(rowStream, tableSchema, ChangelogMode.all())
            );

            aliasRegistry.register(
                    serverTopicInfo.getBusinessDataSourceId(),
                    serverTopicInfo.getSchemaName(),
                    serverTopicInfo.getTableName(), // 短名，如 opt_fee
                    viewName                        // 唯一名，如 ds1_his_opt_fee
            );


//            // TODO 5. 使用 SQL 查询，验证数据
//


        }

        // node的初始化环境
        PipelineInitContext initContext = PipelineInitContext.builder()
                .kafkaBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                .build();

        // node执行的上下文环境
        PlanContext context = new PlanContext(tableEnv);
        context.registerService(SqlScopeAliasRegistry.class.getName(), aliasRegistry);
        context.putRuntimeArg(EXECUTION_MODE_RUNTIME_ARG, PipelineExecutionMode.INCREMENTAL.name());

        // PlanNode 创建工厂，其持有可用的PlanNodeType
        PlanNodeFactory factory = CdcPipelinePlanNodeFactoryBuilder.build(initContext);
        // 执行器
        PlanExecutor executor = new PlanExecutor(factory);

        // 从表中读取执行计划，每个RulePlan持有多个可执行PlanNode
        for (RulePlan plan : this.rulePlans) {
            executor.execute(plan, context);
        }

//        tableEnv.createTemporaryTable(
//                "print_sink",
//                org.apache.flink.table.api.TableDescriptor.forConnector("print")
//                        .schema(schema)
//                        .build()
//        );
//        tableEnv.executeSql("INSERT INTO print_sink SELECT * FROM opt_record");

        // 关键：真正提交并启动 Flink 作业
        env.execute("sync-pipeline-");

    }

    // 网络缓冲区配置
    public void configureNetworkBufferPool(Configuration configuration) // 窗口长度，步长默认为5s
            throws IOException {

        // 总进程内存给宽一点
        configuration.setString("taskmanager.memory.process.size", "2048m");

        // ✅ 固定网络内存为 256MB（min==max）
        configuration.setString("taskmanager.memory.network.min", "256mb");
        configuration.setString("taskmanager.memory.network.max", "256mb");
    }

//    public void configureStatisticsWindows(SingleOutputStreamOperator<OperationResult> operationResults ,
//                                           WindowTimeUnit windowTimeUnit, // 窗口长度
//                                           Time time)                     // 窗口步长
//            throws IOException {
//
//        // 基于操作结果进行窗口统计
//        SingleOutputStreamOperator<TableOperationResultStats> statistics = operationResults
//                .keyBy(result -> result.getTableName()) // 按表名分组
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowTimeUnit.getSecondsValue()), time))  //滑动窗口，长度,步长均由参数指定
//                .aggregate(
//                        new OperationResultStatsAggregator(windowTimeUnit.getName()),  // 聚合窗口函数
//                        new OperationResultWindowFunction()    // 全局窗口函数
//                );
//
//
//        //  输出统计结果到控制台（保留原有功能）;
//        statistics.addSink(new OperationResultStatisticsSink());
//
//        // 输出统计数据到zk，有问题，暂时先不输出
////        statistics.addSink(new ZooKeeperStatisticsSink(serverInfo,windowTimeUnit));
//    }

    /**
     * 配置检查点
     * @param env Flink流执行环境
     * @throws IOException
     */
    public void configureCheckpointing(StreamExecutionEnvironment env) throws IOException {
        logger.info("⚙️ 配置检查点...");

        // TODO 检查点常用配置
        // 1、启用检查点: 默认是barrier对齐的，周期为10分钟, 精准一次
        env.enableCheckpointing( 3 * 60 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);

        // 检查点的配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 2、指定检查点的存储位置
        // 2、指定检查点的存储位置
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(stateBackendProperties.getCheckpointPath());

        // 3、checkpoint的超时时间: 默认30分钟，超过这个时间，认为当前checkpoint保存失败
        checkpointConfig.setCheckpointTimeout(1 * 60 * 60 * 1000L);

        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔，如果设置了 > 0，那么并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 60 * 1000L);

        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(100);


        // TODO 开启 非对齐检查点（barrier非对齐）
        // 开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
        checkpointConfig.enableUnalignedCheckpoints();
//        // 开启非对齐检查点才生效： 默认0，表示一开始就直接用 非对齐的检查点
//        // 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数，自动切换成 非对齐检查点（barrier非对齐）
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));
    }


    /**
     * 获取某 JobID 下最新存在 _metadata 文件的 checkpoint 路径（chk-xxx），用于作为 savepoint 恢复。
     * @param checkpointBasePath 例如 file:///F:/flink-checkpoints/demo
     * @param jobIdHex 例如 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
     * @return _metadata 所在的 chk-x 目录的 URI 字符串，如果没有则返回 null
     * @throws Exception
     */
    public static String findLatestCheckpointPathWithMetadata(String checkpointBasePath, String jobIdHex) throws Exception {
        // 处理 file:/// 前缀，转成本地路径
        String localBasePath = new Path(checkpointBasePath).toUri().getPath();

        File jobDir = new File(localBasePath, jobIdHex);
        if (!jobDir.exists() || !jobDir.isDirectory()) {
            return null;
        }

        File[] chkDirs = jobDir.listFiles((dir, name) -> name.startsWith("chk-"));
        if (chkDirs == null || chkDirs.length == 0) {
            return null;
        }

        // 按 chk-数字 降序排序
        Arrays.sort(chkDirs, Comparator.comparing(File::getName).reversed());

        for (File chkDir : chkDirs) {
            File metadata = new File(chkDir, "_metadata");
            if (metadata.exists() && metadata.isFile()) {
                return metadata.getParentFile().toURI().toString(); // chk-x 的路径
            }
        }

        return null;
    }

//    @Scheduled(fixedRate = 60000) // 每5秒执行一次
//    public void monitorKafkaTopics() throws Exception {
//        GlobalZooKeeperManager offsetManager = GlobalZooKeeperManager.getInstance(serverInfo);
//
//        try {
//            for (ServerTopicInfo serverTopicInfo : topicInfoRegistry.getServerTopicInfoList()) {
//
//                Map<String,String> metric = new HashMap<>();
//                metric.put("topic",serverTopicInfo.getTopic());
//                metric.put("targetDataSourceId",serverTopicInfo.getTargetDataSourceId());
//
//                metric.put("targetTableName",serverTopicInfo.getTargetTableName());
//                metric.put("unconsumedMessageCount",kafkaUtils.getUnconsumedMessageCount(serverTopicInfo.getTopic()).toString());
//
//                offsetManager.updateUnconsumedMessageCount(metric);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//    }
//
//    public void sinkSyncLogsToKafka(DataStream<String> syncLogEntryDataStream) {
//        // 为每个侧输出流创建 Kafka Sink
//        syncLogEntryDataStream
//                .sinkTo(KafkaSink.<String>builder()
//                        .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
//                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
//                                .setTopic(this.serverLogTopic)
//                                .setValueSerializationSchema(new SimpleStringSchema())
//                                .build())
//                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
//                        //  连接管理配置(防止频繁创建/销毁连接)
//                        .setProperty("connections.max.idle.ms", "540000") // 9分钟空闲超时
//                        .setProperty("max.block.ms", "60000")
//                        .setProperty("metadata.max.age.ms", "300000") // 5分钟刷新元数据
//                        // 性能优化配置
//                        .setProperty("batch.size", "262144") // 增加批大小到64KB
//                        .setProperty("linger.ms", "100") // 增加等待时间
//                        .setProperty("buffer.memory", "67108864") // 64MB缓冲区内存
//                        .setProperty("compression.type", "lz4") // 启用LZ4压缩
//                        // 无限重试配置
//                        .setProperty("retries", String.valueOf(Integer.MAX_VALUE))
//                        .setProperty("retry.backoff.ms", "1000")
//                        .setProperty("reconnect.backoff.ms", "1000")
//                        .setProperty("reconnect.backoff.max.ms", "10000")
//                        .setProperty("request.timeout.ms", "30000")
//                        .setProperty("delivery.timeout.ms", String.valueOf(Integer.MAX_VALUE))
//                        .setProperty("acks", "1")
//                        .build())
////                    .uid(clientId + "-sink-" + topic)
//                .name("KafkaSink-" + this.serverLogTopic);
//
//    }


    // todo 1：从 CDC_DATA_SUBSCRIBE 中拿到需要读取的topic

    // todo 2：这些流转成表

    // todo 3:


    /**
     * 按 clientId -> schemaName 分组构造请求，分多次调用 clientHTTPClient#getTableSchema，
     * 并缓存所有 OracleColumnsQueryResult。
     */
    private void requestTableSchemaFromClient() throws Exception {
        List<ServerTopicInfo> topicInfos = topicInfoRegistry.getServerTopicInfoList();

        // key: normalized clientId + normalized schemaName
        Map<String, OracleColumnsQueryRequest> requestMap = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> groupedTableNames = new LinkedHashMap<>();

        for (ServerTopicInfo t : topicInfos) {
            if (t == null) {
                continue;
            }
            if (t.getClientId() == null || t.getClientId().trim().isEmpty()) {
                continue;
            }
            if (t.getSchemaName() == null || t.getSchemaName().trim().isEmpty()) {
                continue;
            }
            if (t.getTableName() == null || t.getTableName().trim().isEmpty()) {
                continue;
            }

            String clientId = t.getClientId().trim();
            String schemaName = t.getSchemaName().trim().toUpperCase(Locale.ROOT);
            String tableName = t.getTableName().trim().toUpperCase(Locale.ROOT);

            String cacheKey = buildTableSchemaCacheKey(clientId, schemaName);

            requestMap.computeIfAbsent(cacheKey, k -> {
                OracleColumnsQueryRequest req = new OracleColumnsQueryRequest();
                req.setClientId(clientId);
                req.setOwner(schemaName);
                req.setTableNames(new ArrayList<>());
                return req;
            });

            groupedTableNames.computeIfAbsent(cacheKey, k -> new LinkedHashSet<>()).add(tableName);
        }

        // 每个 (clientId, schemaName) 只发一次请求
        for (Map.Entry<String, OracleColumnsQueryRequest> entry : requestMap.entrySet()) {
            String cacheKey = entry.getKey();
            OracleColumnsQueryRequest request = entry.getValue();

            List<String> tableNames = new ArrayList<>(
                    groupedTableNames.getOrDefault(cacheKey, new LinkedHashSet<>())
            );
            if (tableNames.isEmpty()) {
                continue;
            }

            request.setTableNames(tableNames);

            OracleColumnsQueryResult result = syncClientHTTPClient.getTableSchema(request.getClientId(), request);
            this.tableSchemaCache.put(cacheKey, result);

            logger.info("表结构拉取完成 clientId={}, schemaName={}, tableCount={}, columnCount={}",
                    request.getClientId(),
                    request.getOwner(),
                    tableNames.size(),
                    (result == null || result.getColumns() == null) ? 0 : result.getColumns().size());
        }

    }


    private RuntimeTableSchema buildRuntimeTableSchema(ServerTopicInfo topicInfo) {
        List<OracleColumnMetadata> columns = resolveTableColumns(topicInfo);

        String[] fieldNames = new String[columns.size()];
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[columns.size()];
        Schema.Builder schemaBuilder = Schema.newBuilder();

        for (int i = 0; i < columns.size(); i++) {
            OracleColumnMetadata c = columns.get(i);

            // CDC 消息里字段名是小写，统一转小写做映射
            String fieldName = c.getColumnName().toLowerCase(Locale.ROOT);

            fieldNames[i] = fieldName;
            fieldTypes[i] = oracleToTypeInfo(c);
            schemaBuilder.column(fieldName, oracleToDataType(c));
        }

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        Schema schema = schemaBuilder.build();

        return new RuntimeTableSchema(columns, fieldNames, fieldTypes, rowTypeInfo, schema);
    }

    private List<OracleColumnMetadata> resolveTableColumns(ServerTopicInfo topicInfo) {
        String cacheKey = buildTableSchemaCacheKey(topicInfo.getClientId(), topicInfo.getSchemaName());
        OracleColumnsQueryResult result = tableSchemaCache.get(cacheKey);
        if (result == null) {
            throw new IllegalStateException("tableSchemaCache miss, key=" + cacheKey);
        }

        String tableUpper = topicInfo.getTableName().trim().toUpperCase(Locale.ROOT);
        List<OracleColumnMetadata> columns = result.getTableColumns().get(tableUpper);

        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("No columns found in cache for key=" + cacheKey + ", table=" + tableUpper);
        }
        return columns;
    }

    private TypeInformation<?> oracleToTypeInfo(OracleColumnMetadata c) {
        String t = normalizeType(c.getDataType());

        switch (t) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                Integer p = c.getDataPrecision();
                Integer s = c.getDataScale();
                if (s != null && s > 0) return Types.BIG_DEC;
                if (p == null) return Types.BIG_DEC;
                if (p <= 9) return Types.INT;
                if (p <= 18) return Types.LONG;
                return Types.BIG_DEC;

            case "FLOAT":
            case "BINARY_FLOAT":
                return Types.FLOAT;

            case "BINARY_DOUBLE":
                return Types.DOUBLE;

            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return Types.SQL_TIMESTAMP;

            case "CHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "CLOB":
            case "NCLOB":
            default:
                return Types.STRING;
        }
    }

    private DataType oracleToDataType(OracleColumnMetadata c) {
        String t = normalizeType(c.getDataType());

        switch (t) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                Integer p = c.getDataPrecision();
                Integer s = c.getDataScale();
                int precision = (p == null || p <= 0) ? 38 : Math.min(p, 38);
                int scale = (s == null || s < 0) ? 0 : Math.min(s, 18);
                return DataTypes.DECIMAL(precision, scale);

            case "FLOAT":
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();

            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();

            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                // 关键：外部 Row 字段类型与 mapper 返回类型保持一致（java.sql.Timestamp）
                return DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);

            case "CHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "CLOB":
            case "NCLOB":
            default:
                return DataTypes.STRING();
        }
    }

    private String normalizeType(String t) {
        return t == null ? "" : t.trim().toUpperCase(Locale.ROOT);
    }


    private String buildUniqueViewName(ServerTopicInfo t) {
        String raw = String.join("_",
                safe(t.getBusinessDataSourceId()),
                safe(t.getSchemaName()),
                safe(t.getTableName())
        ).toLowerCase(Locale.ROOT);

        // 非法字符替换
        String sanitized = raw.replaceAll("[^a-z0-9_]", "_");

        // 不能以数字开头
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "v_" + sanitized;
        }

        return sanitized;
    }

    private String safe(String s) {
        return s == null ? "null" : s.trim();
    }













    /**
     * 遍历已加载的 RulePlan，提取 SQL 节点的 sql，调用 client 接口获取字段元数据并缓存。
     */
    private void requestSqlNodeSchemaFromClient() throws Exception {
        if (rulePlans == null || rulePlans.isEmpty()) {
            return;
        }

        for (RulePlan plan : rulePlans) {
            if (plan.getNodes() == null || plan.getNodes().isEmpty()) {
                continue;
            }

            for (PlanNodeSpec node : plan.getNodes()) {
                if (node.getType() != PlanNodeType.SQL) {
                    continue;
                }

                String sql = extractRequiredStringParam(node, "sql");
                String schema = extractRequiredStringParam(node, "schema");
                String clientId = resolveClientIdFromNode(node);

                OracleSqlColumnsQueryRequest request = new OracleSqlColumnsQueryRequest();
                request.setClientId(clientId);
                request.setSql(sql);
                request.setSchema(schema);

                // 依赖 ClientHTTPClient 新增方法：
                // getSqlColumnsSchema(clientId, request) -> /sync-client/api/tables/sql-columns/export
                OracleColumnsQueryResult result = syncClientHTTPClient.getSqlColumnsSchema(clientId, request);

                String cacheKey = buildSqlNodeCacheKey(plan.getPlanId(), node.getId());
                sqlNodeSchemaCache.put(cacheKey, result);
                sqlNodeSchemaRegistry.register(plan.getPlanId(), node.getId(), result);

                logger.info("SQL节点字段元数据拉取完成, planId={}, nodeId={}, clientId={}, columnCount={}",
                        plan.getPlanId(),
                        node.getId(),
                        clientId,
                        (result == null || result.getColumns() == null) ? 0 : result.getColumns().size());
            }
        }
    }


    private String extractRequiredStringParam(PlanNodeSpec node, String paramKey) {
        if (node == null) {
            throw new IllegalArgumentException("node is null");
        }
        if (paramKey == null || paramKey.trim().isEmpty()) {
            throw new IllegalArgumentException("paramKey is blank, nodeId=" + node.getId());
        }

        Map<String, Object> params = node.getParams();
        if (params == null) {
            throw new IllegalArgumentException("node params is null, nodeId=" + node.getId());
        }

        Object value = params.get(paramKey);
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "node requires non-empty param `" + paramKey + "`, nodeId=" + node.getId()
            );
        }

        return String.valueOf(value).trim();
    }

    private String resolveClientIdFromNode(PlanNodeSpec node) {
        Map<String, Object> params = node.getParams();
        if (params == null) {
            throw new IllegalArgumentException("node params is null, nodeId=" + node.getId());
        }

        Object dsObj = params.get("data_source_id");
        String dataFromSourceId = dsObj == null ? null : String.valueOf(dsObj).trim();

        return configFetcher.resolveClientIdByDataSourceId(dataFromSourceId);
    }

    private String buildSqlNodeCacheKey(String planId, String nodeId) {
        return String.valueOf(planId).trim() + "|" + String.valueOf(nodeId).trim();
    }

    private String buildTableSchemaCacheKey(String clientId, String schemaName) {
        return String.valueOf(clientId).trim() + "_" + String.valueOf(schemaName).trim().toUpperCase(Locale.ROOT);
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

    private boolean hasSqlPlans() {
        if (this.rulePlans == null || this.rulePlans.isEmpty()) {
            return false;
        }

        for (RulePlan plan : this.rulePlans) {
            if (plan == null || plan.getNodes() == null) {
                continue;
            }

            for (PlanNodeSpec node : plan.getNodes()) {
                if (node == null || node.getType() == null) {
                    continue;
                }

                if (node.getType() == PlanNodeType.SQL || node.getType() == PlanNodeType.SINK_TABLE) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean shouldRunSqlSnapshotPhase() {

        return true;

//        if (!hasSqlPlans()) {
//            return false;
//        }
//
//        String fromProperty = System.getProperty("pipeline.sql.snapshot.enabled");
//        if (fromProperty != null && !fromProperty.trim().isEmpty()) {
//            return Boolean.parseBoolean(fromProperty.trim());
//        }
//
//        String fromEnv = System.getenv("PIPELINE_SQL_SNAPSHOT_ENABLED");
//        if (fromEnv != null && !fromEnv.trim().isEmpty()) {
//            return Boolean.parseBoolean(fromEnv.trim());
//        }
//
//        // 默认先不自动跑，避免每次重启重复全量
//        return false;
    }
}
