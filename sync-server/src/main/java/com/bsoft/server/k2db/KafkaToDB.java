package com.bsoft.server.k2db;


import com.bsoft.common.bean.*;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.WindowTimeUnit;


import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.common.service.TopicInfoService;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import com.bsoft.common.zk.service.ServerProcessZkService;
import com.bsoft.server.api.ServerController;
import com.bsoft.server.function.OperationResultStatsAggregator;
import com.bsoft.server.function.OperationResultWindowFunction;
import com.bsoft.server.function.TargetTableWriteProcessFunction;
import com.bsoft.server.offset.JdbcTopicOffsetRepository;
import com.bsoft.server.offset.TopicOffsetCommitEvent;
import com.bsoft.server.offset.TopicOffsetRepository;
import com.bsoft.server.sink.OperationResultStatisticsSink;
import com.bsoft.server.sink.TopicOffsetCommitJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

import java.util.*;
import java.util.stream.Collectors;
import com.bsoft.common.enums.TopicSourceKind;

// 注册为 Spring 管理的组件
@Component
public class KafkaToDB  extends CDCJob {


    private static final Logger logger = LogManager.getLogger(KafkaToDB.class);

    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;

//    @Autowired
//    private ConfigDataSourceConfig configDataSourceConfig;


    // kafka工具类
    @Autowired
    private KafkaUtils kafkaUtils;


    // 检查点相关
//    @Autowired
//    private StateBackendProperties stateBackendProperties;

    // 所有业务库的配置，一般有多个业务库
    private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;

    // 所有目标库的配置，目标库可能有多个
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 订阅关系注册器，订阅关系可能有多个
    private SubscriberMappingRegistry subscriberMappingRegistry;

    @Autowired
    private ServerController serverController;

//    @Autowired
//    private TableNameGenerator tableNameGenerator;

    /**
     * 稳定消费组前缀：flink-k2db-{serverId}-
     * 约定 serverId 在当前实例生命周期内不变。
     */
    private String groupIdPrefix;

    private PipelineSqlSinkTopicResolver pipelineSqlSinkTopicResolver;

    private List<RulePlan> rulePlans = Collections.emptyList();

    /**
     * server 进程级 ZK 服务。
     *
     * 负责：
     * 1. server 进程级节点初始化
     * 2. server 进程级状态更新
     */
    private ServerProcessZkService serverProcessZkService;

    @Autowired
    private TopicInfoService topicInfoService;

    // topic offset 仓储，负责配置库中的位点读写
    private TopicOffsetRepository topicOffsetRepository;


    /**
     * 单个 union 分组的大小。
     *
     * 背景：
     * 生产环境 topic 数量很多时，例如 4000 个 topic，会产生 4000 条日志侧输出流、
     * 4000 条 offset 提交侧输出流。如果按原来的方式逐个 union：
     *
     *     merged = merged.union(next)
     *
     * 会形成很深的左递归 StreamGraph，Flink 在启动时生成执行图可能触发 StackOverflowError。
     *
     * 这里按 64 条流一组做分层合并，降低 StreamGraph 递归深度。
     */
    private static final int STREAM_UNION_BATCH_SIZE = 64;
    public void init() throws Exception {


        // 初始化 server 进程级 zk 服务
        this.serverProcessZkService = ServerProcessZkService.open(this.serverInfo);

        // 初始化 topic offset 仓储，复用配置库 DataSource
        this.topicOffsetRepository = new JdbcTopicOffsetRepository(
                this.configFetcher.getConfigDataSource()

        );


    }

    public void startJob() {
        try {

            // 1. 初始化 server 进程级 ZK 节点并注册当前服务端状态。
            initServerProcessZkState();


            // 2.从配置库读取各种配置
            prepareForJobExecution();

//            // 3. 验证 topic offset 仓储是否可正常读写
//            verifyTopicOffsetRepository();

            // 4.根据配置库的内容，初始化当前执logger.info("1");行所需的环境
            initServerExecutionEnvironment();

            // 5.是从 kafka 读取数据，写入目标库，可同时写入多个目标库
            pullFromKafkaAndPublishToDB();


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * 初始化 server 进程级 ZK 状态。
     *
     * 职责：
     * 1. 初始化 server process 级节点结构
     * 2. 写入当前 server IP
     * 3. 写入当前进程状态为 INITIALIZING
     */
    private void initServerProcessZkState() throws Exception {
        serverProcessZkService.initServerProcessNodes();
        serverProcessZkService.updateIp();

        this.jobStatus = JobStatus.INITIALIZING;
        serverProcessZkService.updateStatus(this.jobStatus);
    }

    // 从配置库读取各种配置
    public void prepareForJobExecution() throws Exception {

        // 1.向配置库注册当前server
        configFetcher.registerServerIfAbsent(this.serverInfo);
        // 2.从配置库中加载当前中心端的配置
        configFetcher.loadServerInfoConfig(this.serverInfo);

        // 3.从配置库读取各个探针的 配置，主要是表的命名方式
        configFetcher.refreshActiveClientCache();

        // 3.读取目标库的相关连接信息, 设置到serverInfo，注意，现在不能创建datasource，等算子分配到集群后，才能创建serverInfo
        // 一同处理DATA_TO_SOURCE_ID中逗号分隔 DATA_TO_SOURCE_ID，并去重，保证每个目标库只有一个 TargetDataSourceConfig
        // 只读取 有效订阅关系中 的目标数据库，
        targetDataSourceConfigRegistry = configFetcher.generateTargetDataSourceConfigRegistry();


        // 4.读取所有订阅关系,最终都解析成 一对一 的关系
        subscriberMappingRegistry = configFetcher.generateSubscriberMappingRegistry();

        // 5.读取所有 有效的业务库 配置
        businessDataSourceConfigRegistry = configFetcher.generateBusinessDataSourceConfigRegistry();

        // 6.读取所有bbx流程
        this.rulePlans = configFetcher.loadRulePlans();




    }

    // 根据配置库的内容，初始化当前执行所需的环境
    public void initServerExecutionEnvironment() throws Exception {

        // 1.通过 订阅关系，把所有对象关联起来，并进行必要的初始化
        postProcessSubscriberMappingRegistry();

        // 2.生成当前 服务端 的执行计划，
        executionPlan = configFetcher.batchGenerateTableExecutionPlan(subscriberMappingRegistry);

        // 3. 初始化serverController，该类用于外部通信，比如建表
        serverController.init(businessDataSourceConfigRegistry,targetDataSourceConfigRegistry);
        serverController.startWorkers();
        serverController.buildDorisBizAttrBySqlNodeId(rulePlans);


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


        // 6. 按需创建 日志，监控 主题
        kafkaUtils.createKafkaTopicsIfNotExists(Arrays.asList(this.serverInfo.getServerLogTopic(), this.serverInfo.getMonitorMetricTopic()));

        // 7. 初始化 groupId 前缀
        initGroupIdPrefix();

        // 8. 注册flink sql对应的主题
//        this.pipelineSqlSinkTopicResolver = new PipelineSqlSinkTopicResolver(configFetcher,targetDataSourceConfigRegistry,tableNameGenerator);
//        List<ServerTopicInfo> sqlSinkTopics = pipelineSqlSinkTopicResolver.resolve(this.rulePlans);
//        topicInfoRegistry.registerServerTopicInfo(sqlSinkTopics);

        // final. 设置一个启动时间
        serverInfo.notifyClientStart();


        logger.info("当前 job 启动,启动时间: {}", serverInfo.printStartTime());
    }

    // 通过 订阅关系，把所有对象关联起来，并进行必要的初始化
    private void postProcessSubscriberMappingRegistry()   {
        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);


        // 为每个 业务库，配置对应的订阅关系，一个业务库，可能有多个 一对一 的订阅关系
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {
            // 每个业务源，可能有多个 一对一 的订阅关系
            businessDataSourceConfig.setSubscriberMappingList(this.subscriberMappingRegistry.findSubscriberMappingByDataFromSourceId(businessDataSourceConfig));

            // 每个业务源，可能有多个 目标库
//            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 读取业务库中每个需要同步的表的元数据，创建对应的 TableMappingConfig
//            businessDataSourceConfig.init();
        }

    }
    public void pullFromKafkaAndPublishToDB() throws Exception {
        // 获取客户端id，为了生成固定的job id
        String serverId = serverInfo.getServerId();
        // 生成固定的 job id
        String jobIdHex = JobIdUtil.generateFixedJobId(serverId);

        // 创建 执行环境 的 Configuration
        Configuration configuration = new Configuration();
        configuration.setString("$internal.pipeline.job-id", jobIdHex);
        configuration.setString("parallelism.default", "1");

        // 配置网络和进程内存
        configureNetworkBufferPool(configuration);


//        // 检查点设置
//        String latestCheckpointPathWithMetadata = findLatestCheckpointPathWithMetadata(
//                stateBackendProperties.getCheckpointPath(), jobIdHex);
//        if (latestCheckpointPathWithMetadata != null) {
//            configuration.setString("execution.savepoint.path", latestCheckpointPathWithMetadata);
//            logger.info("从检查点恢复: {}", latestCheckpointPathWithMetadata);
//        } else {
//            logger.info(" 没有找到检查点，将从头开始");
//        }


        // 为避免默认 8081 被占用，使用配置文件中指定的端口
        configuration.setInteger(RestOptions.PORT, serverInfo.getFlinkUiPort());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

//        // 应用检查点
//        configureCheckpointing(env);

        // 启动时一次性读取当前 server 已持久化的 topic -> nextOffset
        Map<String, Long> persistedOffsets = loadPersistedTopicOffsets(serverId);

        // 定义侧输出标签，用于输入日志
        OutputTag<String> syncLogEntryOutputTag = new OutputTag<String>("sync-server-log-side-output") {};
        // 收集所有 日志 侧输出流
        List<DataStream<String>> allLogStreams = new ArrayList<>();

        // 定义侧输出标签，用于输入监控数据
        OutputTag<String> monitorMetricOutputTag = new OutputTag<String>("sync-server-metric-side-output") {};
        // 收集所有 监控 侧输出流
        List<DataStream<String>> allMonitorMetricOutputTagStreams = new ArrayList<>();


        // 定义 每个topic offset 提交事件侧输出标签
        OutputTag<TopicOffsetCommitEvent> topicOffsetCommitOutputTag = new OutputTag<TopicOffsetCommitEvent>("sync-server-topic-offset-side-output") {};
        // 收集所有 topic offset 提交事件侧输出流
        List<DataStream<TopicOffsetCommitEvent>> allTopicOffsetCommitStreams = new ArrayList<>();


        // 按目标表对 topic 分组，一个目标表对应一个 KafkaSource 和一个 writer
        Map<TargetTableKey, List<ServerTopicInfo>> topicGroups = groupTopicInfosByTargetTable();

        // 打印目标表分组摘要
        logTargetTableTopicGroups(topicGroups);

        // 遍历每一个目标表分组
        for (Map.Entry<TargetTableKey, List<ServerTopicInfo>> entry : topicGroups.entrySet()) {
            // 获取目标表分组键
            TargetTableKey targetTableKey = entry.getKey();

            // 获取当前目标表下的所有 topic
            List<ServerTopicInfo> groupedTopicInfos = entry.getValue();

            // 提取 Kafka topic 名称列表
            List<String> kafkaTopics = extractKafkaTopics(groupedTopicInfos);

            // 空 topic 分组直接跳过
            if (kafkaTopics.isEmpty()) {
                logger.warn("skip empty topic group, targetTableKey={}", targetTableKey);
                continue;
            }

            // 构造 operator 名称片段
            String operatorNamePart = buildOperatorNamePart(targetTableKey);

            // 构造目标表写入上下文
            TargetTableWriteContext writeContext = buildTargetTableWriteContext(targetTableKey, groupedTopicInfos);

            // 按目标表生成 Kafka groupId
            String groupId = buildGroupId(targetTableKey);

            // 为当前目标表分组内每个 topic 构造启动 offset
            OffsetsInitializer startingOffsetsInitializer = buildStartingOffsetsInitializerForTopics(kafkaTopics, persistedOffsets);

            // 当前目标表 writer 使用的固定 key。
            String targetTableRuntimeKey = targetTableKey.getTargetDataSourceId() + "|" + targetTableKey.getTargetTableName();
            // 创建多 topic KafkaSource
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                    .setGroupId(groupId)
                    .setTopics(kafkaTopics)
                    .setDeserializer(new InjectOffsetIntoMessageSchema())
                    .setStartingOffsets(startingOffsetsInitializer)
                    .build();

            // 创建当前目标表分组的数据流
            DataStreamSource<String> kafkaDataStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "kafka-source-" + operatorNamePart
            );

            // 解析 Kafka 消息并写入同一张目标表
            SingleOutputStreamOperator<OperationResult> operationResults = kafkaDataStream
                    .map(OracleCdcJsonParser::parseKafkaMessage)
                    .name("parse-kafka-message-" + operatorNamePart)
                    .filter(kafkaPayload -> {
                        // 判断消息是否有效
                        boolean isValid = kafkaPayload != null;

                        // 无效消息记录调试日志
                        if (!isValid) {
                            logger.debug("filtered invalid Kafka message");
                        }

                        // 返回过滤结果
                        return isValid;
                    })
                    .name("filter-" + operatorNamePart)
                    .uid("filter-" + operatorNamePart)
                    .keyBy(kafkaPayload -> targetTableRuntimeKey)
                    .process(new TargetTableWriteProcessFunction(serverInfo, writeContext,
                                                                 syncLogEntryOutputTag,
                                                                 monitorMetricOutputTag,
                                                                 topicOffsetCommitOutputTag
                    ))
                    .name("target-table-writer-" + operatorNamePart)
                    .uid("target-table-writer-" + operatorNamePart);

            // 收集当前目标表 writer 的日志侧输出流
            DataStream<String> syncLogEntryDataStream = operationResults.getSideOutput(syncLogEntryOutputTag);
            // 加入全局日志流列表
            allLogStreams.add(syncLogEntryDataStream);


            // 收集当前目标表 writer 的 offset commit 侧输出流
            DataStream<TopicOffsetCommitEvent> topicOffsetCommitDataStream = operationResults.getSideOutput(topicOffsetCommitOutputTag);
            // 加入全局 offset commit 流列表
            allTopicOffsetCommitStreams.add(topicOffsetCommitDataStream);
        }


        //  合并所有日志流后统一 sink(只创建 1 个 KafkaSink)
        DataStream<String> mergedLogStream = mergeSyncLogStreams(allLogStreams);
        if (mergedLogStream != null) {
            sinkStringStreamToKafka(mergedLogStream, this.serverInfo.getServerLogTopic(), "KafkaSink-sync-log");
        }

//        DataStream<String> mergedMetricStream = mergeStreams(allMonitorMetricOutputTagStreams);
//        if (mergedMetricStream != null) {
//            sinkStringStreamToKafka(mergedMetricStream, this.serverInfo.getMonitorMetricTopic(), "KafkaSink-monitor-metric");
//        }

        // 合并所有 topic offset 提交事件流，当前阶段先打印观察
        DataStream<TopicOffsetCommitEvent> mergedTopicOffsetCommitStream = mergeTopicOffsetStreams(allTopicOffsetCommitStreams);

        if (mergedTopicOffsetCommitStream != null) {
            sinkTopicOffsetCommitStream(mergedTopicOffsetCommitStream, "jdbc-topic-offset-commit-sink");
        }

        // 添加JVM关闭钩子，确保应用关闭时提交所有offset
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Application shutting down, releasing server zk resources...");
            try {
                if (serverProcessZkService != null) {
                    serverProcessZkService.close();
                }

            } catch (Exception e) {
                logger.error("Failed to shutdown server zk resources: ", e);
            }
        }));



        env.execute("Kafka ➔ MySQL Dynamic Sync");

    }

    // 网络缓冲区配置
    public void configureNetworkBufferPool(Configuration configuration) {
//        // 调大进程内存，避免本地运行时内存过小
//        configuration.setString("taskmanager.memory.process.size", "32g");

        // 固定网络内存大小
        configuration.setString("taskmanager.memory.network.min", "2g");
        configuration.setString("taskmanager.memory.network.max", "2g");
        configuration.setString("taskmanager.memory.network.fraction", "0.125");
    }

    public void configureStatisticsWindows(SingleOutputStreamOperator<OperationResult> operationResults ,
                                           WindowTimeUnit windowTimeUnit, // 窗口长度
                                           Time time)                     // 窗口步长
            throws IOException {

        // 基于操作结果进行窗口统计
        SingleOutputStreamOperator<TableOperationResultStats> statistics = operationResults
                .keyBy(result -> result.getTableName()) // 按表名分组
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowTimeUnit.getSecondsValue()), time))  //滑动窗口，长度,步长均由参数指定
                .aggregate(
                        new OperationResultStatsAggregator(windowTimeUnit.getName()),  // 聚合窗口函数
                        new OperationResultWindowFunction()    // 全局窗口函数
                );


        //  输出统计结果到控制台（保留原有功能）;
        statistics.addSink(new OperationResultStatisticsSink());

        // 输出统计数据到zk，有问题，暂时先不输出
//        statistics.addSink(new ZooKeeperStatisticsSink(serverInfo,windowTimeUnit));
    }

//    /**
//     * 配置检查点
//     * @param env Flink流执行环境
//     * @throws IOException
//     */
//    public void configureCheckpointing(StreamExecutionEnvironment env) throws IOException {
//        logger.info("⚙️ 配置检查点...");
//
//        // TODO 检查点常用配置
//        // 1、启用检查点: 默认是barrier对齐的，周期为10分钟, 精准一次
//        env.enableCheckpointing( 3 * 60 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
//
//        // 检查点的配置对象
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//
//        // 2、指定检查点的存储位置
//        // 2、指定检查点的存储位置
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(stateBackendProperties.getCheckpointPath());
//
//        // 3、checkpoint的超时时间: 默认30分钟，超过这个时间，认为当前checkpoint保存失败
//        checkpointConfig.setCheckpointTimeout(1 * 60 * 60 * 1000L);
//
//        // 4、同时运行中的checkpoint的最大数量
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//
//        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔，如果设置了 > 0，那么并发就会变成1
//        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 60 * 1000L);
//
//        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
//        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
//        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
//        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//
//        // 7、允许 checkpoint 连续失败的次数，默认0--》表示checkpoint一失败，job就挂掉
//        checkpointConfig.setTolerableCheckpointFailureNumber(100);
//
//
//        // TODO 开启 非对齐检查点（barrier非对齐）
//        // 开启的要求： Checkpoint模式必须是精准一次，最大并发必须设为1
//        checkpointConfig.enableUnalignedCheckpoints();
////        // 开启非对齐检查点才生效： 默认0，表示一开始就直接用 非对齐的检查点
////        // 如果大于0， 一开始用 对齐的检查点（barrier对齐）， 对齐的时间超过这个参数，自动切换成 非对齐检查点（barrier非对齐）
//        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));
//    }


    /**
     * 获取某 JobID 下最新存在 _metadata 文件的 checkpoint 路径（chk-xxx），用于作为 savepoint 恢复。
//     * @param checkpointBasePath 例如 file:///F:/flink-checkpoints/demo
//     * @param jobIdHex 例如 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
     * @return _metadata 所在的 chk-x 目录的 URI 字符串，如果没有则返回 null
     * @throws Exception
     */
//    public static String findLatestCheckpointPathWithMetadata(String checkpointBasePath, String jobIdHex) throws Exception {
//        // 处理 file:/// 前缀，转成本地路径
//        String localBasePath = new Path(checkpointBasePath).toUri().getPath();
//
//        File jobDir = new File(localBasePath, jobIdHex);
//        if (!jobDir.exists() || !jobDir.isDirectory()) {
//            return null;
//        }
//
//        File[] chkDirs = jobDir.listFiles((dir, name) -> name.startsWith("chk-"));
//        if (chkDirs == null || chkDirs.length == 0) {
//            return null;
//        }
//
//        // 按 chk-数字 降序排序
//        Arrays.sort(chkDirs, Comparator.comparing(File::getName).reversed());
//
//        for (File chkDir : chkDirs) {
//            File metadata = new File(chkDir, "_metadata");
//            if (metadata.exists() && metadata.isFile()) {
//                return metadata.getParentFile().toURI().toString(); // chk-x 的路径
//            }
//        }
//
//        return null;
//    }

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
//                metric.put("unconsumedMessageCount",kafkaUtils.getUnconsumedMessageCount(serverTopicInfo.getTopic(),buildGroupId(serverTopicInfo.getTopic())).toString());
//
//                offsetManager.updateUnconsumedMessageCount(metric);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//    }

    public void sinkStringStreamToKafka(DataStream<String> stream, String topic, String sinkName) {
        stream
                .sinkTo(KafkaSink.<String>builder()
                        .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("connections.max.idle.ms", "540000")
                        .setProperty("max.block.ms", "60000")
                        .setProperty("metadata.max.age.ms", "300000")
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
                .name(sinkName);
    }


    public void sinkTopicOffsetCommitStream(
            DataStream<TopicOffsetCommitEvent> stream,
            String sinkName) {

        // 将 topic offset 提交事件批量持久化到 CDC_TOPIC_OFFSET 表
        stream
                .addSink(new TopicOffsetCommitJdbcSink(configDataSourceConfig))
                .name(sinkName);
    }

    // ========= 初始化（在 serverInfo 已加载后调用一次） =========

    /**
     * 初始化 Kafka 消费组前缀。
     * 建议在 prepareForJobExecution() 中、loadServerInfoConfig(...) 之后调用。
     */
    private void initGroupIdPrefix() {
        if (serverInfo == null || serverInfo.getServerId() == null) {
            throw new IllegalStateException("serverInfo or serverId is null, cannot init groupIdPrefix");
        }
        this.groupIdPrefix = "flink-k2db-" + serverInfo.getServerId() + "-";
    }

    /**
     * 按 topic 生成稳定 groupId。
     */
    private String buildGroupId(String kafkaTopic) {
        if (groupIdPrefix == null) {
            throw new IllegalStateException("groupIdPrefix is not initialized");
        }
        if (kafkaTopic == null || kafkaTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("kafkaTopic is blank");
        }
        return groupIdPrefix + kafkaTopic;
    }



    /**
     * 合并同步日志侧输出流。
     *
     * 当前用于合并每个 topic 的 sync log side output，
     * 合并后统一写入 server log Kafka topic。
     */
    private DataStream<String> mergeSyncLogStreams(List<DataStream<String>> streams) {
        return mergeStreamsBalanced(streams, STREAM_UNION_BATCH_SIZE);
    }

    /**
     * 合并 topic offset commit 侧输出流。
     *
     * 当前用于把每个 topic 的 offset commit event 合并后统一写入 JDBC sink。
     */
    private DataStream<TopicOffsetCommitEvent> mergeTopicOffsetStreams(
            List<DataStream<TopicOffsetCommitEvent>> streams) {
        return mergeStreamsBalanced(streams, STREAM_UNION_BATCH_SIZE);
    }

    /**
     * 分层合并 DataStream，避免大量 DataStream 链式 union 导致 Flink 生成 StreamGraph 时递归过深。
     *
     * 示例：
     * 4000 条流，batchSize = 64。
     *
     * 第 1 轮：
     *   每 64 条合并成 1 条，4000 条变成约 63 条。
     *
     * 第 2 轮：
     *   继续每 64 条合并成 1 条，约 63 条变成 1 条。
     *
     * 这样 StreamGraph 的 union 层级会从几千层降低到少数几层。
     *
     * @param streams   待合并的流
     * @param batchSize 每一轮每组最多合并多少条流
     * @param <T>       流中的元素类型
     * @return 合并后的流；如果输入为空，返回 null
     */
    private <T> DataStream<T> mergeStreamsBalanced(List<DataStream<T>> streams, int batchSize) {
        if (streams == null || streams.isEmpty()) {
            return null;
        }

        if (streams.size() == 1) {
            return streams.get(0);
        }

        // 防御性处理，避免误配置为 0 或负数。
        int effectiveBatchSize = batchSize <= 1 ? 64 : batchSize;

        // current 表示当前轮待合并的流列表。
        List<DataStream<T>> current = new ArrayList<>(streams);

        while (current.size() > 1) {
            // next 保存本轮合并后的结果，作为下一轮的输入。
            List<DataStream<T>> next = new ArrayList<>(
                    (current.size() + effectiveBatchSize - 1) / effectiveBatchSize
            );

            for (int i = 0; i < current.size(); i += effectiveBatchSize) {
                int end = Math.min(i + effectiveBatchSize, current.size());

                // 合并当前分组内的流。每组最多 effectiveBatchSize 条。
                DataStream<T> merged = current.get(i);
                for (int j = i + 1; j < end; j++) {
                    merged = merged.union(current.get(j));
                }

                next.add(merged);
            }

            current = next;
        }

        return current.get(0);
    }

//    /**
//     * 验证 topic offset 仓储的读写链路。
//     *
//     * 说明：
//     * 1. 这是临时验证方法，只用于确认 Oracle offset 表可正常读写
//     * 2. 当前不接入主消费流程
//     */
//    private void verifyTopicOffsetRepository() throws Exception {
//        // serverId 作为 offset 主键的一部分，不能为空
//        String serverId = serverInfo == null ? null : serverInfo.getServerId();
//        if (serverId == null || serverId.trim().isEmpty()) {
//            throw new IllegalStateException("serverId is blank, cannot verify topic offset repository");
//        }
//
//        // 使用一个固定测试 topic，避免影响真实 topic 恢复逻辑
//        String testTopic = "__offset_repo_test_topic__";
//
//        // 写入一个固定测试值，便于日志核对
//        long testNextOffset = 12345L;
//
//        // 先打印当前 server 已有的 offset 条数
//        Map<String, Long> before = topicOffsetRepository.loadNextOffsets(serverId);
//        logger.info("topic offset repository verify - before write, serverId={}, topicCount={}",
//                serverId, before.size());
//
//        // 写入测试 offset
//        topicOffsetRepository.upsertNextOffset(serverId, testTopic, testNextOffset);
//        logger.info("topic offset repository verify - wrote test offset, serverId={}, topic={}, nextOffset={}",
//                serverId, testTopic, testNextOffset);
//
//        // 再读取一次，确认能读到刚写入的数据
//        Map<String, Long> after = topicOffsetRepository.loadNextOffsets(serverId);
//        Long actual = after.get(testTopic);
//
//        // 验证读取结果是否符合预期
//        if (actual == null || actual.longValue() != testNextOffset) {
//            throw new IllegalStateException(
//                    "topic offset repository verify failed, expected=" + testNextOffset + ", actual=" + actual
//            );
//        }
//
//        logger.info("topic offset repository verify - success, serverId={}, topic={}, nextOffset={}",
//                serverId, testTopic, actual);
//    }


    // 一次性读取当前 server 已持久化的 topic -> nextOffset
    private Map<String, Long> loadPersistedTopicOffsets(String serverId) throws Exception {
        // 从 CDC_TOPIC_OFFSET 表中读取当前 server 的全部 topic offset
        Map<String, Long> persistedOffsets = topicOffsetRepository.loadNextOffsets(serverId);

        // 防御性处理，避免 repository 返回 null
        if (persistedOffsets == null) {
            persistedOffsets = Collections.emptyMap();
        }

        // 记录启动时读取到的 offset 条数
        logger.info("加载到已经持久化的topic offsets, serverId={}, topicCount={}",
                serverId, persistedOffsets.size());

        return persistedOffsets;
    }

//    private OffsetsInitializer buildStartingOffsetsInitializer(
//            String kafkaTopic,
//            Map<String, Long> persistedOffsets) {
//
//        // 读取当前 topic 持久化的 nextOffset
//        Long nextOffset = persistedOffsets.get(kafkaTopic);
//
//        // 如果没有持久化 offset，则兜底从最早开始消费
//        if (nextOffset == null) {
//            logger.info("No persisted offset found, fallback to earliest, topic={}", kafkaTopic);
//            return OffsetsInitializer.earliest();
//        }
//
//        // 当前阶段约定每个 topic 只有 0 号分区
//        Map<TopicPartition, Long> startingOffsets = new HashMap<>();
//        startingOffsets.put(new TopicPartition(kafkaTopic, 0), nextOffset);
//
//        // 使用持久化的 nextOffset 作为启动 offset
//        logger.info("Using persisted nextOffset as starting offset, topic={}, partition=0, nextOffset={}",
//                kafkaTopic, nextOffset);
//
//        return OffsetsInitializer.offsets(startingOffsets);
//    }

    // 按目标数据源和目标表对 topic 分组
    private Map<TargetTableKey, List<ServerTopicInfo>> groupTopicInfosByTargetTable() {
        // 创建分组结果，保留原始 topic 顺序
        Map<TargetTableKey, List<ServerTopicInfo>> grouped = new LinkedHashMap<>();

        // 遍历当前 server 需要消费的所有 topic
        for (ClientTopicInfo clientTopicInfo : topicInfoRegistry.getServerTopicInfoList()) {
            // 当前执行计划中实际应为 ServerTopicInfo
            ServerTopicInfo serverTopicInfo = (ServerTopicInfo) clientTopicInfo;

            // 为每个topic创建TargetTableKey(构造目标表分组键)
            TargetTableKey key = new TargetTableKey(
                    serverTopicInfo.getTargetDataSourceId(),
                    serverTopicInfo.getTargetTableName()
            );

            // 将 topic 加入目标表分组
            grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(serverTopicInfo);
        }

        // 校验每个目标表分组
        validateTargetTableTopicGroups(grouped);

        // 返回分组结果
        return grouped;
    }

    // 校验目标表 topic 分组，避免同一目标表混合不同 sourceKind
    private void validateTargetTableTopicGroups(Map<TargetTableKey, List<ServerTopicInfo>> grouped) {
        // 空分组直接返回
        if (grouped == null || grouped.isEmpty()) {
            return;
        }

        // 遍历每个目标表分组
        for (Map.Entry<TargetTableKey, List<ServerTopicInfo>> entry : grouped.entrySet()) {
            // 校验当前分组的 sourceKind
            validateSingleTargetTableTopicGroup(entry.getKey(), entry.getValue());
        }
    }

    // 校验单个目标表分组
    private void validateSingleTargetTableTopicGroup(TargetTableKey key,
                                                     List<ServerTopicInfo> topicInfos) {
        // 空 topic 列表直接失败
        if (topicInfos == null || topicInfos.isEmpty()) {
            throw new IllegalStateException("empty topic group, targetTableKey=" + key);
        }

        // 保存第一个非空 sourceKind
        TopicSourceKind firstSourceKind = null;

        // 遍历当前目标表下的 topic
        for (ServerTopicInfo topicInfo : topicInfos) {
            // 跳过空对象
            if (topicInfo == null) {
                continue;
            }

            // 空 sourceKind 默认 TABLE_CDC
            TopicSourceKind currentSourceKind = topicInfo.getSourceKind() == null
                    ? TopicSourceKind.TABLE_CDC
                    : topicInfo.getSourceKind();

            // 初始化第一个 sourceKind
            if (firstSourceKind == null) {
                firstSourceKind = currentSourceKind;
                continue;
            }

            // 不允许同一目标表混合不同 sourceKind
            if (firstSourceKind != currentSourceKind) {
                throw new IllegalStateException("mixed sourceKind in same target table group"
                        + ", targetTableKey=" + key
                        + ", firstSourceKind=" + firstSourceKind
                        + ", currentSourceKind=" + currentSourceKind
                        + ", topic=" + topicInfo.getTopic());
            }
        }
    }

    // 构造目标表写入上下文
    private TargetTableWriteContext buildTargetTableWriteContext(TargetTableKey key,
                                                                 List<ServerTopicInfo> topicInfos) {
        // 根据目标数据源 ID 查找目标库配置
        TargetDataSourceConfig targetDataSourceConfig =
                targetDataSourceConfigRegistry.findTargetDataSourceConfigByDataSourceId(key.getTargetDataSourceId());

        // 目标库配置不存在时直接失败，避免 writer 运行期空指针
        if (targetDataSourceConfig == null) {
            throw new IllegalStateException("targetDataSourceConfig not found, targetDataSourceId="
                    + key.getTargetDataSourceId());
        }

        // 创建目标表写入上下文
        return new TargetTableWriteContext(key, targetDataSourceConfig, topicInfos);
    }

    // 按目标表生成稳定 Kafka consumer groupId
    private String buildGroupId(TargetTableKey targetTableKey) {
        // groupId 前缀必须已初始化
        if (groupIdPrefix == null) {
            throw new IllegalStateException("groupIdPrefix is not initialized");
        }

        // 目标表分组键不能为空
        if (targetTableKey == null) {
            throw new IllegalArgumentException("targetTableKey is null");
        }

        // 生成目标表级别 groupId
        return groupIdPrefix
                + sanitizeGroupIdPart(targetTableKey.getTargetDataSourceId())
                + "-"
                + sanitizeGroupIdPart(targetTableKey.getTargetTableName());
    }

    // 清理 groupId 片段，避免特殊字符影响 Kafka groupId 可读性
    private String sanitizeGroupIdPart(String value) {
        // 空值统一成 unknown
        if (value == null || value.trim().isEmpty()) {
            return "unknown";
        }

        // 非字母数字下划线横线点号的字符替换为下划线
        return value.trim().replaceAll("[^A-Za-z0-9_.-]", "_");
    }

    // 从 ServerTopicInfo 列表中提取 topic 名
    private List<String> extractKafkaTopics(List<ServerTopicInfo> topicInfos) {
        // 创建 topic 列表
        List<String> topics = new ArrayList<>();

        // 遍历 topic 信息
        for (ServerTopicInfo topicInfo : topicInfos) {
            // 跳过空对象
            if (topicInfo == null) {
                continue;
            }

            // 跳过空 topic
            if (topicInfo.getTopic() == null || topicInfo.getTopic().trim().isEmpty()) {
                continue;
            }

            // 加入 topic 列表
            topics.add(topicInfo.getTopic().trim());
        }

        // 返回 topic 列表
        return topics;
    }

//    // 为多个 topic 构造启动 offset，每个 topic 独立读取 CDC_TOPIC_OFFSET，缺失则 earliest
//    private OffsetsInitializer buildStartingOffsetsInitializer(List<String> kafkaTopics,
//                                                               Map<String, Long> persistedOffsets) {
//        // topic 列表不能为空
//        if (kafkaTopics == null || kafkaTopics.isEmpty()) {
//            throw new IllegalArgumentException("kafkaTopics is empty");
//        }
//
//        // 防御性处理，避免空 map
//        Map<String, Long> safePersistedOffsets =
//                persistedOffsets == null ? Collections.emptyMap() : persistedOffsets;
//
//        // 保存有持久化 offset 的 topic 分区
//        Map<TopicPartition, Long> startingOffsets = new LinkedHashMap<>();
//
//        // 逐个 topic 读取持久化 offset
//        for (String kafkaTopic : kafkaTopics) {
//            // 跳过空 topic
//            if (kafkaTopic == null || kafkaTopic.trim().isEmpty()) {
//                continue;
//            }
//
//            // 标准化 topic
//            String normalizedTopic = kafkaTopic.trim();
//
//            // 读取当前 topic 的 nextOffset
//            Long nextOffset = safePersistedOffsets.get(normalizedTopic);
//
//            // 没有 offset 的 topic 交给 earliest fallback
//            if (nextOffset == null) {
//                logger.info("No persisted offset found, fallback to earliest, topic={}", normalizedTopic);
//                continue;
//            }
//
//            // 约定每个 topic 只有 0 号分区
//            TopicPartition topicPartition = new TopicPartition(normalizedTopic, 0);
//
//            // 保存当前 topic 的启动 offset
//            startingOffsets.put(topicPartition, nextOffset);
//
//            // 记录当前 topic 的启动 offset
//            logger.info("Using persisted nextOffset as starting offset, topic={}, partition=0, nextOffset={}",
//                    normalizedTopic, nextOffset);
//        }
//
//        // 如果当前分组没有任何 topic 有持久化 offset，则整组从 earliest 启动
//        if (startingOffsets.isEmpty()) {
//            logger.info("No persisted offsets found for target table group, fallback all topics to earliest, topics={}",
//                    kafkaTopics);
//            return OffsetsInitializer.earliest();
//        }
//
//        // 有 offset 的 topic 使用指定 offset，缺失的 topic 使用 earliest
//        return OffsetsInitializer.offsets(startingOffsets, OffsetResetStrategy.EARLIEST);
//    }

    // 打印目标表分组摘要
    private void logTargetTableTopicGroups(Map<TargetTableKey, List<ServerTopicInfo>> topicGroups) {
        // 空分组直接记录
        if (topicGroups == null || topicGroups.isEmpty()) {
            logger.warn("no target table topic groups found");
            return;
        }

        // 统计 topic 总数
        int topicCount = 0;

        // 遍历分组统计 topic 数
        for (List<ServerTopicInfo> topicInfos : topicGroups.values()) {
            // 累加 topic 数
            topicCount += topicInfos == null ? 0 : topicInfos.size();
        }

        // 打印汇总信息
        logger.info("target table topic groups initialized, groupCount={}, topicCount={}",
                topicGroups.size(), topicCount);

        // 打印每组摘要
        for (Map.Entry<TargetTableKey, List<ServerTopicInfo>> entry : topicGroups.entrySet()) {
            // 获取分组 key
            TargetTableKey key = entry.getKey();

            // 获取 topic 列表
            List<ServerTopicInfo> topicInfos = entry.getValue();

            // 获取 sourceKind
            TopicSourceKind sourceKind = resolveGroupSourceKind(topicInfos);

            // 打印分组信息
            logger.info("target table group, key={}, sourceKind={}, topicCount={}",
                    key, sourceKind, topicInfos == null ? 0 : topicInfos.size());
        }
    }

    // 解析目标表分组 sourceKind
    private TopicSourceKind resolveGroupSourceKind(List<ServerTopicInfo> topicInfos) {
        // 空分组默认 TABLE_CDC
        if (topicInfos == null || topicInfos.isEmpty()) {
            return TopicSourceKind.TABLE_CDC;
        }

        // 取第一个 topic
        ServerTopicInfo first = topicInfos.get(0);

        // 空 sourceKind 默认 TABLE_CDC
        return first.getSourceKind() == null ? TopicSourceKind.TABLE_CDC : first.getSourceKind();
    }

    // 为目标表分组内的多个 topic 构造启动 offset
    private OffsetsInitializer buildStartingOffsetsInitializerForTopics(List<String> kafkaTopics,
                                                                        Map<String, Long> persistedOffsets) {
        // topic 列表不能为空
        if (kafkaTopics == null || kafkaTopics.isEmpty()) {
            throw new IllegalArgumentException("kafkaTopics is empty");
        }

        // 防御性处理，避免空 map
        Map<String, Long> safePersistedOffsets =
                persistedOffsets == null ? Collections.emptyMap() : persistedOffsets;

        // 保存有持久化 offset 的 topic 分区
        Map<TopicPartition, Long> startingOffsets = new LinkedHashMap<>();

        // 逐个 topic 读取持久化 offset
        for (String kafkaTopic : kafkaTopics) {
            // 跳过空 topic
            if (kafkaTopic == null || kafkaTopic.trim().isEmpty()) {
                continue;
            }

            // 标准化 topic
            String normalizedTopic = kafkaTopic.trim();

            // 读取当前 topic 的 nextOffset
            Long nextOffset = safePersistedOffsets.get(normalizedTopic);

            // 没有 offset 的 topic 交给 earliest fallback
            if (nextOffset == null) {
                logger.info("No persisted offset found, fallback to earliest, topic={}", normalizedTopic);
                continue;
            }

            // 约定每个 topic 只有 0 号分区
            TopicPartition topicPartition = new TopicPartition(normalizedTopic, 0);

            // 保存当前 topic 的启动 offset
            startingOffsets.put(topicPartition, nextOffset);

            // 记录当前 topic 的启动 offset
            logger.info("Using persisted nextOffset as starting offset, topic={}, partition=0, nextOffset={}",
                    normalizedTopic, nextOffset);
        }

        // 当前分组没有任何持久化 offset 时，整组从 earliest 启动
        if (startingOffsets.isEmpty()) {
            logger.info("No persisted offsets found for target table group, fallback all topics to earliest, topics={}",
                    kafkaTopics);
            return OffsetsInitializer.earliest();
        }

        // 有 offset 的 topic 用指定 offset，缺失 topic 用 earliest
        return OffsetsInitializer.offsets(startingOffsets, OffsetResetStrategy.EARLIEST);
    }

    // 构造 Flink operator 名称片段
    private String buildOperatorNamePart(TargetTableKey targetTableKey) {
        // 空 key 返回 unknown
        if (targetTableKey == null) {
            return "unknown";
        }

        // 拼接目标数据源和目标表
        String raw = targetTableKey.getTargetDataSourceId() + "-" + targetTableKey.getTargetTableName();

        // 替换特殊字符
        return raw.replaceAll("[^A-Za-z0-9_.-]", "_");
    }
}
