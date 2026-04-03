package com.bsoft.server.k2db;


import com.bsoft.common.bean.*;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.WindowTimeUnit;
import com.bsoft.common.factory.TableNameGenerator;
import com.bsoft.common.global.GlobalLogCollectorManager;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import com.bsoft.server.api.ServerController;
import com.bsoft.server.function.OperationResultStatsAggregator;
import com.bsoft.server.function.OperationResultWindowFunction;
import com.bsoft.server.sink.OperationResultStatisticsSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;


// 注册为 Spring 管理的组件
@Component
public class KafkaToDB  extends CDCJob {


    private static final Logger logger = LogManager.getLogger(KafkaToDB.class);

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

    @Autowired
    private ServerController serverController;

    @Autowired
    private TableNameGenerator tableNameGenerator;

    /**
     * 稳定消费组前缀：flink-k2db-{serverId}-
     * 约定 serverId 在当前实例生命周期内不变。
     */
    private String groupIdPrefix;

    private PipelineSqlSinkTopicResolver pipelineSqlSinkTopicResolver;

    private List<RulePlan> rulePlans = Collections.emptyList();



    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(this.serverInfo);
    }

    public void startJob() {
        try {

            // 1. 初始化zk节点,ip+status，并注册当前服务端ip+port
            initServerZKNode();

            // 2. 通知初始化开始
            notifyJobStatusChange(JobStatus.INITIALIZING);

            // 3.从配置库读取各种配置
            prepareForJobExecution();

            // 4.根据配置库的内容，初始化当前执行所需的环境
            initServerExecutionEnvironment();

            // 5.是从 kafka 读取数据，写入目标库，可同时写入多个目标库
            pullFromKafkaAndPublishToDB();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
        serverController.init(targetDataSourceConfigRegistry);
        serverController.startWorkers();
        serverController.buildDorisBizAttrBySqlNodeId(rulePlans);



        // 4. 生成需要读取主题信息，包括每个主题的数据最终写入哪个目标库的哪个表
        topicInfoRegistry.registerServerTopicInfo(executionPlan.generateServerTopicInfo(businessDataSourceConfigRegistry,
                targetDataSourceConfigRegistry,
                serverInfo,
                configFetcher));






        // 5. 检查需要监听的主题是否已经创建，还没创建则等待创建
        List<String> topics = topicInfoRegistry.getServerTopicInfoList().stream()
                .map(ClientTopicInfo::getTopic)
                .collect(Collectors.toList());
        kafkaUtils.waitUntilTopicExists(topics);


        // 6. 按需创建 日志，监控 主题
        kafkaUtils.createKafkaTopicsIfNotExists(Arrays.asList(this.serverInfo.getServerLogTopic(), this.serverInfo.getMonitorMetricTopic()));

        // 7. 初始化 groupId 前缀
        initGroupIdPrefix();


        // 8. 注册flink sql对应的主题
        this.pipelineSqlSinkTopicResolver = new PipelineSqlSinkTopicResolver(configFetcher,targetDataSourceConfigRegistry,tableNameGenerator);
        List<ServerTopicInfo> sqlSinkTopics = pipelineSqlSinkTopicResolver.resolve(this.rulePlans);
        topicInfoRegistry.registerServerTopicInfo(sqlSinkTopics);

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
            businessDataSourceConfig.setSubscriberMappingRegistry(subscriberMappingRegistry);
            // 每个业务源，可能有多个 目标库
            businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 读取业务库中每个需要同步的表的元数据，创建对应的 TableMappingConfig
            businessDataSourceConfig.init();
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

        // 应用检查点
        configureCheckpointing(env);


        // 定义侧输出标签，用于输入日志
        OutputTag<String> syncLogEntryOutputTag = new OutputTag<String>("sync-server-log-side-output") {};
        // 收集所有日志侧输出流
        List<DataStream<String>> allLogStreams = new ArrayList<>();

        // 定义侧输出标签，用于输入日志
        OutputTag<String> monitorMetricOutputTag = new OutputTag<String>("sync-server-metric-side-output") {};
        // 收集所有日志侧输出流
        List<DataStream<String>> allMonitorMetricOutputTagStreams = new ArrayList<>();

        // 读取每个主题的数据
        for (ClientTopicInfo topicInfo : topicInfoRegistry.getServerTopicInfoList()) {

            ServerTopicInfo serverTopicInfo = (ServerTopicInfo)topicInfo;
            // 主题名
            String kafkaTopic = serverTopicInfo.getTopic();

            // 稳定 groupId：serverId + topic，便于断点续读
            String groupId = buildGroupId(kafkaTopic);

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

            // 解析Kafka消息并处理数据库写入，获得操作结果
            SingleOutputStreamOperator<OperationResult> operationResults = kafkaDataStream
                    .map(OracleCdcJsonParser::parseKafkaMessage)
                    .name("parse-kafka-message-" + kafkaTopic)
                    .filter(kafkaPayload -> {
                        boolean isValid = kafkaPayload != null;
                        if (!isValid) {
                            logger.debug("🚫 过滤掉无效的Kafka消息");
                        }
                        return isValid;
                    })
                    .name("filter-" + kafkaTopic).uid("filter-" + kafkaTopic)
                    .process(new DatabaseRichProcessBatchFunction(serverInfo,serverTopicInfo,syncLogEntryOutputTag,monitorMetricOutputTag)) // 使用ProcessFunction处理写入
                    .name("target-writer-" + kafkaTopic).uid("target-writer-" + kafkaTopic);


            // 收集获取控制流（侧输出流）
            DataStream<String> syncLogEntryDataStream = operationResults.getSideOutput(syncLogEntryOutputTag);
            allLogStreams.add(syncLogEntryDataStream);

            DataStream<String> metricEventDataStream = operationResults.getSideOutput(monitorMetricOutputTag);
            allMonitorMetricOutputTagStreams.add(metricEventDataStream);

//          // 配置统计窗口,长度为一天，步长100分钟
            configureStatisticsWindows(operationResults, WindowTimeUnit.DAY,Time.seconds(100 * 60));
            // 配置统计窗口,长度为一小时，步长10分钟
            configureStatisticsWindows(operationResults, WindowTimeUnit.HOUR,Time.seconds(10 * 60));
            // 配置统计窗口,长度为一分钟，步长30秒
            configureStatisticsWindows(operationResults, WindowTimeUnit.MINUTE,Time.seconds(1 * 30));


        }

        //  合并所有日志流后统一 sink(只创建 1 个 KafkaSink)
        DataStream<String> mergedLogStream = mergeStreams(allLogStreams);
        if (mergedLogStream != null) {
            sinkStringStreamToKafka(mergedLogStream, this.serverInfo.getServerLogTopic(), "KafkaSink-sync-log");
        }

        DataStream<String> mergedMetricStream = mergeStreams(allMonitorMetricOutputTagStreams);
        if (mergedMetricStream != null) {
            sinkStringStreamToKafka(mergedMetricStream, this.serverInfo.getMonitorMetricTopic(), "KafkaSink-monitor-metric");
        }



        // 添加JVM关闭钩子，确保应用关闭时提交所有offset
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Application shutting down, flushing offsets...");
            try {
                // 1. 关闭全局 ZK 管理器
                GlobalZooKeeperManager instance = GlobalZooKeeperManager.getInstance(serverInfo);
                if (instance != null) {
                    instance.shutdown();
                }

                // 2. 关闭所有 LogCollector
                GlobalLogCollectorManager.shutdownAll();
            } catch (Exception e) {
                logger.error("Failed to shutdown GlobalZooKeeperOffsetManager: ", e);
            }
        }));



        env.execute("Kafka ➔ MySQL Dynamic Sync");

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


    private DataStream<String> mergeStreams(List<DataStream<String>> streams) {
        if (streams == null || streams.isEmpty()) {
            return null;
        }
        DataStream<String> merged = streams.get(0);
        for (int i = 1; i < streams.size(); i++) {
            merged = merged.union(streams.get(i));
        }
        return merged;
    }
}
