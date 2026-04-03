package com.bsoft.client.o2k;

import com.bsoft.client.api.ClientController;
import com.bsoft.client.function.OracleSourceLogRichMapFunction;
import com.bsoft.client.function.SideOutputProcessFunction;
import com.bsoft.client.function.SnapshotControlProcessFunction;
import com.bsoft.client.dispatcher.ClientJobDispatcher;
import com.bsoft.client.function.SnapshotSignalSplitProcessFunction;
import com.bsoft.client.signal.ClusterAwareStopSignalSink;
import com.bsoft.client.signal.JobStopMonitor;
import com.bsoft.common.bean.*;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.JobStartupMode;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.TableStatus;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;

import com.bsoft.common.util.OraclePrereqChecker;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.OracleSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Oracle CDC 到 Kafka 的数据同步组件
 * 支持正常模式和快照模式两种运行方式
 */
@Component // 注册为 Spring 管理的组件
public class OracleToKafka extends CDCJob {





    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;
    // kafka工具类
    @Autowired
    private KafkaUtils kafkaUtils;


    // 中心端工具，用来发送建表语句
    @Autowired
    private ClientController clientController;

    // 业务库
    private BusinessDataSourceConfig businessDataSourceConfig;

    // 目标库
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 当前业务库的订阅关系
    private SubscriberMappingRegistry subscriberMappingRegistry;


    // 状态后端配置
    @Autowired
    private StateBackendProperties stateBackendProperties;

    private JobClient jobClient;

    // 集群停止信号监控器
    private JobStopMonitor jobStopMonitor;


    private ClientJobDispatcher clientJobDispatcher;


    public void init(ClientJobDispatcher clientJobDispatcher) throws Exception {
        this.clientJobDispatcher = clientJobDispatcher;
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(clientInfo);
    }



    /**
     * 启动作业
     * @throws Exception
     */
    public void startJob() throws Exception {

        // 1.初始化当前客户端的zk节点,更新状态,ip,清空信号
        initClientZKNode();

        // 2.从配置库读取各种配置，如果配置不完整，间隔10秒重试
        prepareForJobExecution();

        // 3.验证源库是否满足CDC的前置条件
//        verifyOracleCdcPrerequisites();

        // 4.解析配置库的内容，初始化客户端执行环境，比如获取表的主键等,向中心端发送建表语句等
        initClientExecutionEnvironment();

        // 5.分析配置内容，制定执行计划，决定job的启动模式，JobStartupMode设置到ClientInfo中
        this.executionPlan = configFetcher.determineJobStartupMode();

        // 6.生成本次作业的topic，并在Kafka创建topic
        initKafkaTopics();


        // 7.设置任务状态，（准备中，还未运行）
        switch (clientInfo.getJobStartupMode()) {
            case NORMAL:
                notifyJobStatusChange(JobStatus.INCREMENTAL_INITIALIZING);
                break;
            case SNAPSHOT:
                notifyJobStatusChange(JobStatus.SNAPSHOT_INITIALIZING);
                // 对于快照任务，创建 停止信号监听器
                jobStopMonitor = new JobStopMonitor(clientInfo,this.clientJobDispatcher);
                break;
            default:
                throw new IllegalStateException("Unsupported job startup mode: " + clientInfo.getJobStartupMode());
        }
        // 6. 启动 CDC 捕获并发布到 Kafka
        captureOracleCdcAndPublishToKafka();
    }

    // 验证源库是否满足CDC的前置条件
    public void verifyOracleCdcPrerequisites() throws Exception {
        if (clientInfo.isOracleCdcPreRequisites()) {
            int attempt = 1;
            OraclePrereqChecker.Result result = null;

            while (true) {
                try {
                    logger.info("第 {} 次检查 Oracle CDC 前置条件...", attempt++);
                    result = configFetcher.assertOracleCdcReady(businessDataSourceConfig);

                    if (!result.ok()) {
                        result.logToSlf4j();
                        throw new IllegalStateException("Oracle CDC 前置条件检查失败，请根据日志补齐缺项后重试。");
                    }

                    logger.info(" ----------    Congratulations ！！！！！！,Oracle CDC 前置条件检查通过    --------------");
                    break;
                } catch (Exception e) {
                    notifyJobStatusChange(JobStatus.ORACLE_CDC_PRECONDITION_NOT_MET.withDetailInfo(
                            result != null ? result.errors().toString() : "未知错误"));
                    Thread.sleep(10000);
                }
            }
        }
    }


    public void prepareForJobExecution() throws Exception {
        // 1.启动时，注册客户端，便于配置端分配数据源
        configFetcher.registerClientIfAbsent(clientInfo);

        // 2.从配置库读取 业务库数据源，订阅关系，所有目标库
        do{
            // 短路检查：任何一个配置获取失败都立即重试
            if (loadAllConfigurations()) {  // 如果所有配置都加载成功
                break;                      // 跳出循环
            }
        }
        while(!checkPreconditions());  // 检查必要的配置
    }

    /**
     * 加载所有必要的配置，短路返回
     * @return 如果所有配置都成功加载返回 true，否则返回 false
     */
    private boolean loadAllConfigurations() throws Exception {
        // 3-1.读取业务库数据源的基本配置，包括url，用户名，密码等，
        // 一个客户端只对应一个业务库数据源
        this.businessDataSourceConfig = configFetcher.getBusinessDataSourceBasicConfig();
        if (this.businessDataSourceConfig == null) {
            logger.warn("当前客户端未被配置 业务库数据源，将在10秒后重试");
            notifyJobStatusChange(JobStatus.CONFIG_MISSING.withDetailInfo("当前客户端未被配置 业务库数据源，将在10秒后重试"));
            Thread.sleep(10000);
            return false; // 短路返回
        }

        // 3-2.读取当前业务库的订阅关系,为了安全起见，把所有 当前业务库 的订阅关系都读出来,
        // 订阅表中可能存在的一对一，一对多，多对多等乱七八糟的关系，这里全部解析成一对一的关系，最后还是合并成一个SubscriberMappingRegistry，其持有多个SubscriberMapping
        this.subscriberMappingRegistry = configFetcher.getSubscriberMapping(businessDataSourceConfig);
        if (this.subscriberMappingRegistry == null) {
            logger.warn("订阅关系配置缺失，将在10秒后重试");
            notifyJobStatusChange(JobStatus.CONFIG_MISSING.withDetailInfo("订阅关系配置缺失，将在10秒后重试"));
            Thread.sleep(10000);
            return false; // 短路返回
        }

        // 3-3.读取当前业务库的所有目标库
        this.targetDataSourceConfigRegistry = configFetcher.getTargetDataSourceConfigRegistry(businessDataSourceConfig);
        if (this.targetDataSourceConfigRegistry == null) {
            logger.warn("目标数据源配置缺失，将在10秒后重试");
            notifyJobStatusChange(JobStatus.CONFIG_MISSING.withDetailInfo("目标数据源配置缺失，将在10秒后重试"));
            Thread.sleep(10000);
            return false; // 短路返回
        }

        return true; // 所有配置都加载成功
    }
    // 检查必要的配置
    public boolean checkPreconditions() {
        return false;
    }



    // 主任务启动流程：从配置库读取当前客户端执行所需的环境
    public void initClientExecutionEnvironment() throws Exception {
        // 1.建立 生产库，订阅关系，目标库之间的关系,获取源表主键
        postProcessBusinessDataSource();

        // 2.向中心端发送当前业务库的建表语句
//        serverClient.fetchAndSendTableMetadata(businessDataSourceConfig);
        clientController.initializeMetadataReader(this.businessDataSourceConfig);
        // 3.设置一个启动时间
        clientInfo.notifyClientStart();

        logger.info("当前 job 启动,启动时间: {}", clientInfo.printStartTime());
    }


    // 1.建立 生产库，订阅关系，目标库之间的关系,获取源表主键
    private void postProcessBusinessDataSource() throws Exception {
        // 1.建立 生产库，订阅关系，目标库之间的关系
        this.businessDataSourceConfig.setSubscriberMappingRegistry(this.subscriberMappingRegistry);
        this.businessDataSourceConfig.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);

        // 2.为每个源表，创建对应的 TableMappingConfig
        this.businessDataSourceConfig.init();

        // 3.表的主键设置到每一个 TableMappingConfig 中
        businessDataSourceConfig.assignPrimaryKeysOrUniqueKeysForAllTables();
    }


    // 主任务启动流程：从配置库读取当前客户端执行所需的环境
    public void initKafkaTopics() {
        // 1.万事俱备，生成 客户端 必须的主题名
        topicInfoRegistry.registerClientTopicInfo(executionPlan.generateClientTopicInfo(clientInfo));
        // 2.自动创建每个需要同步的表对应的Kafka Topic，一个源表可能对应主题
        kafkaUtils.createKafkaTopicsIfNotExists(topicInfoRegistry.getClientTopicInfoList());

    }



    /**
     * 从 Oracle 数据库捕获变更数据，并将其按表名实时写入对应的 Kafka Topic
     */
    public void captureOracleCdcAndPublishToKafka() throws Exception {
        logger.info("开始配置 Oracle CDC 到 Kafka 的数据流...");

        // 获取客户端id待用
        String clientId = clientInfo.getClientId();

        // 按需生成jobid
        String jobIdHex;

        switch (clientInfo.getJobStartupMode()) {
            case NORMAL:
                jobIdHex = JobIdUtil.generateFixedJobId(clientId);     // 主任务,生成固定jobid
                logger.info(" 普通模式 - 使用固定Job ID: {}", jobIdHex);
                break;
            case SNAPSHOT:
                jobIdHex = JobIdUtil.generateRandomJobId();          // 快照任务，生成随机jobid
                logger.info(" 快照模式 - 生成随机Job ID: {}", jobIdHex);
                break;
            default:
                throw new IllegalStateException("不支持的作业状态: " +  this.jobStatus);
        }

        // 创建执行环境配置
        Configuration configuration = new Configuration();
        configuration.setString("$internal.pipeline.job-id", jobIdHex);
        configuration.setString("parallelism.default", "1");

        // 主任务，需要检查checkpoint
        if(clientInfo.getJobStartupMode() == JobStartupMode.NORMAL){
            String latestCheckpointPathWithMetadata = findLatestCheckpointPathWithMetadata(
                    stateBackendProperties.getCheckpointPath(), jobIdHex);
            if (latestCheckpointPathWithMetadata != null) {
                configuration.setString("execution.savepoint.path", latestCheckpointPathWithMetadata);
                logger.info("从检查点恢复: {}", latestCheckpointPathWithMetadata);
            } else {
                logger.info(" 没有找到检查点，将从头开始");
            }
        }

        // 端口由 配置文件指定，避免默认 8081 被占用
        configuration.setInteger(RestOptions.PORT, clientInfo.getFlinkUiPort());

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);


        // 主任务，需要配置checkpoint
        if(clientInfo.getJobStartupMode() == JobStartupMode.NORMAL){
            configureCheckpointing(env);
            logger.info(" 检查点配置已启用");
        }


        // 构建 Oracle CDC Source
        logger.info(" 创建 构建 Oracle CDC Source: {}:{}/{}",
                businessDataSourceConfig.getDataSourceHost(),
                businessDataSourceConfig.getDataSourcePort(),
                businessDataSourceConfig.getDataSourceServiceName());

        SourceFunction<String> oracleSource = OracleSource.<String>builder()
                .hostname(businessDataSourceConfig.getDataSourceHost())
                .port(businessDataSourceConfig.getDataSourcePort())
                .username(businessDataSourceConfig.getDataSourceUserName())
                .password(businessDataSourceConfig.getDataSourcePassword())
                .database(businessDataSourceConfig.getDataSourceServiceName())

                .tableList(getCurrentJobSyncTables())
                .url("jdbc:oracle:thin:@//" + businessDataSourceConfig.getDataSourceHost() + ":" +
                        businessDataSourceConfig.getDataSourcePort() + "/" + businessDataSourceConfig.getDataSourceServiceName())
                .startupOptions(StartupOptions.initial())
//                .startupOptions(StartupOptions.latest())
                .debeziumProperties(DebeziumProperties.getDebeziumProperties()) // 通过Debezium属性配置DDL
                .deserializer(new CustomDebeziumDeserializationSchema(businessDataSourceConfig,clientInfo))

                .build();

        DataStream<String> oracleStream = env.addSource(oracleSource)
                .uid("oracle-cdc-source")
                .map(new OracleSourceLogRichMapFunction(this.clientInfo))      // 更新scn
                .uid("oracle-cdc-map")
                .name("Oracle-CDC-Source");

        // 快照模式需要处理快照完成控制信号
        if(clientInfo.getJobStartupMode() == JobStartupMode.SNAPSHOT){
            // 使用处理后的流作为主数据流，侧输出流来处理快照完成信号
            oracleStream = configureSnapshotControlSignalHandling(oracleStream);
        }

        // 使用侧输出流方式将数据分流到不同的 Kafka Topic
        toSinkWithSideOutput(oracleStream);



        jobClient = env.executeAsync();
//        flinkJobManager.registerCdcJobHandler(this);
        logger.info("✅ 实际的 job ID: {}", jobClient.getJobID());

        // 🔧 启动停止信号监听器（现在 jobClient 已经可用了）
        if (jobStopMonitor != null && clientInfo.getJobStartupMode() == JobStartupMode.SNAPSHOT) {
            try {
                jobStopMonitor.startMonitoring(jobClient);
                logger.info("📡 快照任务停止信号监听器已启动");
            } catch (Exception e) {
                logger.error("❌ 启动停止信号监听器失败: {}", e.getMessage(), e);
                // 继续执行，不因为监听器启动失败而中断整个作业
            }
        }

        // last.设置任务状态
        switch (clientInfo.getJobStartupMode()) {
            case NORMAL:
                notifyJobStatusChange(JobStatus.INCREMENTAL_RUNNING);
                break;
            case SNAPSHOT:
                notifyJobStatusChange(JobStatus.SNAPSHOT_RUNNING);
                break;
        }

        logger.info("🚀🚀🚀🚀🚀🚀 启动Flink作业...🚀🚀🚀🚀🚀🚀");
    }

    /**
     * 配置检查点
     * @param env Flink流执行环境
     * @throws IOException
     */
    public void configureCheckpointing(StreamExecutionEnvironment env) throws IOException {
        logger.info("⚙️ 配置检查点...");

        // TODO 检查点常用配置
        // 1、启用检查点: 默认是barrier对齐的，周期为 60 分钟, 精准一次
        env.enableCheckpointing(6 * 60 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);

        // 检查点的配置对象
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();

        // 2、指定检查点的存储位置
        env.setStateBackend(new HashMapStateBackend());env.getCheckpointConfig().setCheckpointStorage(stateBackendProperties.getCheckpointPath());

        // 3、checkpoint的超时时间: 默认 60 分钟，超过这个时间，认为当前checkpoint保存失败
        checkpointConfig.setCheckpointTimeout(3 * 60 * 60 * 1000L);

        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔，如果设置了 > 0，那么并发就会变成1，这里间隔10分钟
        checkpointConfig.setMinPauseBetweenCheckpoints(1 * 60 * 60 * 1000L);

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
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));
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


    /**
     * 配置快照完成控制信号处理
     * @param oracleStream Oracle CDC数据流
     * @return 处理后的数据流
     */
    private DataStream<String> configureSnapshotControlSignalHandling(DataStream<String> oracleStream) {
        logger.info("📸 快照模式 - 配置快照完成检测...");

        // 定义侧输出标签
        OutputTag<String> controlOutputTag = new OutputTag<String>("snapshot-control-stream") {};

        // 使用独立的ProcessFunction分离控制流和数据流
        SingleOutputStreamOperator<String> dataStream = oracleStream
                .process(new SnapshotSignalSplitProcessFunction(controlOutputTag))
                .uid("snapshot-signal-split")
                .name("Snapshot-Signal-Split");


        // 获取控制流（侧输出流）
        DataStream<String> controlStream = dataStream.getSideOutput(controlOutputTag);

        // 创建广播流
        BroadcastStream<String> broadcastStream = controlStream
                .broadcast(SnapshotControlProcessFunction.SNAPSHOT_COMPLETE_STATE_DESC);

        // 连接数据流和广播流
        BroadcastConnectedStream<String, String> connectedStream = dataStream
                .connect(broadcastStream);


        // 创建快照控制处理函数，当所有的表已经完成快照任务时，触发侧输出流
        SnapshotControlProcessFunction snapshotControlFunction =
                new SnapshotControlProcessFunction(new HashSet<>(Arrays.asList(getCurrentJobSyncTables())));

        // 处理连接的流
        SingleOutputStreamOperator<String> processedStream = connectedStream
                .process(snapshotControlFunction)
                .uid("snapshot-control-process")
                .name("Snapshot-Control-Process");



        // 获取侧输出流并处理停止信号，当有侧输出流时，表示快照同步任务已经完成，需要停止作业
        DataStream<String> stopSignalStream = processedStream
                .getSideOutput(SnapshotControlProcessFunction.JOB_STOP_SIGNAL_TAG);

        stopSignalStream
                .addSink(new ClusterAwareStopSignalSink(clientInfo))
                .uid("cluster-aware-stop-signal-sink")
                .name("Cluster-Aware-Stop-Signal-Sink");

        return processedStream;
    }




    /**
     * 使用侧输出流方式将数据分流到不同的 Kafka Topic
     */
    public void toSinkWithSideOutput(DataStream<String> oracleStream) {
        String clientId = clientInfo.getClientId();

        // 创建侧输出标签，key:全限定类名
        Map<String, List<OutputTag<String>>> outputTagMap = generateOutputTagMap();

        // 使用侧输出流分流数据
        SingleOutputStreamOperator<String> mainStream = oracleStream
                .process(new SideOutputProcessFunction(outputTagMap))
//                .uid(clientId + "-side-output-split")
                .name("SideOutput-Split");


        List<OutputTag<String>> outputTagList = outputTagMap.values().stream().flatMap(List::stream).collect(Collectors.toList());

        // 为每条分流创建对应的 Kafka Sink
        for (OutputTag<String> outputTag : outputTagList) {
            // 获取侧输出流
            DataStream<String> tableStream = mainStream.getSideOutput(outputTag);
            String topic = outputTag.getId();

            // 为每个侧输出流创建 Kafka Sink
            tableStream
                    .sinkTo(KafkaSink.<String>builder()
                            .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())
                            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                    .setTopic(topic)
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build())
                            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                            // 性能优化配置
                            .setProperty("batch.size", "262144") // 增加批大小到64KB
                            .setProperty("linger.ms", "100") // 增加等待时间
                            .setProperty("buffer.memory", "67108864") // 64MB缓冲区内存
                            .setProperty("compression.type", "lz4") // 启用LZ4压缩
                            // 无限重试配置
                            .setProperty("retries", String.valueOf(Integer.MAX_VALUE))
                            .setProperty("retry.backoff.ms", "1000")
                            .setProperty("reconnect.backoff.ms", "1000")
                            .setProperty("reconnect.backoff.max.ms", "10000")
                            .setProperty("request.timeout.ms", "30000")
                            .setProperty("delivery.timeout.ms", String.valueOf(Integer.MAX_VALUE))
                            .setProperty("acks", "1")
                            .build())
//                    .uid(clientId + "-sink-" + topic)
                    .name("KafkaSink-" + topic);
        }

    }

    // 创建侧输出标签，key:全限定类名
    private Map<String, List<OutputTag<String>>> generateOutputTagMap(){
        List<ClientTopicInfo> clientTopicInfoList = topicInfoRegistry.getClientTopicInfoList();
        // 侧输出标签映射
        Map<String, List<OutputTag<String>>> outputTagMap = new HashMap<>();


        for (ClientTopicInfo clientTopicInfo : clientTopicInfoList) {
            String fullyQualifiedTableName = clientTopicInfo.getFullyQualifiedTableName();
            List<OutputTag<String>> outputTags = outputTagMap.computeIfAbsent(fullyQualifiedTableName, k -> new ArrayList<>());
            String topic = clientTopicInfo.getTopic();
            outputTags.add(new OutputTag<>(topic, Types.STRING));

        }
        return outputTagMap;
    }
    // Getter和Setter方法





    public JobClient getJobClient() {
        return jobClient;
    }




    /**
     * 清理状态，释放资源
     */
    public void clearState() throws Exception {
        logger.info("🧹 清理OracleToKafka状态...");

        try {
            if (jobClient != null) {
                // 尝试取消作业
                jobClient.cancel();
                logger.info("🛑 作业已取消");
            }
        } catch (Exception e) {
            logger.warn("⚠️ 取消作业时出现异常: {}", e.getMessage());
        }

        // 🔧 停止作业监听器
        if (jobStopMonitor != null) {
            try {
                jobStopMonitor.stopMonitoring();
                logger.info("📡 作业停止监听器已关闭");
            } catch (Exception e) {
                logger.warn("⚠️ 关闭作业停止监听器时出现异常: {}", e.getMessage());
            }
        }

        // 重置状态
        jobClient = null;
        notifyJobStatusChange(JobStatus.UNKNOWN);
        jobStopMonitor = null;


        logger.info("✅ 状态清理完成");
    }



    // 获取需要同步的表名数组
    public String[] getCurrentJobSyncTables() {
        HashSet<String> tableSet = new HashSet<>();
        this.topicInfoRegistry.getClientTopicInfoList()
                .forEach(clientTopicInfo -> tableSet.add(clientTopicInfo.getFullyQualifiedTableName()));

        return tableSet.toArray(new String[0]);
    }



    // 快照完成后处理
    public void afterSnapshotCompletion() throws Exception {

        // 1.所有表都切换为 NORMAL 状态
        List<ExecutionPlan.TableExecutionPlan> snapShotTableExecutionPlanList = this.executionPlan.getTableExecutionPlans()
                .stream()
                .filter(tableExecutionPlan -> TableStatus.SNAPSHOT == (tableExecutionPlan.getTableStatus()))
                .collect(Collectors.toList());

        configFetcher.batchUpdateExecutionPlans(snapShotTableExecutionPlanList);

        // 2.更新job状态
        notifyJobStatusChange(JobStatus.MODE_SWITCHING);
    }









}