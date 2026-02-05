package com.bsoft.log.k2db;


import com.bsoft.common.bean.*;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.WindowTimeUnit;
import com.bsoft.common.global.GlobalLogCollectorManager;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;

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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;


// 注册为 Spring 管理的组件
@Component
@EnableScheduling
public class LogPersistenceJob extends CDCJob {


    private static final Logger logger = LogManager.getLogger(LogPersistenceJob.class);




    // kafka工具类
    @Autowired
    private KafkaUtils kafkaUtils;


    // 检查点相关
    @Autowired
    private StateBackendProperties stateBackendProperties;


    private final String serverLogTopic = "sync_server_logs";

//    @Autowired
//    private ClientHTTPClient clientHTTPClient;


    public void init() throws Exception {
//        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(this.serverInfo);
//        this.serverlogTopic = "server_logs_" + this.serverInfo.getServerId();
    }

    public void startJob() {
        try {

            // 4.根据配置库的内容，初始化当前执行所需的环境
            initLogServerExecutionEnvironment();

            // 5.是从 kafka 读取数据，写入目标库，可同时写入多个目标库
            pullFromKafkaAndPublishToDB();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }



    // 根据配置库的内容，初始化当前执行所需的环境
    public void initLogServerExecutionEnvironment() throws Exception {

        kafkaUtils.waitUntilTopicExists(this.serverLogTopic);

        // 6. 设置一个启动时间
        this.logServerInfo.notifyClientStart();

        logger.info("当前 job 启动,启动时间: {}", this.logServerInfo.printStartTime());
    }


    public void pullFromKafkaAndPublishToDB() throws Exception {
        // 获取客户端id，为了生成固定的job id
        String serverId = this.logServerInfo.getLogServerId();
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





            // 稳定 groupId：serverId + topic，便于断点续读
            String groupId = "flink-k2db-" + serverInfo.getServerId() + "-" + this.serverLogTopic;

            // TODO 从Kafka读(消费者角色)： 新Source架构
            KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())   // 指定kafka节点的地址和端口
                    .setGroupId(groupId)  // 指定消费者组的id
                    .setTopics(this.serverLogTopic)                        // 指定消费的 Topic
                    .setDeserializer(new KafkaMessageDeserializer())    // 指定 反序列化器，这个是反序列化value
                    .setStartingOffsets(OffsetsInitializer.earliest())                                 // flink消费kafka的策略
                    .build();


            // 创建数据流
            DataStreamSource<String> kafkaDataStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "kafka-source-" + this.serverLogTopic);

            // 解析Kafka消息并处理数据库写入，获得操作结果
            SingleOutputStreamOperator<Void> operationResults = kafkaDataStream
                    .map(KafkaMessageConverter::parseKafkaMessage)
                    .name("parse-kafka-message-" + this.serverLogTopic)
                    .filter(kafkaPayload -> {
                        boolean isValid = kafkaPayload != null;
                        if (!isValid) {
                            logger.debug("🚫 过滤掉无效的Kafka消息");
                        }
                        return isValid;
                    })
                    .name("filter-" + this.serverLogTopic).uid("filter-" + this.serverLogTopic)
                    .process(new DatabaseBatchWriteFunction(this.logServerInfo)) // 使用ProcessFunction处理写入
                    .name("target-writer-" + this.serverLogTopic).uid("target-writer-" + this.serverLogTopic);




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



}
