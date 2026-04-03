package com.bsoft.monitor.k2zk;

import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.MonitorServerInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.constants.MetricKeys;
import com.bsoft.common.core.CDCJob;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.MetricType;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.metric.MonitorMetricEvent;
import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class MonitorMetricJob extends CDCJob {
    private static final Logger logger = LogManager.getLogger(MonitorMetricJob.class);

    @Autowired
    private DatasourceConfigFetcher configFetcher;

    @Autowired
    private KafkaUtils kafkaUtils;

    @Autowired
    private StateBackendProperties stateBackendProperties;

    private String groupIdPrefix;

    // 监控端信息
    @Autowired
    protected MonitorServerInfo monitorServerInfo;


    // 类字段新增
    private final ConcurrentHashMap<String, Long> lastLagCache = new ConcurrentHashMap<>();




    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(this.monitorServerInfo);
    }

    public void startJob() throws Exception {
        // 1.从配置库读取各种配置
        prepareForMonitorExecution();
        // 2.根据配置库的内容，初始化当前执行所需的环境
        initMonitorExecutionEnvironment();

        pullMetricsFromKafkaAndWriteToZk();
    }

    private void prepareForMonitorExecution() throws Exception {
        // 1.从配置库中加载当前中心端的配置
        configFetcher.loadServerInfoConfig(this.monitorServerInfo);
        initGroupIdPrefix();
    }

    private void initMonitorExecutionEnvironment() {
        kafkaUtils.waitUntilTopicExists(this.monitorServerInfo.getMonitorMetricTopic());
        logger.info("sync-monitor initialized, metric topic={}", this.monitorServerInfo.getMonitorMetricTopic());
    }


    private void pullMetricsFromKafkaAndWriteToZk() throws Exception {
        // 生成固定的 job id
        String jobIdHex = JobIdUtil.generateFixedJobId("sync-monitor-" + serverInfo.getServerId());

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
        configuration.setInteger(RestOptions.PORT, this.monitorServerInfo.getFlinkUiPort());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 应用检查点
        configureCheckpointing(env);

        String groupId = buildGroupId(this.monitorServerInfo.getMonitorMetricTopic());

        // 创建kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())  // 指定kafka节点的地址和端口
                .setGroupId(groupId)                                         // 指定消费者组的id
                .setTopics(this.monitorServerInfo.getMonitorMetricTopic())                               // 指定消费的 Topic
                .setValueOnlyDeserializer(new SimpleStringSchema())          // 指定 反序列化器，这个是反序列化value
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // 从 kafka source 生成数据流
        DataStreamSource<String> kafkaDataStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source-" + this.monitorServerInfo.getMonitorMetricTopic());

        kafkaDataStream
                .map(MonitorMetricJsonParser::parse)   // 消息中的json，换装成MonitorMetricEvent
                .name("parse-monitor-metric")
                .filter(Objects::nonNull)
                .filter(event -> MetricType.OFFSET.equals(event.getMetricType()))
                .name("filter-offset-events")
                .addSink(new EventDrivenMonitorOffsetZkSink(this.monitorServerInfo))
                .name("offset-zk-sink-event-driven");

        env.execute("Kafka Metric To ZooKeeper");
    }


    @Scheduled(fixedRateString = "${monitor.lag.fixed-rate-ms:60000}")
    public void monitorKafkaTopics() throws Exception {

        for (ServerTopicInfo serverTopicInfo : topicInfoRegistry.getServerTopicInfoList()) {
            String topic = serverTopicInfo.getTopic();
            String groupId = buildGroupId(topic);
            long currentLag = kafkaUtils.getUnconsumedMessageCount(topic, groupId);

            String cacheKey = lagCacheKey(serverTopicInfo);
            Long previousLag = lastLagCache.put(cacheKey, currentLag);

            if (previousLag != null && previousLag == currentLag) {
                logger.debug("Lag unchanged, skip zk update. topic={}, targetDs={}, table={}, lag={}",
                        topic,
                        serverTopicInfo.getTargetDataSourceId(),
                        serverTopicInfo.getTargetTableName(),
                        currentLag);
                continue;
            }

            Map<String, String> metric = new HashMap<>();
            metric.put(MetricKeys.TOPIC, topic);
            metric.put(MetricKeys.TARGET_DATA_SOURCE_ID, serverTopicInfo.getTargetDataSourceId());
            metric.put(MetricKeys.TARGET_TABLE_NAME, serverTopicInfo.getTargetTableName());
            metric.put(MetricKeys.UNCONSUMED_MESSAGE_COUNT, String.valueOf(currentLag));

            globalZooKeeperManager.updateUnconsumedMessageCount(metric);
            globalZooKeeperManager.flushLagMetrics();

            logger.info("Lag changed, update zk. topic={}, targetDs={}, table={}, oldLag={}, newLag={}",
                    topic,
                    serverTopicInfo.getTargetDataSourceId(),
                    serverTopicInfo.getTargetTableName(),
                    previousLag,
                    currentLag);
        }
    }

    private void initGroupIdPrefix() {
        if (monitorServerInfo == null || monitorServerInfo.getMonitorServerId() == null) {
            throw new IllegalStateException("monitorServer or monitorServerId is null, cannot init groupIdPrefix");
        }
        this.groupIdPrefix = "flink-k2zk-" + monitorServerInfo.getMonitorServerId() + "-";
    }

    private String buildGroupId(String kafkaTopic) {
        if (groupIdPrefix == null) {
            throw new IllegalStateException("groupIdPrefix is not initialized");
        }
        if (kafkaTopic == null || kafkaTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("kafkaTopic is blank");
        }
        return groupIdPrefix + kafkaTopic;
    }

    public void configureNetworkBufferPool(Configuration configuration) throws IOException {
        configuration.setString("taskmanager.memory.process.size", "2048m");
        configuration.setString("taskmanager.memory.network.min", "256mb");
        configuration.setString("taskmanager.memory.network.max", "256mb");
    }

    public void configureCheckpointing(StreamExecutionEnvironment env) throws IOException {
        env.enableCheckpointing(3 * 60 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(stateBackendProperties.getCheckpointPath());

        checkpointConfig.setCheckpointTimeout(1 * 60 * 60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setMinPauseBetweenCheckpoints(5 * 60 * 1000L);
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(100);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(10));
    }

    public static String findLatestCheckpointPathWithMetadata(String checkpointBasePath, String jobIdHex) throws Exception {
        String localBasePath = new Path(checkpointBasePath).toUri().getPath();
        File jobDir = new File(localBasePath, jobIdHex);
        if (!jobDir.exists() || !jobDir.isDirectory()) {
            return null;
        }

        File[] chkDirs = jobDir.listFiles((dir, name) -> name.startsWith("chk-"));
        if (chkDirs == null || chkDirs.length == 0) {
            return null;
        }
        Arrays.sort(chkDirs, Comparator.comparing(File::getName).reversed());
        for (File chkDir : chkDirs) {
            File metadata = new File(chkDir, "_metadata");
            if (metadata.exists() && metadata.isFile()) {
                return metadata.getParentFile().toURI().toString();
            }
        }
        return null;
    }

    private String lagCacheKey(ServerTopicInfo info) {
        return info.getTopic() + "|" + info.getTargetDataSourceId() + "|" + info.getTargetTableName();
    }
}
