package com.bsoft.log.k2db;


import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.LogInfo;
import com.bsoft.common.core.CDCJob;

import com.bsoft.common.util.DatasourceConfigFetcher;
import com.bsoft.common.util.JobIdUtil;
import com.bsoft.common.util.KafkaUtils;


import com.bsoft.log.global.GlobalLogCollectorManager;
import com.bsoft.log.offset.JdbcLogTopicOffsetRepository;
import com.bsoft.log.offset.LogTopicOffsetRepository;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.stereotype.Component;


import java.io.IOException;

import java.util.*;



// 注册为 Spring 管理的组件
@Component
@EnableScheduling
public class LogPersistenceJob extends CDCJob {


    private static final Logger logger = LogManager.getLogger(LogPersistenceJob.class);


    @Autowired
    private LogInfo logInfo;


    // 配置库读取工具
    @Autowired
    private DatasourceConfigFetcher configFetcher;

    // kafka工具类
    @Autowired
    private KafkaUtils kafkaUtils;




    // topic offset 仓储，负责配置库中的位点读写
    private LogTopicOffsetRepository logTopicOffsetRepository;




    public void init() throws Exception {
//        // 初始化 server 进程级 zk 服务
//        this.serverProcessZkService = ServerProcessZkService.open(this.serverInfo);

        // 初始化 topic offset 仓储，复用配置库 DataSource
        this.logTopicOffsetRepository = new JdbcLogTopicOffsetRepository(
                this.configFetcher.getConfigDataSource()

        );
    }

    public void startJob() {
        try {

            // 1.从配置库读取各种配置
            prepareForJobExecution();

            // 2.根据配置库的内容，初始化当前执行所需的环境
            initLogServerExecutionEnvironment();

            // 3.是从 kafka 读取数据，写入目标库，可同时写入多个目标库
            pullFromKafkaAndPublishToDB();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }




    /**
     * 从配置库读取 sync-log 运行所需配置。
     */
    public void prepareForJobExecution() throws Exception {
        // 从 CDC_SERVER_CONFIG 读取日志 topic 配置。
        initLogTopicFromConfig();

        // 从 CDC_SERVER_CONFIG 读取原始消息存储策略配置。
        initRawMessageStorageStrategyFromConfig();

        // 校验 logId 不能为空。
        validateLogInfo();
    }

    /**
     * 从 CDC_SERVER_CONFIG 初始化日志 topic。
     */
    private void initLogTopicFromConfig() throws Exception {
        // 读取服务端写入日志使用的 Kafka topic。
        String logTopic = configFetcher.loadServerConfigValue("server-log-topic-name");

        // 配置库中存在有效日志 topic 时，覆盖 application.yml 中的默认值。
        if (logTopic != null && !logTopic.trim().isEmpty()) {
            // 去除首尾空白后写入 LogInfo。
            this.logInfo.setLogTopic(logTopic.trim());
        }
    }

    /**
     * 从 CDC_SERVER_CONFIG 初始化原始消息存储策略。
     */
    private void initRawMessageStorageStrategyFromConfig() throws Exception {
        // 读取 CDC_LOG_CORRECT 表中 raw_message 字段的存储策略。
        String rawMessageStorageStrategyValue =
                configFetcher.loadServerConfigValue("raw-message-storage-strategy");

        // 配置库未配置时保留 LogInfo 中的默认值。
        if (rawMessageStorageStrategyValue == null || rawMessageStorageStrategyValue.trim().isEmpty()) {
            return;
        }

        // 标准化配置值，兼容数据库中小写或混合大小写。
        String normalizedValue = rawMessageStorageStrategyValue.trim().toUpperCase();

        try {
            // 将配置值转换为枚举。
            RawMessageStorageStrategy strategy = RawMessageStorageStrategy.valueOf(normalizedValue);

            // 写入 LogInfo，后续创建 LogCollector 时使用该策略。
            this.logInfo.setRawMessageStorageStrategy(strategy);
        } catch (IllegalArgumentException e) {
            // 配置值非法时直接启动失败，避免 raw_message 存储行为不明确。
            throw new IllegalStateException(
                    "invalid raw-message-storage-strategy: " + rawMessageStorageStrategyValue
                            + ", supported values: NONE, PLAIN, COMPRESS",
                    e
            );
        }
    }

    /**
     * 校验 sync-log 基础配置。
     */
    private void validateLogInfo() {
        // 校验 logId 不能为空，因为它是 CDC_LOG_TOPIC_OFFSET 的主键维度。
        if (this.logInfo.getLogId() == null || this.logInfo.getLogId().trim().isEmpty()) {
            throw new IllegalStateException("logInfo.logId is blank");
        }

        // 校验日志 topic 不能为空，因为它是 KafkaSource 的消费目标。
        if (this.logInfo.getLogTopic() == null || this.logInfo.getLogTopic().trim().isEmpty()) {
            throw new IllegalStateException("logInfo.logTopic is blank");
        }

        // 校验 rawMessageStorageStrategy 不能为空，避免 LogCollector 处理 raw_message 时出现空策略。
        if (this.logInfo.getRawMessageStorageStrategy() == null) {
            throw new IllegalStateException("logInfo.rawMessageStorageStrategy is null");
        }
    }



    // 根据配置库的内容，初始化当前执行所需的环境
    public void initLogServerExecutionEnvironment() throws Exception {

        kafkaUtils.waitUntilTopicExists(this.logInfo.getLogTopic());

        // 6. 设置一个启动时间
        this.logInfo.notifyClientStart();

        logger.info("当前 job 启动,启动时间: {}", this.logInfo.printStartTime());
    }


    public void pullFromKafkaAndPublishToDB() throws Exception {
        // 获取 log id，为了生成固定的job id
        String logId = this.logInfo.getLogId();

        // 获取日志 topic。
        String logTopic = this.logInfo.getLogTopic();

        // 生成固定的 job id
        String jobIdHex = JobIdUtil.generateFixedJobId(logId);

        // 创建 执行环境 的 Configuration
        Configuration configuration = new Configuration();
        configuration.setString("$internal.pipeline.job-id", jobIdHex);
        configuration.setString("parallelism.default", "1");

        configureNetworkBufferPool(configuration);



        // 为避免默认 8081 被占用，使用配置文件中指定的端口
        configuration.setInteger(RestOptions.PORT, this.logInfo.getFlinkUiPort());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);


        // 从 CDC_LOG_TOPIC_OFFSET 读取日志 topic 的 nextOffset。
        Map<String, Long> persistedOffsets = loadPersistedLogTopicOffsets(logId, logTopic);

        // 根据 Kafka 当前 earliest offset 修正初始或已过期的持久化 offset
        Map<String, Long> correctedOffsets = correctPersistedOffsetsByKafkaEarliest(logId, logTopic, persistedOffsets);

        // 使用修正后的 offset 构造 KafkaSource 起始位置
        OffsetsInitializer startingOffsetsInitializer = buildStartingOffsetsInitializer(logTopic, correctedOffsets);






            // 稳定 groupId：serverId + topic，便于断点续读
            String groupId = "flink-k2db-" + this.logInfo.getLogId();

            // TODO 从Kafka读(消费者角色)： 新Source架构
            KafkaSource<LogKafkaMessage> kafkaSource = KafkaSource.<LogKafkaMessage>builder()
                    .setBootstrapServers(kafkaUtils.getKafkaBootstrapServers())   // 指定kafka节点的地址和端口
                    .setGroupId(groupId)                                          // 指定消费者组的id
                    .setTopics(logTopic)                                          // 指定消费的 Topic
                    .setDeserializer(new KafkaMessageDeserializer())     // 指定 反序列化器，这个是反序列化value
                    .setStartingOffsets(startingOffsetsInitializer)   // 设置从 CDC_LOG_TOPIC_OFFSET 读取到的启动 offset。
                    .build();                                            // 构建 KafkaSource。


            // 创建数据流
            DataStreamSource<LogKafkaMessage> kafkaDataStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "kafka-source-" + logTopic);

            // 解析Kafka消息并处理数据库写入，获得操作结果
            SingleOutputStreamOperator<Void> operationResults = kafkaDataStream

                    .name("parse-kafka-message-" + this.logInfo.getLogTopic())
                    .filter(kafkaPayload -> {
                        boolean isValid = kafkaPayload != null;
                        if (!isValid) {
                            logger.debug("🚫 过滤掉无效的Kafka消息");
                        }
                        return isValid;
                    })
                    .name("filter-" + logTopic)
                    // 设置过滤算子 uid。
                    .uid("filter-" + logTopic)
                    .process(new DatabaseBatchWriteFunction(this.logInfo)) // 使用ProcessFunction处理写入
                    // 设置写入算子名称。
                    .name("target-writer-" + logTopic)
                    // 设置写入算子 uid。
                    .uid("target-writer-" + logTopic);


        // 注册 JVM 关闭钩子，关闭异步日志写入器。
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // 打印关闭日志。
            logger.info("sync-log shutting down, flushing log collector");

            // 关闭所有 LogCollector。
            GlobalLogCollectorManager.shutdownAll();
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


    /**
     * 从 CDC_LOG_TOPIC_OFFSET 读取当前日志 topic 的 nextOffset。
     */
    private Map<String, Long> loadPersistedLogTopicOffsets(String logId, String logTopic) throws Exception {
        // 调用仓储读取 offset；如果缺失，仓储负责初始化 nextOffset=0。
        Map<String, Long> persistedOffsets = logTopicOffsetRepository.loadNextOffsets(
                logId,
                Collections.singletonList(logTopic)
        );

        // 防御仓储返回 null。
        if (persistedOffsets == null) {
            // 使用空 Map 避免后续空指针。
            persistedOffsets = Collections.emptyMap();
        }

        // 打印读取到的 offset 信息。
        logger.info("从CDC_LOG_TOPIC_OFFSET表，加载日志的偏移量, logId={}, topic={}, topicCount={}",
                logId, logTopic, persistedOffsets.size());

        // 返回 topic -> nextOffset。
        return persistedOffsets;
    }

    /**
     * 根据 CDC_LOG_TOPIC_OFFSET 中的 nextOffset 构造 KafkaSource 起始位点。
     */
    private OffsetsInitializer buildStartingOffsetsInitializer(String logTopic,
                                                               Map<String, Long> persistedOffsets) {
        // 标准化 topic。
        String normalizedTopic = normalizeText(logTopic);

        // 校验 topic 不能为空。
        if (normalizedTopic.isEmpty()) {
            // topic 为空时无法构造 KafkaSource 起始位点。
            throw new IllegalArgumentException("logTopic is blank");
        }

        // 防御 offset map 为空。
        Map<String, Long> safePersistedOffsets = persistedOffsets == null ? Collections.emptyMap() : persistedOffsets;

        // 读取当前 topic 的 nextOffset。
        Long nextOffset = safePersistedOffsets.get(normalizedTopic);

        // 如果没有 offset 或 offset 为 0，则从 topic 头开始读取。
        if (nextOffset == null || nextOffset == 0L) {
            // 记录从头消费。
            logger.info("sync-log starts from topic head, topic={}, nextOffset={}", normalizedTopic, nextOffset);

            // 从 Kafka topic 最早可用位置开始读取。
            return OffsetsInitializer.earliest();
        }

        // 校验 offset 不能为负数。
        if (nextOffset < 0L) {
            // 持久化 offset 异常时直接失败，避免错误消费。
            throw new IllegalStateException("persisted nextOffset cannot be negative, topic=" + normalizedTopic);
        }

        // 当前日志 topic 有且仅有一个分区，固定使用 0 号分区。
        TopicPartition topicPartition = new TopicPartition(normalizedTopic, 0);

        // 构造 partition -> offset 映射。
        Map<TopicPartition, Long> startingOffsets = new LinkedHashMap<>();

        // 设置 0 号分区的启动 offset。
        startingOffsets.put(topicPartition, nextOffset);

        // 打印使用持久化 offset 启动。
        logger.info("sync-log starts from persisted offset, topic={}, partition=0, nextOffset={}",
                normalizedTopic, nextOffset);

        // 使用指定 offset 启动；异常缺失分区时回退到 earliest。
        return OffsetsInitializer.offsets(startingOffsets, OffsetResetStrategy.EARLIEST);
    }


    /**
     * 根据 Kafka 当前 earliest offset 修正持久化 offset。
     */
    private Map<String, Long> correctPersistedOffsetsByKafkaEarliest(String logId,
                                                                     String logTopic,
                                                                     Map<String, Long> persistedOffsets) throws Exception {
        // 标准化 topic
        String normalizedTopic = normalizeText(logTopic);

        // topic 不能为空
        if (normalizedTopic.isEmpty()) {
            throw new IllegalArgumentException("logTopic is blank");
        }

        // 复制 offset map，避免修改调用方对象
        Map<String, Long> safeOffsets =
                persistedOffsets == null ? new LinkedHashMap<>() : new LinkedHashMap<>(persistedOffsets);

        // 读取配置库中的 nextOffset，缺失时按 0 处理
        long persistedNextOffset = safeOffsets.getOrDefault(normalizedTopic, 0L);

        // 查询 Kafka 当前仍保留的最早 offset
        long kafkaEarliestOffset = kafkaUtils.fetchEarliestOffsetOfSinglePartitionTopic(normalizedTopic);

        // 持久化 offset 不能为负数
        if (persistedNextOffset < 0L) {
            throw new IllegalStateException("persisted nextOffset cannot be negative, topic=" + normalizedTopic);
        }


        // 已持久化 offset 小于 Kafka 当前最早 offset，
        if (persistedNextOffset < kafkaEarliestOffset) {
            logger.error("sync-log:{}的持久化offset: {} 不可用，部分日志已无法消费，将跳过过期区间并从Kafka当前最早offset: {} 启动,丢失offset范围={}-{}",
                        logId,persistedNextOffset,  kafkaEarliestOffset, persistedNextOffset, kafkaEarliestOffset - 1);



            // 更新 CDC_LOG_TOPIC_OFFSET.NEXT_OFFSET
            logTopicOffsetRepository.updateNextOffsets(
                    logId,
                    Collections.singletonMap(logTopic, kafkaEarliestOffset)
            );

            // 记录配置库 offset 修正结果
            logger.warn("sync-log 已修正 CDC_LOG_TOPIC_OFFSET。logId={}，topic={}，correctedNextOffset={}",
                           logId, logTopic, kafkaEarliestOffset);
            // 更新内存中的 offset map
            safeOffsets.put(normalizedTopic, kafkaEarliestOffset);

            // 返回修正后的 offset
            return safeOffsets;
        }

        // 持久化 offset 仍然可用，直接使用配置库 offset
        logger.info("sync-log 使用配置库中的日志 topic offset 启动。logId={}，topic={}，persistedNextOffset={}，kafkaEarliestOffset={}",
                logId, normalizedTopic, persistedNextOffset, kafkaEarliestOffset);

        // 返回原始 offset
        return safeOffsets;
    }



}
