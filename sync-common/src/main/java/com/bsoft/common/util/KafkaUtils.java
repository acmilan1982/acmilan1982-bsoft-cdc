package com.bsoft.common.util;


import com.bsoft.common.bean.ClientTopicInfo;
import com.bsoft.common.bean.OracleStartupOffset;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
/**
 * ClassName: KafkaUtils
 * Package:   com.bsoft.o2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/5/6 - 14:01
 */
@Component
public class KafkaUtils {

    private static final Logger logger = LogManager.getLogger(KafkaUtils.class);

    private  String KAFKA_BOOTSTRAP_SERVERS;

    private AdminClient adminClient;



    @PostConstruct
    public void init() {
        try {
            Properties props = new Properties();
            logger.info("KAFKA_BOOTSTRAP_SERVERS --> {}",KAFKA_BOOTSTRAP_SERVERS);
            props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
            adminClient = AdminClient.create(props);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    // 检查topic是否存在，如不存在，则创建
    public  void createKafkaTopicsIfNotExists(List<ClientTopicInfo> clientTopicInfoList) {
        logger.info("按需创建 kafka 主题");
        List<String> topics = clientTopicInfoList.stream()
                .map(ClientTopicInfo::getTopic)
                .collect(Collectors.toList());
        createKafkaTopicsIfNotExists(topics);
    }

    public  void createKafkaTopicsIfNotExists(Collection<String> topics) {
        createKafkaTopicsIfNotExistsInternal(topics);
    }


    // 按需创建 Kafka topics，并容忍并发或历史残留导致的 TopicExistsException。
    private void createKafkaTopicsIfNotExistsInternal(Collection<String> topics) {
        // 规范化 topic 名称，避免空值和重复值参与 Kafka Admin 操作。
        List<String> topicNames = normalizeTopicNames(topics);

        // 没有有效 topic 时直接返回。
        if (topicNames.isEmpty()) {
            return;
        }

        // 创建 Kafka AdminClient 配置。
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        // 使用局部 AdminClient 执行本次创建操作。
        try (AdminClient adminClient = AdminClient.create(props)) {
            // 逐个创建 topic，避免单个 topic 异常导致整批失败。
            for (String topic : topicNames) {
                createTopicIfNotExists(adminClient, topic);
            }
        } catch (Exception e) {
            // 将 Kafka Admin 异常统一包装为运行时异常。
            throw new RuntimeException("Kafka Topic 创建失败", e);
        }
    }

    // 创建 topic，如果 topic 已经存在则视为成功。
    private void createTopicIfNotExists(AdminClient adminClient, String topic) throws Exception {
        // 如果 topic 已经可描述，则不需要创建。
        if (topicExists(adminClient, topic)) {
            logger.info("Topic 已存在，跳过创建: {}", topic);
            return;
        }

        try {
            // 当前系统约定每个 topic 只有一个分区和一个副本。
            logger.info("Topic 不存在，自动创建: {}", topic);
            adminClient.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            // 普通按需创建场景下，TopicExistsException 可以视为已成功创建。
            if (e.getCause() instanceof TopicExistsException) {
                logger.info("Topic 创建时发现已存在，视为创建成功: {}", topic);
                return;
            }

            // 其它异常继续抛出。
            throw e;
        }

        // 创建后等待 topic 可描述。
        waitUntilTopicsDescribable(adminClient, Collections.singletonList(topic), 30_000L);
    }


    /**
     * 等待指定 topics 全部可描述。
     */
    private void waitUntilTopicsDescribable(AdminClient adminClient,
                                            Collection<String> topics,
                                            long timeoutMillis) throws Exception {
        // 计算等待截止时间。
        long deadline = System.currentTimeMillis() + timeoutMillis;

        // 固定待检查 topic 集合，避免外部集合变化影响判断。
        List<String> expectedTopics = normalizeTopicNames(topics);

        // 在超时时间内轮询 topic 描述信息。
        while (System.currentTimeMillis() < deadline) {
            boolean allDescribable = true;

            // 逐个确认 topic 是否可描述。
            for (String topic : expectedTopics) {
                if (!topicExists(adminClient, topic)) {
                    allDescribable = false;
                    break;
                }
            }

            // 全部可描述后即可返回。
            if (allDescribable) {
                return;
            }

            // 元数据传播需要时间，因此短暂等待后继续检查。
            Thread.sleep(1000L);
        }

        // 超时仍不可描述时抛出异常。
        throw new TimeoutException("Timed out waiting Kafka topics describable: " + expectedTopics);
    }


    // SNAPSHOT 重跑前,删除并重建 Kafka topics
    public void recreateKafkaTopicsForSnapshot(List<ClientTopicInfo> clientTopicInfoList,
                                               long timeoutMinutes,
                                               long retryIntervalSeconds) {
        // 空 topic 信息不需要处理。
        if (clientTopicInfoList == null || clientTopicInfoList.isEmpty()) {
            return;
        }

        // 从 ClientTopicInfo 中提取 topic 名称。
        List<String> topics = clientTopicInfoList.stream()
                .map(ClientTopicInfo::getTopic)
                .collect(Collectors.toList());

        // 复用字符串集合版本的重建逻辑。
        recreateKafkaTopicsForSnapshot(topics, timeoutMinutes, retryIntervalSeconds);
    }

    /**
     * SNAPSHOT 重跑前删除并重建 Kafka topics。
     */
    public void recreateKafkaTopicsForSnapshot(Collection<String> topics,
                                               long timeoutMinutes,
                                               long retryIntervalSeconds) {
        // 规范化 topic 名称，避免空值和重复值参与 Kafka Admin 操作。
        List<String> topicNames = normalizeTopicNames(topics);

        // 没有有效 topic 时直接返回。
        if (topicNames.isEmpty()) {
            return;
        }

        // 计算重建流程的整体超时时间。
        long timeoutMillis = TimeUnit.MINUTES.toMillis(Math.max(timeoutMinutes, 1L));

        // 计算 TopicExistsException 后的重试间隔。
        long retryIntervalMillis = TimeUnit.SECONDS.toMillis(Math.max(retryIntervalSeconds, 1L));

        // 创建 Kafka AdminClient 配置。
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        // 使用局部 AdminClient 完成删除和重建的完整流程。
        try (AdminClient adminClient = AdminClient.create(props)) {
            // 查询当前已经存在的 topic。
            Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);

            // 只删除当前确实存在的 topic。
            List<String> topicsToDelete = topicNames.stream()
                    .filter(existingTopics::contains)
                    .collect(Collectors.toList());

            // 删除已经存在的旧 topic。
            if (!topicsToDelete.isEmpty()) {
                for (String topic : topicsToDelete) {
                    logger.warn("重新进入快照模式前，删除 Kafka topic: {}", topic);
                }

                // deleteTopics 只是提交删除请求，后续仍需在创建阶段处理异步删除窗口。
                adminClient.deleteTopics(topicsToDelete).all().get(60, TimeUnit.SECONDS);
            } else {
                // 没有旧 topic 时直接进入创建流程。
                logger.info("快照模式重建 topic 时未发现旧 topic: {}", topicNames);
            }

            // 已提交删除请求的 topic，创建时遇到 TopicExistsException 必须重试。
            Set<String> deletedTopicSet = new LinkedHashSet<>(topicsToDelete);

            // 逐个重建 topic，并以创建成功作为最终完成信号。
            for (String topic : topicNames) {
                if (deletedTopicSet.contains(topic)) {
                    createTopicAfterDeleteWithRetry(adminClient, topic, timeoutMillis, retryIntervalMillis);
                } else {
                    createTopicIfNotExists(adminClient, topic);
                }
            }

            // 全部 topic 可描述后，才认为重建完成。
            waitUntilTopicsDescribable(adminClient, topicNames, 60_000L);

            // 记录 topic 重建完成。
            logger.warn("快照模式 Kafka topics 删除并重建完成, topicCount={}", topicNames.size());
        } catch (Exception e) {
            // 将 Kafka Admin 异常统一包装为运行时异常。
            throw new RuntimeException("Kafka Topic 删除并重建失败", e);
        }
    }



    /**
     * 在删除后重建 topic，遇到 TopicExistsException 时等待并重试。
     */
    private void createTopicAfterDeleteWithRetry(AdminClient adminClient,
                                                 String topic,
                                                 long timeoutMillis,
                                                 long retryIntervalMillis) throws Exception {
        // 计算重试截止时间。
        long deadline = System.currentTimeMillis() + timeoutMillis;

        // 在超时时间内持续尝试创建 topic。
        while (System.currentTimeMillis() < deadline) {
            try {
                // 当前系统约定每个 topic 只有一个分区和一个副本。
                logger.info("删除后重建 Kafka topic: {}", topic);
                adminClient.createTopics(Collections.singletonList(new NewTopic(topic, 1, (short) 1)))
                        .all()
                        .get(30, TimeUnit.SECONDS);

                // 创建成功后等待 topic 可描述。
                waitUntilTopicsDescribable(adminClient, Collections.singletonList(topic), 30_000L);

                // 当前 topic 重建成功后返回。
                logger.warn("Kafka topic 删除后重建完成: {}", topic);
                return;
            } catch (ExecutionException e) {
                // TopicExistsException 表示 Kafka 仍处在删除释放窗口。
                if (e.getCause() instanceof TopicExistsException) {
                    logger.warn("Kafka topic 仍被判定为已存在，等待后重试创建: {}", topic);
                    Thread.sleep(retryIntervalMillis);
                    continue;
                }

                // 其它异常继续抛出。
                throw e;
            }
        }

        // 超时仍无法创建时抛出异常。
        throw new TimeoutException("Timed out recreating Kafka topic after delete: " + topic);
    }

    /**
     * 判断 topic 是否已经存在且可描述。
     */
    private boolean topicExists(AdminClient adminClient, String topic) throws Exception {
        try {
            // describeTopics 成功表示 topic 已经存在且可访问。
            adminClient.describeTopics(Collections.singletonList(topic))
                    .all()
                    .get(10, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            // UnknownTopicOrPartitionException 表示 topic 不存在。
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }

            // 其它异常继续抛出。
            throw e;
        }
    }



    /**
     * 规范化 topic 名称集合。
     */
    private List<String> normalizeTopicNames(Collection<String> topics) {
        // 空集合直接返回空列表。
        if (topics == null || topics.isEmpty()) {
            return Collections.emptyList();
        }

        // 去除空值、空白 topic 和重复 topic。
        return topics.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }
    /**
     * 等待指定 topics 在 Kafka 元数据中可见。
     */
    private void waitUntilTopicsExist(AdminClient adminClient,
                                      Collection<String> topics,
                                      long timeoutMillis) throws Exception {
        // 计算等待截止时间。
        long deadline = System.currentTimeMillis() + timeoutMillis;

        // 固定待创建 topic 集合，避免外部集合变化影响判断。
        Set<String> expectedTopics = new LinkedHashSet<>(topics);

        // 在超时时间内轮询 Kafka topic 元数据。
        while (System.currentTimeMillis() < deadline) {
            // 查询当前 Kafka 中已经存在的 topic。
            Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);

            // 全部 topic 可见后即可返回。
            if (existingTopics.containsAll(expectedTopics)) {
                return;
            }

            // 创建 topic 的元数据传播需要时间，因此短暂等待后继续检查。
            Thread.sleep(1000L);
        }

        // 超时仍未创建完成时抛出异常。
        throw new TimeoutException("Timed out waiting Kafka topics created: " + expectedTopics);
    }
    // 检查指定主题是否存在，不存在则等待
    public void waitUntilTopicExists(List<String> topics) {
        logger.info("waitUntilTopicExists --> {}",topics.toString());
        topics.stream().forEach(topic -> this.waitUntilTopicExists(topic));

    }


    // 检查指定主题是否存在，不存在则等待
    public void waitUntilTopicExists(String topicName) {
        logger.info("开始检查 Topic 是否存在:  --> {}",topicName);
        while (true) {
            try {
                Set<String> topics = adminClient.listTopics(new ListTopicsOptions().listInternal(true))
                        .names().get();
                if (topics.contains(topicName)) {
                    logger.info("发现 Topic: {} 已存在，退出检查。 ",topicName);
                    break;
                } else {
                    logger.info(" Topic: {} 不存在，等待 5 秒后重试... ",topicName);
                    Thread.sleep(5000);
                    System.out.println(".................................");
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("查询 topic 失败", e);
            }
        }
    }



    @Value("${kafka.bootstrap.servers}")
    public void setKafkaBootstrapServers(String servers) {
        this.KAFKA_BOOTSTRAP_SERVERS = servers;
    }

    public  String getKafkaBootstrapServers() {
        return  this.KAFKA_BOOTSTRAP_SERVERS;
    }




    /**
     * 获取指定主题当前已消费的最新offset
     * @param topicName 主题名称
     * @param groupId 消费者组ID
     * @return 各分区的已消费offset
     */
    public Map<Integer, Long> getCurrentConsumedOffset(String topicName, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        String monitorGroupId = "monitor-group-" + topicName + "-" + System.currentTimeMillis();

        props.put("group.id", groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topicName)
                    .stream()
                    .map(p -> new TopicPartition(topicName, p.partition()))
                    .collect(Collectors.toList());

            Map<Integer, Long> result = new HashMap<>();
            for (TopicPartition partition : partitions) {
                long position = consumer.committed(partition) != null ?
                        consumer.committed(partition).offset() : 0;
                result.put(partition.partition(), position);
            }

            return result;
        }
    }

//    /**
//     * 获取指定主题尚未被消费的消息总数
//     * @param topicName 主题名称
//     * @return 未消费的消息总数
//     */
//    public Long getUnconsumedMessageCount(String topicName) {
//        // 先检查主题是否存在
//        try {
//            Set<String> existingTopics = adminClient.listTopics().names().get();
//            if (!existingTopics.contains(topicName)) {
////                logger.warn("主题 {} 不存在", topicName);
//                return 0L;
//            }
//        } catch (Exception e) {
//            logger.error("检查主题是否存在时出错", e);
//            return 0L;
//        }
//
//        Properties props = new Properties();
//        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
//        String monitorGroupId = "monitor-group-" + topicName + "-" + System.currentTimeMillis();
//
//        props.put("group.id", monitorGroupId);
//
//        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
//            List<TopicPartition> partitions = consumer.partitionsFor(topicName)
//                    .stream()
//                    .map(p -> new TopicPartition(topicName, p.partition()))
//                    .collect(Collectors.toList());
//
//            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
//
//            long totalUnconsumed = 0;
//            for (TopicPartition partition : partitions) {
//                long committed = consumer.committed(partition) != null ?
//                        consumer.committed(partition).offset() : 0;
//                long latest = endOffsets.get(partition);
//                totalUnconsumed += (latest - committed);
//            }
//
//            return totalUnconsumed;
//        }
//    }


    /**
     * 计算指定 topic 在指定消费组下的“未消费消息数（lag）”。
     *
     * 计算公式（按分区求和）：
     *   totalLag = Σ( logEndOffset(partition) - committedOffset(partition) )
     *
     * 说明：
     * 1) 这是监控意义上的近似值（通常足够快、足够稳定）。
     * 2) 这里使用 committed offset，而不是消费者本地 position。
     * 3) 若某分区没有 committed offset，按 0 处理（与你当前代码语义一致，会偏保守）。
     * 4) 若出现 latest < committed（例如日志截断/位点异常），按 0 计，避免负数。
     */
    public Long getUnconsumedMessageCount(String topicName, String groupId) {
        // 参数防御：空 topic 或空 group 直接返回 0，避免无意义请求
        if (topicName == null || topicName.trim().isEmpty()
                || groupId == null || groupId.trim().isEmpty()) {
            return 0L;
        }

        try {
            // Step 1: 校验 topic 是否存在，并拿到分区列表
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.contains(topicName)) {
                return 0L;
            }

            TopicDescription topicDescription = adminClient
                    .describeTopics(Arrays.asList(topicName))
                    .allTopicNames()
                    .get()
                    .get(topicName);

            List<TopicPartition> partitions = topicDescription.partitions()
                    .stream()
                    .map(p -> new TopicPartition(topicName, p.partition()))
                    .collect(Collectors.toList());

            if (partitions.isEmpty()) {
                return 0L;
            }

            // Step 2: 查询每个分区的最新位点（Log End Offset）
            Map<TopicPartition, OffsetSpec> latestQuery = new HashMap<>();
            for (TopicPartition tp : partitions) {
                latestQuery.put(tp, OffsetSpec.latest());
            }

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    adminClient.listOffsets(latestQuery).all().get();

            // Step 3: 查询该消费组已提交位点（Committed Offset）
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();

            // Step 4: 分区级 lag 累加
            long totalLag = 0L;
            for (TopicPartition tp : partitions) {
                long latest = latestOffsets.get(tp).offset();

                // 无提交位点时按 0 处理（与现有逻辑保持一致）
                OffsetAndMetadata om = committedOffsets.get(tp);
                long committed = (om == null) ? 0L : om.offset();

                long lag = latest - committed;
                if (lag < 0) {
                    lag = 0L;
                }

                totalLag += lag;
            }

            return totalLag;
        } catch (Exception e) {
            logger.error("计算未消费消息数失败, topic={}, groupId={}", topicName, groupId, e);
            return 0L;
        }
    }












    // JSON 解析器，用于从 Kafka 最新消息中解析 source.scn
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * 从多个单分区 topic 的最新消息中读取最大的 Oracle CDC 启动位点。
     */
    public Optional<OracleStartupOffset> readStartupOffsetFromLatestMessages(Collection<String> topics,
                                                                             String expectedDataSourceId) {
        // 空 topic 集合直接返回空。
        if (topics == null || topics.isEmpty()) {
            return Optional.empty();
        }

        // 构造启动位点读取专用 Kafka Consumer 配置。
        Properties props = buildStartupScnConsumerProperties(expectedDataSourceId);

        // 初始化最大启动位点。
        OracleStartupOffset maxOffset = null;

        // 统计可读取 topic 数。
        int readableTopicCount = 0;

        // 统计空 topic 数。
        int emptyTopicCount = 0;

        // 统计无法解析启动位点的 topic 数。
        int noOffsetTopicCount = 0;

        // 创建临时 KafkaConsumer。
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 遍历所有 topic。
            for (String topic : topics) {
                // 读取当前 topic 最新消息中的启动位点。
                Optional<OracleStartupOffset> topicOffset =
                        readLatestStartupOffsetFromSinglePartitionTopic(consumer, topic, expectedDataSourceId);

                // 当前 topic 读到启动位点时参与最大值比较。
                if (topicOffset.isPresent()) {
                    // 累加可读 topic 数。
                    readableTopicCount++;

                    // 获取当前 topic 启动位点。
                    OracleStartupOffset current = topicOffset.get();

                    // 按 commit_scn 的可比较数字部分选最大位点。
                    if (maxOffset == null
                            || current.comparableCommitScn() > maxOffset.comparableCommitScn()) {
                        maxOffset = current;
                    }

                    // 继续下一个 topic。
                    continue;
                }

                // 读取当前 topic 的 end offset。
                Long endOffset = readEndOffsetOfSinglePartitionTopic(consumer, topic);

                // end offset 为空或为 0 时认为 topic 为空。
                if (endOffset == null || endOffset <= 0L) {
                    emptyTopicCount++;
                } else {
                    noOffsetTopicCount++;
                }
            }
        }

        // 记录启动位点读取统计。
        logger.info(
                "Read Oracle startup offset from Kafka latest messages, dataSourceId={}, topicCount={}, readableTopicCount={}, emptyTopicCount={}, noOffsetTopicCount={}, startupOffset={}",
                expectedDataSourceId,
                topics.size(),
                readableTopicCount,
                emptyTopicCount,
                noOffsetTopicCount,
                maxOffset
        );

        // 返回最大启动位点。
        return Optional.ofNullable(maxOffset);
    }

    /**
     * 读取单分区 topic 最新一条消息中的 Oracle CDC 启动位点。
     */
    private Optional<OracleStartupOffset> readLatestStartupOffsetFromSinglePartitionTopic(
            KafkaConsumer<String, String> consumer,
            String topic,
            String expectedDataSourceId) {

        // 当前系统约定每个 topic 只有 partition 0。
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        try {
            // 将 consumer 分配到当前 topic partition。
            consumer.assign(Collections.singletonList(topicPartition));

            // 读取当前 partition 的 end offset。
            long endOffset = consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);

            // end offset 小于等于 0 表示 topic 没有消息。
            if (endOffset <= 0L) {
                return Optional.empty();
            }

            // 定位到最后一条消息。
            consumer.seek(topicPartition, endOffset - 1L);

            // 拉取最后一条消息。
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            // 遍历当前 partition 返回的消息。
            for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                return parseStartupOffsetFromCdcMessage(record.value(), expectedDataSourceId, topic);
            }

            // 记录 seek 后没有拉到消息的情况。
            logger.warn("No latest record returned from Kafka, topic={}, partition=0, endOffset={}",
                    topic, endOffset);

            // 返回空启动位点。
            return Optional.empty();
        } catch (Exception e) {
            // 单个 topic 读取失败时跳过，避免影响其它 topic。
            logger.warn("读取当前 topic 最新启动位点失败，数据可能已经过期。topic={}, dataSourceId={}",
                    topic, expectedDataSourceId, e);

            // 返回空启动位点。
            return Optional.empty();
        }
    }

    /**
     * 从 CDC JSON 消息中解析 Oracle CDC 启动位点。
     */
    private Optional<OracleStartupOffset> parseStartupOffsetFromCdcMessage(String value,
                                                                           String expectedDataSourceId,
                                                                           String topic) {
        // 空消息直接返回空。
        if (value == null || value.trim().isEmpty()) {
            return Optional.empty();
        }

        try {
            // 解析 JSON 根节点。
            JsonNode root = OBJECT_MAPPER.readTree(value);

            // 读取业务库 ID。
            String actualDataSourceId = root.path("business_datasource_id").asText(null);

            // 业务库 ID 不匹配时跳过。
            if (actualDataSourceId != null && !expectedDataSourceId.equals(actualDataSourceId)) {
                logger.warn("Kafka latest message dataSourceId mismatch, topic={}, expected={}, actual={}",
                        topic, expectedDataSourceId, actualDataSourceId);
                return Optional.empty();
            }

            // 读取 source 节点。
            JsonNode sourceNode = root.path("source");

            // 读取 source.scn。
            JsonNode scnNode = sourceNode.path("scn");

            // source.scn 不存在时返回空。
            if (scnNode.isMissingNode() || scnNode.isNull()) {
                return Optional.empty();
            }

            // 获取 scn 文本。
            String scnText = scnNode.asText();

            // scn 为空时返回空。
            if (scnText == null || scnText.trim().isEmpty()) {
                return Optional.empty();
            }

            // 解析 scn 数字。
            Long scn = Long.parseLong(scnText.trim());

            // 读取 source.commit_scn。
            JsonNode commitScnNode = sourceNode.path("commit_scn");

            // 初始化 commitScn。
            String commitScn = null;

            // commit_scn 存在时读取文本。
            if (!commitScnNode.isMissingNode() && !commitScnNode.isNull()) {
                commitScn = commitScnNode.asText(null);
            }

            // 构造启动位点，快照消息没有 commit_scn 时由 OracleStartupOffset 内部兜底。
            return Optional.of(new OracleStartupOffset(scn, commitScn));
        } catch (Exception e) {
            // 记录解析失败。
            logger.warn("Failed to parse Oracle startup offset from Kafka latest message, topic={}",
                    topic, e);

            // 返回空启动位点。
            return Optional.empty();
        }
    }

    // 读取单分区 topic 的 end offset
    private Long readEndOffsetOfSinglePartitionTopic(KafkaConsumer<String, String> consumer, String topic) {
        // 当前系统约定每个 topic 只有 partition 0
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        try {
            // 查询当前 partition 的 end offset
            return consumer.endOffsets(Collections.singleton(topicPartition)).get(topicPartition);
        } catch (Exception e) {
            // 查询失败时返回 null
            return null;
        }
    }


    // 构造读取启动 SCN 的临时 KafkaConsumer 配置
    private Properties buildStartupScnConsumerProperties(String dataSourceId) {
        // 创建 Kafka Consumer 配置对象
        Properties props = new Properties();

        // 设置 Kafka broker 地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);

        // 设置 key 反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置 value 反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 设置临时 groupId，避免影响任何正式消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "sync-client-startup-scn-" + dataSourceId + "-" + UUID.randomUUID());

        // 禁止自动提交 offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 禁止 Kafka 自动重置 offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // 返回 Consumer 配置
        return props;
    }



    /**
     * 查询单分区 Kafka topic 当前仍保留的最早 offset。
     */
    public long fetchEarliestOffsetOfSinglePartitionTopic(String topicName) {
        // 标准化 topic 名称
        String normalizedTopic = normalizeTopicName(topicName);

        // 当前系统约定日志 topic 只有 0 号分区
        TopicPartition topicPartition = new TopicPartition(normalizedTopic, 0);

        // 创建临时 KafkaConsumer 配置
        Properties props = buildByteArrayConsumerProperties("earliest-offset-probe-" + normalizedTopic);

        // 创建临时 KafkaConsumer
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            // 手动分配目标 topic partition
            consumer.assign(Collections.singletonList(topicPartition));

            // 定位到 Kafka 当前仍保留的最早 offset
            consumer.seekToBeginning(Collections.singletonList(topicPartition));

            // 读取当前位置，即当前最早 offset
            long earliestOffset = consumer.position(topicPartition, Duration.ofSeconds(10));

            // 记录查询结果
            logger.info("查询 Kafka 当前最早 offset 完成，topic={}，partition=0，earliestOffset={}",
                    normalizedTopic, earliestOffset);

            // 返回当前最早 offset
            return earliestOffset;
        }
    }

    /**
     * 构造 byte[] 消费者配置。
     */
    private Properties buildByteArrayConsumerProperties(String groupIdPrefix) {
        // 创建 Kafka Consumer 配置
        Properties props = new Properties();

        // 设置 Kafka broker 地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);

        // 设置 key 反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // 设置 value 反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        // 设置临时 groupId，避免影响正式消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdPrefix + "-" + UUID.randomUUID());

        // 禁止自动提交 Kafka offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 禁止自动重置 offset，当前方法使用手动 assign + seekToBeginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // 返回 Consumer 配置
        return props;
    }

    /**
     * 标准化单个 topic 名称。
     */
    private String normalizeTopicName(String topicName) {
        // topic 不能为空
        if (topicName == null || topicName.trim().isEmpty()) {
            throw new IllegalArgumentException("topicName is blank");
        }

        // 返回去除首尾空白后的 topic
        return topicName.trim();
    }









}
