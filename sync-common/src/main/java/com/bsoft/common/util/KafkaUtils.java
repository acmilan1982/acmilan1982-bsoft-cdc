package com.bsoft.common.util;


import com.bsoft.common.bean.ClientTopicInfo;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        adminClient = AdminClient.create(props);
    }


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

    private void createKafkaTopicsIfNotExistsInternal(Collection<String> topics) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<NewTopic> newTopics = new ArrayList<>();

            for (String topic : topics) {
                if (!existingTopics.contains(topic)) {
                    logger.info("[初始化] Kafka Topic不存在，自动创建: : {}", topic);
                    newTopics.add(new NewTopic(topic, 1, (short) 1));
                } else {
                    logger.info("当前主题已经存在，不再创建: {}", topic);
                }
            }

            if (!newTopics.isEmpty()) {
                adminClient.createTopics(newTopics);
            }
        } catch (Exception e) {
            throw new RuntimeException("Kafka Topic 创建失败", e);
        }
    }


    // 检查指定主题是否存在，不存在则等待
    public void waitUntilTopicExists(List<String> topics) {
        topics.stream().forEach(topic -> this.waitUntilTopicExists(topic));

    }


    // 检查指定主题是否存在，不存在则等待
    public void waitUntilTopicExists(String topicName) {
        System.out.println("开始检查 Topic 是否存在: " + topicName);
        while (true) {
            try {
                Set<String> topics = adminClient.listTopics(new ListTopicsOptions().listInternal(true))
                        .names().get();
                if (topics.contains(topicName)) {
                    System.out.println("发现 Topic: " + topicName + " 已存在，退出检查。");
                    break;
                } else {
                    System.out.println("Topic " + topicName + " 不存在，等待 5 秒后重试...");
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
}
