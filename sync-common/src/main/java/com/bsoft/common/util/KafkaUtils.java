package com.bsoft.common.util;


import com.bsoft.common.bean.ClientTopicInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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


        List<String> topics =  clientTopicInfoList.stream().map(ClientTopicInfo::getTopic).collect(Collectors.toList());
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<NewTopic> newTopics = new ArrayList<>();

            for (String topic : topics) {
                if (!existingTopics.contains(topic)) {
                    System.out.println("[初始化] Kafka Topic不存在，自动创建: " + topic);

                    newTopics.add(new NewTopic(topic, 1, (short) 1));
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

    /**
     * 获取指定主题尚未被消费的消息总数
     * @param topicName 主题名称
     * @return 未消费的消息总数
     */
    public Long getUnconsumedMessageCount(String topicName) {
        // 先检查主题是否存在
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.contains(topicName)) {
//                logger.warn("主题 {} 不存在", topicName);
                return 0L;
            }
        } catch (Exception e) {
            logger.error("检查主题是否存在时出错", e);
            return 0L;
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        String monitorGroupId = "monitor-group-" + topicName + "-" + System.currentTimeMillis();

        props.put("group.id", monitorGroupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topicName)
                    .stream()
                    .map(p -> new TopicPartition(topicName, p.partition()))
                    .collect(Collectors.toList());

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            long totalUnconsumed = 0;
            for (TopicPartition partition : partitions) {
                long committed = consumer.committed(partition) != null ?
                        consumer.committed(partition).offset() : 0;
                long latest = endOffsets.get(partition);
                totalUnconsumed += (latest - committed);
            }

            return totalUnconsumed;
        }
    }
}
