package com.bsoft.client.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

// 顺序 Kafka 路由 Sink：单 sink 内解析消息并动态选择 topic
public class OrderedKafkaRoutingSink extends RichSinkFunction<String> {

    // 日志对象
    private static final Logger logger = LogManager.getLogger(OrderedKafkaRoutingSink.class);

    // Kafka broker 地址
    private final String bootstrapServers;

    // 表名到 topic 的映射，key 为 schema.table
    private final Map<String, String> topicByFullyQualifiedTableName;

    // Kafka Producer，运行时创建
    private transient KafkaProducer<String, String> producer;

    // JSON 解析器，运行时创建
    private transient ObjectMapper objectMapper;

    // 构造顺序写入 sink
    public OrderedKafkaRoutingSink(String bootstrapServers,
                                   Map<String, String> topicByFullyQualifiedTableName) {
        // 校验 Kafka 地址
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");

        // 复制 topic 映射，避免外部修改
        this.topicByFullyQualifiedTableName = new LinkedHashMap<>(
                Objects.requireNonNull(topicByFullyQualifiedTableName, "topicByFullyQualifiedTableName")
        );
    }

    // Flink 算子初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        // 调用父类初始化
        super.open(parameters);

        // 创建 JSON 解析器
        this.objectMapper = new ObjectMapper();

        // 创建 Kafka Producer
        this.producer = new KafkaProducer<>(buildProducerProperties());

        // 记录初始化日志
        logger.info("OrderedKafkaRoutingSink opened, topicRouteCount={}", topicByFullyQualifiedTableName.size());
    }

    // 处理单条 CDC 消息
    @Override
    public void invoke(String value, Context context) throws Exception {
        // 解析目标 topic
        String topic = resolveTopic(value);

        // 如果没有匹配到 topic，直接失败，让作业暴露配置问题
        if (topic == null) {
            throw new IllegalStateException("No Kafka topic route found for CDC message: " + preview(value));
        }

        // 构造 Kafka 消息
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

        // 同步发送 Kafka，等待 broker ack 后才处理下一条
        Future<?> future = producer.send(record);

        // 阻塞等待发送完成，保证当前 sink 内写入顺序
        future.get();
    }

    // Flink 算子关闭
    @Override
    public void close() throws Exception {
        // 关闭 Kafka Producer
        if (producer != null) {
            // 刷出缓冲消息
            producer.flush();

            // 关闭 producer
            producer.close();

            // 清空引用
            producer = null;
        }

        // 调用父类关闭
        super.close();

        // 记录关闭日志
        logger.info("OrderedKafkaRoutingSink closed");
    }

    // 根据 CDC 消息解析目标 topic
    private String resolveTopic(String value) throws Exception {
        // 解析 JSON 根节点
        JsonNode root = objectMapper.readTree(value);

        // 读取 schema
        String schema = root.path("schema").asText(null);

        // 读取 table
        String table = root.path("table").asText(null);

        // schema 或 table 为空时无法路由
        if (schema == null || table == null) {
            return null;
        }

        // 组装全限定表名
        String fullyQualifiedTableName = schema + "." + table;

        // 返回目标 topic
        return topicByFullyQualifiedTableName.get(fullyQualifiedTableName);
    }

    // 构造 Kafka Producer 配置
    private Properties buildProducerProperties() {
        // 创建配置对象
        Properties props = new Properties();

        // 设置 broker 地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // 设置 key 序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置 value 序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置 ack 策略
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // 设置无限重试
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        // 限制单连接未确认请求数，降低重试乱序风险
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

        // 设置请求超时
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");

        // 设置重试退避
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");

        // 设置压缩类型
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // 返回配置
        return props;
    }

    // 截断日志预览内容
    private String preview(String value) {
        // 空消息直接返回 null
        if (value == null) {
            return null;
        }

        // 返回前 500 个字符
        return value.length() <= 500 ? value : value.substring(0, 500);
    }
}