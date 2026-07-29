package com.bsoft.log.k2db;

/**
 * ClassName: InjectOffsetIntoMessageSchema
 * Package:   com.bsoft.server.k2m
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/21 - 20:19
 */

import com.bsoft.common.bean.SyncLogEntry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

// 自定义反序列化器
public class KafkaMessageDeserializer implements KafkaRecordDeserializationSchema<LogKafkaMessage> {

    private static final Logger logger = LogManager.getLogger(KafkaMessageDeserializer.class);

    private static final ObjectMapper mapper = new ObjectMapper();


    // 当前主题已处理的记录总数
    private long recordCount = 0L;

    private long startTime = System.currentTimeMillis();

    private long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    public KafkaMessageDeserializer() {

    }


    /**
     * 反序列化 Kafka record，并生成携带 Kafka 位点信息的日志消息对象。
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<LogKafkaMessage> out) {
        // 记录方法开始时间。
        long methodStartTime = System.currentTimeMillis();

        // 将 Kafka value 转成 UTF-8 字符串。
        String msgJson = new String(record.value(), StandardCharsets.UTF_8);

        // 将 Kafka key 转成 UTF-8 字符串。
        String kafkaKey = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);

        // 将 Kafka value 解析为 SyncLogEntry。
        SyncLogEntry syncLogEntry = null;
        try {
            syncLogEntry = KafkaMessageConverter.parseKafkaMessage(msgJson);
        } catch (Exception e) {
            logger.error("解析日志消息失败，{}",e.getMessage());
        }

        // 构造携带 Kafka 元数据的日志消息对象。
        LogKafkaMessage logKafkaMessage = new LogKafkaMessage(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                kafkaKey,
                msgJson,
                syncLogEntry
        );

        // 输出给 Flink 下游算子。
        out.collect(logKafkaMessage);

        // 累计反序列化耗时。
        totalDeserializeTime += (System.currentTimeMillis() - methodStartTime);

        // 累计处理消息数。
        recordCount++;

        // 每处理 10000 条记录一次统计日志。
        if (recordCount % 10000 == 0) {
            // 计算本周期耗时秒数。
            double executionTimeSeconds = (System.currentTimeMillis() - startTime) / 1000.0;

            // 重置周期开始时间。
            startTime = System.currentTimeMillis();

            // 打印消费进度。
            logger.info("sync-log deserialized records={}, topic={}, partition={}, latestOffset={}, elapsedSeconds={}",
                    recordCount,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    executionTimeSeconds);
        }
    }

    /**
     * 返回反序列化输出类型。
     */
    @Override
    public TypeInformation<LogKafkaMessage> getProducedType() {
        // 返回 LogKafkaMessage 类型信息。
        return TypeInformation.of(LogKafkaMessage.class);
    }
}
