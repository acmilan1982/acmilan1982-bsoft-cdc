package com.bsoft.pipeline.functions;

/**
 * ClassName: InjectOffsetIntoMessageSchema
 * Package:   com.bsoft.server.k2m
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/21 - 20:19
 */

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
public class InjectOffsetIntoMessageSchema implements KafkaRecordDeserializationSchema<String> {

    private static final Logger logger = LogManager.getLogger(InjectOffsetIntoMessageSchema.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    // 当前反序列化器处理的主题名
    private String topic;

    // 当前主题已处理的记录总数
    private long recordCount = 0L;

    private long startTime = System.currentTimeMillis();

    private long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    public InjectOffsetIntoMessageSchema(String topic) {
        this.topic = topic;
    }


    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws JsonProcessingException {

        // ✅ 记录方法开始时间
        long methodStartTime = System.currentTimeMillis();

        // 获取 msg 原始 JSON 字符串
        String msgJson = new String(record.value(), StandardCharsets.UTF_8);

        // 解析为 ObjectNode
        ObjectNode msgNode = (ObjectNode) mapper.readTree(msgJson);

        // 消息中写入偏移量
        msgNode.put("kafka_offset", record.offset());
        // 消息中写入原始消息，用于日志
        msgNode.put("raw_message", msgJson);

        out.collect(mapper.writeValueAsString(msgNode));

        // ✅ 累计执行时间
        totalDeserializeTime += (System.currentTimeMillis() - methodStartTime);


        ++recordCount;

        if(recordCount % 1000 == 0){

            double executionTimeSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
            startTime = System.currentTimeMillis();
//            logger.info(" 当前主题: {} , 已处理消息数量: {},耗时 {}", topic,recordCount,executionTimeSeconds);
//            logger.info("累计反序列化耗时: {}s, 平均单条耗时: {}s",
//
//                    totalDeserializeTime / 1000.0,
//                    (totalDeserializeTime / recordCount) / 1000.0);
//
//            logger.info(" [Kafka消息原始内容]: {}", msgJson);


        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
