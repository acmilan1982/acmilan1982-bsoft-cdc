package com.bsoft.server.k2db;

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

// Kafka 反序列化器，用于给原始 JSON 注入 Kafka 元数据
public class InjectOffsetIntoMessageSchema implements KafkaRecordDeserializationSchema<String> {

    private static final Logger logger = LogManager.getLogger(InjectOffsetIntoMessageSchema.class);
    // JSON 工具
    private static final ObjectMapper mapper = new ObjectMapper();

    // 当前反序列化器处理的记录数
    private long recordCount = 0L;


    // 累计反序列化耗时
    private long totalDeserializeTime = 0L;

    // 默认构造器，多 topic source 下不再固定 topic
    public InjectOffsetIntoMessageSchema() {
        // topic 从 ConsumerRecord 读取
    }

    // 兼容旧调用方，旧代码传 topic 时也使用 ConsumerRecord 中的真实 topic
    public InjectOffsetIntoMessageSchema(String topic) {
        // 保留旧构造器签名，避免旧代码编译失败
    }

    // 反序列化 Kafka record，并注入 topic、partition、offset
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out)
            throws JsonProcessingException {
        // 记录方法开始时间
        long methodStartTime = System.currentTimeMillis();

        // 获取消息中的 msg
        String msgJson = new String(record.value(), StandardCharsets.UTF_8);
        // 转换格式，用于写入元数据
        ObjectNode msgNode = (ObjectNode) mapper.readTree(msgJson);


        /*
             以下是消息的元数据，消息格式一般是：
                {
                  "partition": 0,
                  "offset": 781729,
                  "msg": "{\"op\":\"r\",\"schema\":\"SPT_HIS_2023\",\"table\":\"OPT_FEE\",\"dml_time\":\"1900-01-01 08:00:00\",\"kafka_enqueue_time\":\"2026-05-13 09:58:27\",\"business_datasource_id\":\"199-source\",\"source\":{\"version\":\"1.9.8.Final\",\"connector\":\"oracle\",\"name\":\"hosp-005_199-source\",\"ts_ms\":\"1778665957000\",\"snapshot\":\"true\",\"lastrecordintable\":\"false\",\"db\":\"ORCL\",\"schema\":\"SPT_HIS_2023\",\"table\":\"OPT_FEE\",\"scn\":\"309598766\",\"ssn\":\"0\"},\"after\":{\"hid\":\"9678371-420000001476\",\"patientid\":\"3858959\",\"organizationnanme\":\"孝感市第一人民医院\",\"organizationcode\":\"420000001476\",\"hospitalcode\":\"420000001476\",\"jzlsh\":\"4477080\",\"fyje\":621.64,\"ysfyje\":621.64,\"ybfwwzf\":0,\"ylfffsdm\":\"99\",\"fyid\":\"9678371\",\"tfbz\":\"1\",\"fphm\":\"9678371\",\"stfsj\":\"1771753897000\",\"jsly\":\"1\",\"ypcfzs\":1,\"xycfs\":0,\"kjywcfs\":0,\"zcycfs\":0,\"zcaoycfs\":0,\"ghf\":0,\"xyf\":88.92,\"zcyf\":0,\"zcyf1\":0,\"zcf\":0,\"jcf\":0,\"hyf\":0,\"fsf\":0,\"zlf\":0,\"ssf\":0,\"mzf\":0,\"hlf\":0,\"ysfwf\":0,\"bzzlf\":0,\"gzclf\":0,\"dzclf\":0,\"xyzpf\":0,\"qtf\":0,\"grzfje\":0,\"yffdje\":0,\"dsfzfje\":0,\"dzfpbz\":\"0\",\"city_p\":\"4209\",\"country_p\":\"420902\",\"effectivetime_p\":\"1771753897000\",\"updatedate\":\"1771804807000\",\"uploadtime_p\":\"1771804822000\",\"state\":\"0\",\"jslymc\":\"诊间\",\"cdc_rowid\":\"AABSaXAAHAAEYV/AAP\"},\"before\":{},\"meta\":{\"dateFields\":[\"stfsj\",\"effectivetime_p\",\"updatedate\",\"uploadtime_p\",\"starttime\",\"endtime\",\"checktime\"],\"primaryKey\":[]}}",
                  "timespan": 1778637507515,
                  "date": "2026-05-13 09:58:27"
                }
         */
        // 注入 Kafka topic
        msgNode.put("kafka_topic", record.topic());

        // 注入 Kafka 分区号
        msgNode.put("kafka_partition", record.partition());

        // 注入 Kafka offset
        msgNode.put("kafka_offset", record.offset());

        // 注入原始消息，用于日志补偿
        msgNode.put("raw_message", msgJson);

        // 输出增强后的 JSON
        out.collect(mapper.writeValueAsString(msgNode));

        // 累计反序列化耗时
        totalDeserializeTime += (System.currentTimeMillis() - methodStartTime);

        // 累计记录数
        ++recordCount;

        // 保留统计入口，默认不打印
        if (recordCount % 10000 == 0) {
//            logger.info("累计反序列化时间, {} 秒",(totalDeserializeTime / 1000) );
            String tableName = msgNode.get("table").asText();
            logger.info("源表 {} ，已累计读取 {} 条消息",tableName,recordCount );
        }
    }

    // 返回输出类型
    @Override
    public TypeInformation<String> getProducedType() {
        // 输出增强后的 JSON 字符串
        return Types.STRING;
    }
}