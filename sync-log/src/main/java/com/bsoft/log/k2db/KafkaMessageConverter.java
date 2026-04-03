package com.bsoft.log.k2db;

import com.bsoft.common.bean.SyncLogEntry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaMessageConverter {
    private static final Logger logger = LogManager.getLogger(KafkaMessageConverter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 当前主题已处理的记录总数
    private static long recordCount = 0L;


    private static long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    public KafkaMessageConverter() {
    }

    /**
     * 解析 Kafka 消息为 SyncLogEntry
     * @param kafkaMessage Kafka 原始消息 JSON 字符串
     * @return SyncLogEntry 对象
     */
    public static SyncLogEntry parseKafkaMessage(String kafkaMessage) throws Exception {
        JsonNode msgNode = objectMapper.readTree(kafkaMessage);

        return SyncLogEntry.builder()
                .setInstructionType(msgNode.get("instructionType").asText())
                .setSourceTime(msgNode.get("sourceTime").asText())
                .setTargetTime(msgNode.get("targetTime").asText())
                .setInsertTime(msgNode.has("insertTime") && !msgNode.get("insertTime").isNull()
                        ? msgNode.get("insertTime").asText() : null)
                .setLogDetail(msgNode.get("logDetail").asText())
                .setSourceDataSourceId(msgNode.get("sourceDataSourceId").asText())
                .setTargetDataSourceId(msgNode.get("targetDataSourceId").asText())
                .setSourceSchemaName(msgNode.get("sourceSchemaName").asText())
                .setSourceTableName(msgNode.get("sourceTableName").asText())
                .setTargetTableName(msgNode.get("targetTableName").asText())
                .setResultDetail(msgNode.has("resultDetail") && !msgNode.get("resultDetail").isNull()
                        ? msgNode.get("resultDetail").asText() : null)
                .setResultCode(msgNode.get("resultCode").asInt())
                .setOffset(msgNode.get("offset").asLong())
                .setKafkaEnqueueTime(msgNode.get("kafkaEnqueueTime").asText())
                .setRawMessage(msgNode.get("rawMessage").asText())
                .build();
    }



}
