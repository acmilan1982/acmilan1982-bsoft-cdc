package com.bsoft.server.k2db;

import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.constants.CdcSqlConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class OracleCdcJsonParser  {
    private static final Logger logger = LogManager.getLogger(OracleCdcJsonParser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 当前主题已处理的记录总数
    private static long recordCount = 0L;


    private static long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    public OracleCdcJsonParser() {
    }


    public static KafkaPayload parseKafkaMessage(String value) throws Exception {
        // ✅ 记录方法开始时间
        long methodStartTime = System.currentTimeMillis();
        JsonNode root = objectMapper.readTree(value);


        // 操作类型 r:插入 d：删除 u：更新
        String op = root.get("op").asText();

        // 非以下操作类型的，直接忽略不处理
        if (!op.equals("r") && !op.equals("c") && !op.equals("u") && !op.equals("d") && !op.equals("ddl")) {
            return null;
        }

        if(op.equals("ddl")){
            // ddl操作
            String ddlStatement = root.get("ddl_statement").asText();
            return new KafkaPayload(op,
                    ddlStatement);
        }


        // kafka上的原始消息
        String rawMessage = root.get("raw_message").asText();;
        // 源库用户名
        String schemaName = root.get("schema").asText();;
        // 源库表名
        String tableName = root.get("table").asText();;
        // 当前数据的事务提交时间
        String dmlTime = root.get("dml_time").asText();
        // 当前消息写入kafka的时间
        String kafkaEnqueueTime = root.get("kafka_enqueue_time").asText();

        // 业务库数据源id
        String businessDataSourceId = root.get("business_datasource_id").asText();
        // 消息偏移量
        Long offset = root.get("kafka_offset").asLong();






        // 提取 消息 中的 after节点，before节点
        JsonNode afterNode = root.get("after");
        JsonNode beforeNode = root.get("before");


        // afterNode和beforeNode都没有，啥都干不了
        if ((afterNode == null || afterNode.isNull()) && (beforeNode == null || beforeNode.isNull())) {
            return null;
        }

        // 持有当前表中，date,timestamp类型的字段，这些字段需要特殊处理
        Set<String> dateFieldSet = new HashSet<>();
        JsonNode dateFieldNode = root.get("meta").get("dateFields");
        if (dateFieldNode != null && dateFieldNode.isArray()) {
            for (JsonNode f : dateFieldNode) {
                dateFieldSet.add(f.asText());   // 已经是小写，不用再处理
            }
        }


        // 解析当前表的主键
        Set<String> primaryKeySet = new HashSet<>();
        JsonNode primaryKeyNode = root.get("meta").get("primaryKey");
        if (primaryKeyNode != null && primaryKeyNode.isArray()) {
            for (JsonNode p : primaryKeyNode) {
                primaryKeySet.add(p.asText()); // 已经是小写，不用再处理
            }
        }

        // dataMap：持有消息中的有效数据，增删改会用到这些数据
        Map<String, Object> beforeMap = convertJsonToMap(beforeNode,dateFieldSet);
        Map<String, Object> afterMap = convertJsonToMap(afterNode,dateFieldSet);



        // 消息中的cdc_rowid实际值就是ROWID
        // 给cdc_rowid字段加上数据源前缀，避免不同数据源间的ROWID冲突
        beforeMap.computeIfPresent(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER,
                (key, nodeValue) -> businessDataSourceId  + "_" + nodeValue);

        afterMap.computeIfPresent(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER,
                (key, nodeValue) -> businessDataSourceId  + "_" + nodeValue);

        // ✅ 累计执行时间
        totalDeserializeTime += (System.currentTimeMillis() - methodStartTime);
        ++recordCount;

        if(recordCount % 1000 == 0){


//            logger.info("累计OracleCdcJsonParser耗时: {}s, 平均单条耗时: {}s",
//
//                    totalDeserializeTime / 1000.0,
//                    (totalDeserializeTime / recordCount) / 1000.0);


        }

        return new KafkaPayload(schemaName,tableName, op,
                                dmlTime,
                                kafkaEnqueueTime,
                                offset,
                                businessDataSourceId,
                                beforeMap, afterMap, dateFieldSet,primaryKeySet,rawMessage,
                                isSnapshotData(root));
    }

    private static Map<String, Object> convertJsonToMap(JsonNode nodeNode, Set<String> dateFields) throws com.fasterxml.jackson.core.JsonProcessingException {
        Map<String, Object> validData = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> beforeFields = nodeNode.fields();
        while (beforeFields.hasNext()) {
            Map.Entry<String, JsonNode> field = beforeFields.next();
            // 字段名，消息中的字段名都是小写，这里转成小写以防万一
            String key = field.getKey().toLowerCase();


//            // date或timestamp类型的，特殊处理
//            if (dateFields.contains(key)) {
//                long epochMillis = field.getValue().asLong();
//                LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
//                validData.put(key, date);
//            } else {// 其他类型的不做处理
//                validData.put(key, objectMapper.treeToValue(field.getValue(), Object.class));
//            }

            // 保持原始类型，date或timestamp类型的，落库前再处理
            validData.put(key, objectMapper.treeToValue(field.getValue(), Object.class));

        }

        return validData;
    }

    private static LocalDateTime convertStringToLocalDateTime(String dateTime)  {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return LocalDateTime.parse(dateTime, formatter);
    }



    /**
     * 判断是否为快照数据
     * @param root JSON根节点
     * @return true表示快照数据,false表示非快照数据
     */
    private static boolean isSnapshotData(JsonNode root) {
        JsonNode sourceNode = root.get("source");
        if (sourceNode == null) {
            return false;
        }

        JsonNode snapshotNode = sourceNode.get("snapshot");
        if (snapshotNode == null || !snapshotNode.isTextual()) {
            return false;
        }

        String snapshotValue = snapshotNode.asText();
        // 字符串"true"或"last"表示快照数据,其他都不是
        return "true".equalsIgnoreCase(snapshotValue) || "last".equalsIgnoreCase(snapshotValue);
    }



}
