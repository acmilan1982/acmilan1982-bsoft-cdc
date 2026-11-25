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

// 解析 Oracle CDC 消息；非删除 DML 必须提供日期字段及单位节点。
public class OracleCdcJsonParser  {
    private static final Logger logger = LogManager.getLogger(OracleCdcJsonParser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 当前主题已处理的记录总数
    private static long recordCount = 0L;


    private static long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    public OracleCdcJsonParser() {
    }

    // 解析 CDC 消息，并兼容缺少 dateFieldUnits 的旧消息。
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

//        if(op.equals("ddl")){
//            // Kafka 原始 topic
//            String kafkaTopic = getTextOrNull(root, "kafka_topic");
//
//            // Kafka 分区号
//            Integer kafkaPartition = getIntOrNull(root, "kafka_partition");
//
//            // Kafka offset
//            Long offset = root.get("kafka_offset").asLong();
//
//            // 原始消息
//            String rawMessage = root.get("raw_message").asText();
//
//            // DDL 语句
//            String ddlStatement = root.get("ddl_statement").asText();
//
//            // 返回带 Kafka 元数据的 DDL payload
//            return new KafkaPayload(kafkaTopic, kafkaPartition, offset, op, ddlStatement, rawMessage);
//        }

        // kafka上的原始消息
        String rawMessage = root.get("raw_message").asText();
        // 源库用户名
        String schemaName = root.get("schema").asText();
        // 源库表名
        String tableName = root.get("table").asText();
        // 当前数据的事务提交时间
        String dmlTime = root.get("dml_time").asText();
        // 当前消息写入kafka的时间
        String kafkaEnqueueTime = root.get("kafka_enqueue_time").asText();

        // 业务库数据源id
        String businessDataSourceId = root.get("business_datasource_id").asText();
        // 消息偏移量
        Long offset = root.get("kafka_offset").asLong();
        // Kafka 原始 topic，来自 InjectOffsetIntoMessageSchema 注入
        String kafkaTopic = getTextOrNull(root, "kafka_topic");
        // Kafka 分区号，当前业务只有 0 分区
        Integer kafkaPartition = getIntOrNull(root, "kafka_partition");



        // 提取 消息 中的 after节点，before节点
        JsonNode afterNode = root.get("after");
        JsonNode beforeNode = root.get("before");


        // afterNode和beforeNode都没有，啥都干不了
        if ((afterNode == null || afterNode.isNull()) && (beforeNode == null || beforeNode.isNull())) {
            return null;
        }

        // 默认创建可修改的空集合，删除消息不解析源表日期元数据。
        Set<String> dateFieldSet = new HashSet<>();
        Map<String, String> dateFieldUnits = Collections.emptyMap();

        // 快照、新增和更新消息按新协议解析，删除消息直接保留空集合。
        if (!"d".equals(op)) {
            // 解析dateFields，必须是数组，允许为空数组。
            dateFieldSet = parseDateFields(root);

            // 解析dateFieldUnits 必须是对象，允许为空对象。
            dateFieldUnits = parseDateFieldUnits(root);
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

//        return new KafkaPayload(schemaName,tableName, op,
//                                dmlTime,
//                                kafkaEnqueueTime,
//                                offset,
//                                businessDataSourceId,
//                                beforeMap, afterMap, dateFieldSet,primaryKeySet,rawMessage,
//                                isSnapshotData(root));

//        logger.info("offset : {}", offset);
//        return new KafkaPayload(schemaName,tableName, op,
//                dmlTime,
//                kafkaEnqueueTime,
//                offset,
//                businessDataSourceId,
//                beforeMap, afterMap, dateFieldSet,primaryKeySet,rawMessage,
//                isSnapshotData(root),
//                isLastSnapshotData(root),
//                isLastRecordInTable(root));

        // 解析新增的日期字段单位节点，旧消息得到空 Map。




         // 将业务数据、日期字段和单位映射一起传入消息对象。
        return new KafkaPayload(
                kafkaTopic,
                kafkaPartition,
                schemaName,
                tableName,
                op,
                dmlTime,
                kafkaEnqueueTime,
                offset,
                businessDataSourceId,
                beforeMap,
                afterMap,
                dateFieldSet,
                dateFieldUnits,
                primaryKeySet,
                rawMessage,
                isSnapshotData(root),
                isLastSnapshotData(root),
                isLastRecordInTable(root)
        );

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

    /**
     * 判断是否为快照数据
     * @param root JSON根节点
     * @return true表示快照数据,false表示非快照数据
     */
    private static boolean isLastSnapshotData(JsonNode root) {
        JsonNode sourceNode = root.get("source");
        if (sourceNode == null) {
            return false;
        }

        JsonNode snapshotNode = sourceNode.get("snapshot");
        if (snapshotNode == null || !snapshotNode.isTextual()) {
            return false;
        }

        String snapshotValue = snapshotNode.asText();

//        logger.info("snapshotValue : {}", snapshotValue);
//
//        if("last".equalsIgnoreCase(snapshotValue)){
//            return true;
//        }


        // 字符串"true"或"last"表示快照数据,其他都不是
        return "last".equalsIgnoreCase(snapshotValue);
    }

    /**
     * 判断是否为 当前表的最后一条快照数据
     * @param root JSON根节点
     * @return true表示快照数据,false表示非快照数据
     */
    private static boolean isLastRecordInTable(JsonNode root) {
        JsonNode sourceNode = root.get("source");
        if (sourceNode == null) {
            return false;
        }

        JsonNode lastRecordInTableNode = sourceNode.get("lastrecordintable");
        if (lastRecordInTableNode == null || !lastRecordInTableNode.isTextual()) {
            return false;
        }

        String lastRecordInTableValue = lastRecordInTableNode.asText();

//        logger.info("snapshotValue : {}", snapshotValue);
//
//        if("last".equalsIgnoreCase(snapshotValue)){
//            return true;
//        }


        // 字符串"true"表示最后一条快照数据,
        // 字符串"false"表示并非最后一条快照数据,
        return "true".equalsIgnoreCase(lastRecordInTableValue);
    }

    // 从 JSON 中读取字符串字段，字段不存在时返回 null
    private static String getTextOrNull(JsonNode root, String fieldName) {
        // 空 JSON 直接返回 null
        if (root == null) {
            return null;
        }

        // 获取字段节点
        JsonNode node = root.get(fieldName);

        // 字段不存在或为 null 时返回 null
        if (node == null || node.isNull()) {
            return null;
        }

        // 返回字符串值
        return node.asText();
    }

    // 从 JSON 中读取整数字段，字段不存在时返回 null
    private static Integer getIntOrNull(JsonNode root, String fieldName) {
        // 空 JSON 直接返回 null
        if (root == null) {
            return null;
        }

        // 获取字段节点
        JsonNode node = root.get(fieldName);

        // 字段不存在或为 null 时返回 null
        if (node == null || node.isNull()) {
            return null;
        }

        // 返回整数值
        return node.asInt();
    }


    // 按新协议解析日期字段数组，节点必须存在，允许返回空 Set。
    private static Set<String> parseDateFields(JsonNode root) {
        // 获取日期字段节点，缺失、null 或其他类型均不符合新协议。
        JsonNode fieldsNode = root.path("meta").path("dateFields");
        if (!fieldsNode.isArray()) {
            throw new IllegalArgumentException(
                    "meta.dateFields 必须存在且为 JSON 数组，允许为空数组 []"
            );
        }

        // 使用可修改集合，兼容后续添加 CDC 时间字段的现有代码。
        Set<String> dateFields = new HashSet<>();

        // 逐项读取日期字段名。
        for (JsonNode fieldNode : fieldsNode) {
            // 字段名必须是非空字符串。
            if (!fieldNode.isTextual() || fieldNode.textValue().trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "meta.dateFields 中的字段名必须是非空字符串"
                );
            }

            // 字段名统一为小写，与消息数据中的字段名保持一致。
            dateFields.add(fieldNode.textValue().toLowerCase(Locale.ROOT));
        }

        // 空数组自然返回空 Set，不返回 null。
        return dateFields;
    }

    // 按新协议解析日期字段单位，节点必须存在，允许返回空 Map。
    private static Map<String, String> parseDateFieldUnits(JsonNode root) {
        // 获取单位节点，不再兼容节点缺失或为 null 的旧消息。
        JsonNode unitsNode = root.path("meta").path("dateFieldUnits");
        if (!unitsNode.isObject()) {
            throw new IllegalArgumentException(
                    "meta.dateFieldUnits 必须存在且为 JSON 对象，允许为空对象 {}"
            );
        }

        // 保存字段对应的明确单位。
        Map<String, String> dateFieldUnits = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = unitsNode.fields();

        // 逐项校验并提取单位。
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode unitNode = field.getValue();

            // 字段名不能为空。
            if (fieldName.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "meta.dateFieldUnits 中的字段名不能为空"
                );
            }

            // 单位必须为字符串，不将 null 当作缺少单位继续处理。
            if (!unitNode.isTextual()) {
                throw new IllegalArgumentException(
                        "meta.dateFieldUnits." + fieldName
                                + " 必须是字符串，且为 ms、us、ns 之一"
                );
            }

            // 严格接受协议约定的单位，不按数字位数猜测。
            String unit = unitNode.textValue();
            if (!"ms".equals(unit)
                    && !"us".equals(unit)
                    && !"ns".equals(unit)) {
                throw new IllegalArgumentException(
                        "meta.dateFieldUnits." + fieldName
                                + " 的单位无效: " + unit
                                + "，仅支持 ms、us、ns"
                );
            }

            // 字段名统一为小写，单位保持协议中的原值。
            dateFieldUnits.put(fieldName.toLowerCase(Locale.ROOT), unit);
        }

        // 空对象正常返回空 Map，不返回 null。
        return dateFieldUnits;
    }

}
