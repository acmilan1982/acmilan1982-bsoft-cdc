
package com.bsoft.client.o2k;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.enums.JobStartupMode;
import com.bsoft.common.constants.CdcSqlConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
// 修改导入语句，使用正确的包名
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 自定义 Debezium 反序列化器
 * 支持快照完成检测和控制信号发送
 */
public class CustomDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final Logger logger = LogManager.getLogger(CustomDebeziumDeserializationSchema.class);

    private final BusinessDataSourceConfig businessDataSourceConfig;
    private final ClientInfo clientInfo;
    private static final ObjectMapper mapper = new ObjectMapper();



    // 快照完成标志
    private final AtomicBoolean snapshotCompleteFlags =  new AtomicBoolean(false);

    public CustomDebeziumDeserializationSchema(BusinessDataSourceConfig businessDataSourceConfig, ClientInfo clientInfo) {
        this.businessDataSourceConfig = businessDataSourceConfig;
        this.clientInfo = clientInfo;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {

        // 1. 处理DDL事件
        if (isDDLEvent(record)) {
            handleDDLEvent(record, out);
            return;
        }

        // 2. 处理DML事件
        // 2-1:提取debezium的基本结构
        Struct value = (Struct) record.value();
        if (value == null) return;

        // 提取基本结构,这3种结构是debezium的基本输出,基础输出不做任何修改，原样输出
        Struct after = value.getStruct("after");
        Struct before = value.getStruct("before");
        Struct source = value.getStruct("source");

        // 2-2:生成输出到kafka的JSON
        // 构建当前方法的输出 JSON
        ObjectNode result = mapper.createObjectNode();
        Envelope.Operation operation = Envelope.operationFor(record);

        // 基本信息
        // 操作类型 r:快照数据 c: 插入 d：删除 u：更新
        result.put("op", operation.code().toLowerCase());
        // 源库schema
        String schema = source.getString("schema");
        // 源库表名
        String tableName = source.getString("table");
        // 源库全限定表名
        String fullyQualifiedTableName  = schema + "." + tableName;


        result.put("schema", schema);
        result.put("table", tableName);

        // 获取快照状态
        String snapshotValue = source.getString("snapshot");
        logger.debug("📊 表 {} 的快照状态: {}", tableName, snapshotValue);

        // 判断是否为快照数据
        boolean isSnapshot = "true".equalsIgnoreCase(snapshotValue)
                || "last".equalsIgnoreCase(snapshotValue);

        // 事务发生的时间
        long tsMs = source.getInt64("ts_ms");
        // 如果是快照，事务发生的时间毫无意义，这里硬编码一个约定的时间
        if (operation == Envelope.Operation.READ && isSnapshot) {
            // 快照数据使用固定时间 "1900-01-01 00:00:00"
            tsMs = -2208988800000L;
        }

        result.put("dml_time", DateFormatUtils.format(tsMs, "yyyy-MM-dd HH:mm:ss"));
        result.put("kafka_enqueue_time", DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss"));
        result.put("business_datasource_id", businessDataSourceConfig.getDataSourceId());




        // 处理数据结构，source，after，before原样输出
        ObjectNode sourceNode = buildSourceNode(source);    // keys小写
        ObjectNode afterNode = parseStructToJson(after);    // keys小写
        ObjectNode beforeNode = parseStructToJson(before);  // keys小写

        // 特别加入meta节点，该节点并不由debezium提供，而是我们自己加入的，其内容包括：主键,时间类型的字段
        ObjectNode metaNode = buildMetaNode(after, fullyQualifiedTableName); // keys小写


        // 除了基本信息之外，再加4个节点
        result.set("source", sourceNode);
        result.set("after", afterNode);
        result.set("before", beforeNode);
        result.set("meta", metaNode);


        // 考虑到业务库的表，可能没有配置主键或唯一键的场景，这里获取rowid以应付这些场景
        // step1:统一从 Headers 中提取 ROWID
        String rowId = extractRowIdFromHeaders(record);

        // step2:ROWID加入到 after/before 节点中，并更名为cdc_rowid
        setRowIdIfAbsent(rowId,record, result);


        // 转换为字符串，准备输出到kafka
        String jsonOutput = mapper.writeValueAsString(result);


        // 发往下游，用来更新scn
        out.collect(jsonOutput);

        // 如果处于快照模式，且已经完成快照，发送完成信号，检测 snapshot = "last" - 表示该表的快照已完成
        if(clientInfo.getJobStartupMode() == JobStartupMode.SNAPSHOT && "last".equalsIgnoreCase(snapshotValue)){
            // 检查并处理快照完成信号
            handleSnapshotCompletionSignal(out);
        }

    }

    // 从 Headers 中提取 ROWID
    public String extractRowIdFromHeaders(SourceRecord record)  {

        Headers headers = record.headers();
        if (headers == null || headers.isEmpty()) {

            return null;
        }
        for (Header header : headers) {
            if(CdcColumnConstants.ROWID_HEADER_KEY.equals(header.key())) {
                return  header.value().toString();
            }
        }
        return null;
    }

    // 从 Headers 中提取 ROWID
    public void setRowIdIfAbsent(String rowId,SourceRecord record, ObjectNode result) {
        if (rowId == null) return;
        ObjectNode afterNode = (ObjectNode) result.get("after");
        ObjectNode beforeNode = (ObjectNode) result.get("before");
        String op = result.get("op").asText();

        switch (op) {
            case "r": // 新增操作
            case "c": // 快照操作
                if (afterNode != null) afterNode.put(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER, rowId);
                break;
            case "u": // 更新操作
                if (afterNode != null) afterNode.put(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER, rowId);
                if (beforeNode != null) beforeNode.put(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER, rowId);
                break;
            case "d": // 删除操作
                if (beforeNode != null) beforeNode.put(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER, rowId);
                break;
            default:
                // 不处理其他操作
        }
    }

    private static void logWhere(Class<?> c, String tag){
        try {
            System.out.println(tag + " -> " + c.getName() + " @ " +
                    c.getProtectionDomain().getCodeSource().getLocation());
        } catch (Throwable t) {
            System.out.println(tag + " -> " + c.getName() + " @ <unknown>");
        }
    }




    // 在多表快照场景中，只有最后一个表的最后一条记录会标记为 "snapshot":"last"，而不是每个表都有自己的 last 标记。
    private void handleSnapshotCompletionSignal(Collector<String> out) throws Exception {


        if (!snapshotCompleteFlags.getAndSet(true)) {
            // 创建快照完成控制信号
            ObjectNode controlSignal = mapper.createObjectNode();
            controlSignal.put("control_type", "SNAPSHOT_COMPLETE");
            controlSignal.put("timestamp", System.currentTimeMillis());
            controlSignal.put("datasource_id", businessDataSourceConfig.getDataSourceId());

            // 输出控制信号
            String controlJson = mapper.writeValueAsString(controlSignal);
            out.collect(controlJson);

            logger.info("快照完成信号已发送");
            logger.debug(" 控制信号内容: {}", controlJson);
        }

    }

//    // 在多表快照场景中，只有最后一个表的最后一条记录会标记为 "snapshot":"last"，而不是每个表都有自己的 last 标记。
//    private void handleSnapshotCompletionSignal(String fullyQualifiedTableName, Struct source, Collector<String> out) throws Exception {
//
//        // 确保每个表只发送一次完成信号
//        AtomicBoolean completedFlag = snapshotCompleteFlags.computeIfAbsent(fullyQualifiedTableName, k -> new AtomicBoolean(false));
//        if (!completedFlag.getAndSet(true)) {
//            // 创建快照完成控制信号
//            ObjectNode controlSignal = mapper.createObjectNode();
//            controlSignal.put("control_type", "SNAPSHOT_COMPLETE");
//            controlSignal.put("table", fullyQualifiedTableName);
//            controlSignal.put("schema", source.getString("schema"));
//            controlSignal.put("timestamp", System.currentTimeMillis());
//            controlSignal.put("datasource_id", businessDataSourceConfig.getDataSourceId());
//
//            // 输出控制信号
//            String controlJson = mapper.writeValueAsString(controlSignal);
//            out.collect(controlJson);
//
//            logger.info("表 {} 的快照完成信号已发送", fullyQualifiedTableName);
//            logger.debug(" 控制信号内容: {}", controlJson);
//        } else {
//            logger.debug("⚠ 表 {} 的快照完成信号已发送过，跳过", fullyQualifiedTableName);
//        }
//
//    }

    /**
     * 构建 source 节点
     */
    private ObjectNode buildSourceNode(Struct source) {
        ObjectNode sourceNode = mapper.createObjectNode();
        Schema sourceSchema = source.schema();

        for (Field field : sourceSchema.fields()) {
            Object val = source.get(field);
            if (val != null) {
                sourceNode.put(field.name().toLowerCase(), val.toString());
            }
        }

        return sourceNode;
    }

    /**
     * 构建 meta 节点
     */
    private ObjectNode buildMetaNode(Struct after, String fullyQualifiedTableName) {
        ObjectNode metaNode = mapper.createObjectNode();

        // 提取日期字段
        ArrayNode dateFieldArrayNode = mapper.createArrayNode();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                if (field.schema().name() != null &&
                        field.schema().name().toLowerCase().contains("timestamp")) {
                    // 转成小写
                    dateFieldArrayNode.add(field.name().toLowerCase());
                }
            }
        }
        metaNode.set("dateFields", dateFieldArrayNode);

        // 提取主键
        ArrayNode primaryKeyArrayNode = mapper.createArrayNode();
        try {
            List<String> primaryKeys = businessDataSourceConfig.getKeyColumnsByFullyQualifiedTableName(fullyQualifiedTableName);

            // 一般已经是小写了，这里再保险起见
            for (String pk : primaryKeys) {
                primaryKeyArrayNode.add(TextNode.valueOf(pk.toLowerCase()));
            }
        } catch (Exception e) {
            logger.warn("⚠️ 获取表 {} 的主键配置失败: {}", fullyQualifiedTableName, e.getMessage());
        }

        metaNode.set("primaryKey", primaryKeyArrayNode);
        return metaNode;
    }

    /**
     * 将 Struct 解析为 JSON 节点
     */
    private ObjectNode parseStructToJson(Struct struct) {
        ObjectNode node = mapper.createObjectNode();
        if (struct == null) return node;

        Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            Object val = struct.get(field);
            if (val == null) continue;

            if (val instanceof Struct) {
                Struct inner = (Struct) val;
                Object innerValue = inner.get("value");

                if (innerValue instanceof byte[]) {
                    byte[] bytes = (byte[]) innerValue;
                    try {
                        int scale = 0;

                        // 修改：从 Field 的 schema 中获取 scale
                        Map<String, String> params = field.schema().parameters();
                        if (params != null && params.containsKey("scale")) {
                            scale = Integer.parseInt(params.get("scale"));
                        } else {
                            // 从内层 Struct 的 fields 中查找 scale 字段
                            List<Field> innerFields = inner.schema().fields();
                            for (Field innerField : innerFields) {
                                if ("scale".equals(innerField.name())) {
                                    Object scaleValue = inner.get(innerField);
                                    if (scaleValue != null) {
                                        scale = Integer.parseInt(scaleValue.toString());
                                        break;
                                    }
                                }
                            }
                        }

                        BigDecimal number = new BigDecimal(new BigInteger(bytes), scale);
                        node.put(field.name().toLowerCase(), number);
                    } catch (Exception e) {
                        logger.warn("解析数值字段 {} 失败: {}", field.name(), e.getMessage());
                        node.put(field.name().toLowerCase(), new String(bytes));
                    }
                } else {
                    node.put(field.name().toLowerCase(), innerValue.toString());
                }
            } else {
                node.put(field.name().toLowerCase(), val.toString());

            }
        }

        return node;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }



    //-----------------------------------DDL的处理------------------------------------
    //-----------------------------------DDL的处理------------------------------------
    //-----------------------------------DDL的处理------------------------------------
    //-----------------------------------DDL的处理------------------------------------
    //-----------------------------------DDL的处理------------------------------------



    /**
     * 检查是否为DDL事件
     */
    private boolean isDDLEvent(SourceRecord record) {
        // 方法1：检查topic名称（DDL事件通常有特殊的topic）
        String topic = record.topic();
        if (topic != null && topic.contains(".schema-changes")) {
            return true;
        }

        // 方法2：检查value结构
        Object value = record.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;

            // io.debezium.connector.oracle.SchemaChangeValue
            String name = struct.schema().name();
            if (name.contains("SchemaChangeValue")) {
                return true;
            }

            System.out.println();
            // DDL事件通常包含ddl字段
            try {
                Object ddlField = struct.get("ddl");
                return ddlField != null;
            } catch (Exception e) {
                // 忽略字段不存在的异常
            }
        }



        return false;
    }

    /**
     * 处理DDL事件
     */
    private void handleDDLEvent(SourceRecord record, Collector<String> out) throws Exception {
        Struct value = (Struct) record.value();
        if (value == null) return;

        ObjectNode ddlResult = mapper.createObjectNode();

        try {
            // 获取DDL基本信息
            Struct source = value.getStruct("source");
            String ddlStatement = value.getString("ddl");
            String databaseName = value.getString("databaseName");
            String schemaName = value.getString("schemaName");

            // 解析DDL操作类型
            String ddlType = parseDDLType(ddlStatement);

            // 只处理修改表字段的操作
            if(!ddlType.contains("ALTER_TABLE")){
                return;
            }

            String tableName = parseTableName(ddlStatement, ddlType);

            // 构建DDL事件JSON
            ddlResult.put("op", "ddl");
            ddlResult.put("ddl_type", ddlType);
            ddlResult.put("ddl_statement", ddlStatement);
            ddlResult.put("database", databaseName);
            ddlResult.put("schema", schemaName != null ? schemaName : source.getString("schema"));
            ddlResult.put("table", tableName);
            ddlResult.put("timestamp", System.currentTimeMillis());
            ddlResult.put("business_datasource_id", businessDataSourceConfig.getDataSourceId());

            // 添加source信息
            if (source != null) {
                ddlResult.set("source", buildSourceNode(source));
            }

            String ddlJson = mapper.writeValueAsString(ddlResult);
            logger.info("🔧 检测到DDL事件: 类型={}, 表={}, SQL={}", ddlType, tableName, ddlStatement);

            // 输出DDL事件
            out.collect(ddlJson);

        } catch (Exception e) {
            logger.error("❌ 处理DDL事件失败: {}", e.getMessage(), e);

            // 创建错误事件
            ddlResult.put("event_type", "DDL_ERROR");
            ddlResult.put("error", e.getMessage());
            ddlResult.put("timestamp", System.currentTimeMillis());

            out.collect(mapper.writeValueAsString(ddlResult));
        }
    }

    /**
     * 解析DDL操作类型
     */
    private String parseDDLType(String ddlStatement) {
        if (ddlStatement == null) return "UNKNOWN";

        String upperDDL = ddlStatement.trim().toUpperCase();

        if (upperDDL.startsWith("CREATE TABLE")) {
            return "CREATE_TABLE";
        } else if (upperDDL.startsWith("ALTER TABLE")) {
            if (upperDDL.contains("ADD COLUMN") || upperDDL.contains("ADD ")) {
                return "ALTER_TABLE_ADD_COLUMN";
            } else if (upperDDL.contains("DROP COLUMN")) {
                return "ALTER_TABLE_DROP_COLUMN";
            } else if (upperDDL.contains("MODIFY") || upperDDL.contains("ALTER COLUMN")) {
                return "ALTER_TABLE_MODIFY_COLUMN";
            } else if (upperDDL.contains("RENAME")) {
                return "ALTER_TABLE_RENAME";
            } else {
                return "ALTER_TABLE";
            }
        } else if (upperDDL.startsWith("DROP TABLE")) {
            return "DROP_TABLE";
        } else if (upperDDL.startsWith("TRUNCATE TABLE")) {
            return "TRUNCATE_TABLE";
        } else if (upperDDL.startsWith("CREATE INDEX")) {
            return "CREATE_INDEX";
        } else if (upperDDL.startsWith("DROP INDEX")) {
            return "DROP_INDEX";
        } else {
            return "OTHER_DDL";
        }
    }

    /**
     * 从DDL语句中解析表名
     */
    private String parseTableName(String ddlStatement, String ddlType) {
        if (ddlStatement == null) return null;

        try {
            String upperDDL = ddlStatement.trim().toUpperCase();
            String tableName = null;

            if (ddlType.startsWith("CREATE_TABLE") || ddlType.startsWith("DROP_TABLE") ||
                    ddlType.startsWith("ALTER_TABLE") || ddlType.startsWith("TRUNCATE_TABLE")) {

                // 正则表达式匹配表名
                String pattern = "(?:CREATE|DROP|ALTER|TRUNCATE)\\s+TABLE\\s+(?:IF\\s+(?:NOT\\s+)?EXISTS\\s+)?([\\w\\.\"]+)";
                java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern, java.util.regex.Pattern.CASE_INSENSITIVE);
                java.util.regex.Matcher m = p.matcher(ddlStatement);

                if (m.find()) {
                    tableName = m.group(1).replaceAll("\"", ""); // 移除引号
                }
            }

            return tableName;
        } catch (Exception e) {
            logger.warn("⚠️ 解析表名失败: {}", e.getMessage());
            return null;
        }
    }














}