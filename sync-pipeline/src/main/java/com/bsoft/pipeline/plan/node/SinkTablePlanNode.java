package com.bsoft.pipeline.plan.node;

import com.bsoft.common.factory.TopicNameFactory;
import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.k2k.PipelineExecutionMode;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;



/**
 * SINK_TABLE 节点：
 * 1) 接收上游 Table（changelog 流）；
 * 2) 将每条 Row 转成 Debezium-like JSON（供 sync-server 复用）；
 * 3) 写入一个或多个 Kafka topic。
 *
 * 关键设计点：
 * - 不保留原始消息，直接在 sink 端重组消息结构；
 * - op 由 RowKind 映射得到：INSERT->r, UPDATE_AFTER->u, DELETE->d；
 * - meta.dateFields 从输出流 schema 自动推导；
 * - meta.primaryKey 这里保持空数组（sync-server 当前 update/delete 会统一改走 cdc_rowid）。
 */
public class SinkTablePlanNode extends AbstractPlanNode {

    private final String bootstrapServers;
    private final List<String> topics = new ArrayList<>();

    public SinkTablePlanNode(String id,
                             List<String> inputs,
                             String output,
                             Map<String, Object> params,
                             PipelineInitContext initContext) {
        super(id, PlanNodeType.SINK_TABLE, inputs, output, params, initContext);
        this.bootstrapServers = resolveBootstrapServers(params, this.initContext);
        resolveTopicsFromNodeValue(params);
    }

    @Override
    public Table apply(PlanContext context) {
        if (inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("SINK_TABLE requires one input");
        }

        Table input = context.getDataset(inputs.get(0));
        if (input == null) {
            throw new IllegalArgumentException("SINK_TABLE input not found: " + inputs.get(0));
        }

        // ===== 1) 获取上游输出 schema（列名 + 列类型） =====
        ResolvedSchema resolvedSchema = input.getResolvedSchema();
        List<String> cols = resolvedSchema.getColumnNames();
        List<DataType> colTypes = resolvedSchema.getColumnDataTypes();

        // dateFields：按当前输入流的类型判断
        Set<String> dateFields = resolveDateFields(cols, colTypes);

        // ===== 2) 解析可配置的 envelope 字段 =====
        // 允许从节点参数覆盖 schema/table（用于关联结果“落到新表名”）
        String envelopeSchema = defaultIfBlank(
                nullableString(params, "envelope_schema", "schema"),
                ""
        );
        String envelopeTable = defaultIfBlank(
                nullableString(params, "envelope_table", "table", "table_name"),
                extractFirstTargetTable(params) // 若未配置，则回落到 target 里第一个 table_name
        );
        if (envelopeTable == null) {
            envelopeTable = "";
        }

        // 业务数据源 ID，供下游日志和路由使用
        String businessDatasourceId = defaultIfBlank(
                nullableString(params, "business_datasource_id", "data_source_id", "businessDataSourceId"),
                ""
        );

        Map<String, Object> sourceTemplate = mapParam(params, "source_template", "sourceTemplate");
        if (sourceTemplate.isEmpty()) {
            sourceTemplate = buildDefaultSourceTemplate(envelopeSchema, envelopeTable);
        } else {
            sourceTemplate = new LinkedHashMap<>(sourceTemplate);
        }

        boolean snapshotMode = PipelineExecutionMode.SNAPSHOT.name()
                .equalsIgnoreCase(context.getRuntimeArgAsString("pipeline.execution.mode"));

        sourceTemplate.put("snapshot", String.valueOf(snapshotMode));
        sourceTemplate.put("name", snapshotMode ? "pipeline_snapshot" : "kafka_pipeline");
        sourceTemplate.put("connector", snapshotMode ? "jdbc" : "flink");
        // ===== 3) Table -> Changelog DataStream<Row> =====
        DataStream<Row> in = context.tableEnv().toChangelogStream(input);

        // 仅保留 INSERT / UPDATE_AFTER / DELETE
        // UPDATE_BEFORE 不输出（避免重复语义和下游歧义）
        DataStream<String> out = in
                .filter(r -> r.getKind() == RowKind.INSERT
                        || r.getKind() == RowKind.UPDATE_AFTER
                        || r.getKind() == RowKind.DELETE)
                .map(new EnvelopeJsonMapFunction(
                        cols,
                        dateFields,
                        envelopeSchema,
                        envelopeTable,
                        businessDatasourceId,
                        sourceTemplate
                ));

        // ===== 4) 发往多个 Kafka topic =====
        for (String topic : topics) {
            KafkaSink<String> sink = KafkaSink.<String>builder()
                    .setBootstrapServers(bootstrapServers)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            out.sinkTo(sink).name("sink-table-" + topic);
        }

        // 返回 input，保持 PlanExecutor 链式行为不变
        return input;
    }

    // -------------------- 内部 MapFunction：Row -> Debezium-like JSON --------------------

    private static class EnvelopeJsonMapFunction implements MapFunction<Row, String> {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private final List<String> cols;
        private final Set<String> dateFields;
        private final String schema;
        private final String table;
        private final String businessDatasourceId;
        private final Map<String, Object> sourceTemplate;

        EnvelopeJsonMapFunction(List<String> cols,
                                Set<String> dateFields,
                                String schema,
                                String table,
                                String businessDatasourceId,
                                Map<String, Object> sourceTemplate) {
            this.cols = new ArrayList<>(cols);
            this.dateFields = new LinkedHashSet<>(dateFields);
            this.schema = schema;
            this.table = table;
            this.businessDatasourceId = businessDatasourceId;
            this.sourceTemplate = new LinkedHashMap<>(sourceTemplate);
        }

        @Override
        public String map(Row r) throws Exception {
            String op = toOp(r.getKind());
            if (op == null) {
                // 理论上前面的 filter 已经过滤了不可识别类型，这里兜底保护
                return null;
            }

            ObjectNode root = MAPPER.createObjectNode();

            // ===== 可自定义/可覆盖的头字段 =====
            root.put("op", op);
            root.put("schema", schema);
            root.put("table", table);
            root.put("dml_time", "1900-01-01 08:00:00");
            root.put("kafka_enqueue_time", LocalDateTime.now().format(FMT));
            root.put("business_datasource_id", businessDatasourceId);

            // source 节点可自定义；下游 isSnapshotData 只会读 source.snapshot
            root.set("source", MAPPER.valueToTree(sourceTemplate));

            // ===== after / before =====
            // INSERT / UPDATE_AFTER -> after
            // DELETE -> before
            ObjectNode afterNode = MAPPER.createObjectNode();
            ObjectNode beforeNode = MAPPER.createObjectNode();
            ObjectNode targetNode = "d".equals(op) ? beforeNode : afterNode;

            for (int i = 0; i < cols.size(); i++) {
                String col = cols.get(i);
                targetNode.set(col, MAPPER.valueToTree(r.getField(i)));
            }

            root.set("after", afterNode);
            root.set("before", beforeNode);

            // ===== meta =====
            ObjectNode meta = MAPPER.createObjectNode();
            ArrayNode dateArr = meta.putArray("dateFields");
            for (String f : dateFields) {
                dateArr.add(f);
            }
            meta.putArray("primaryKey"); // 当前方案固定空；下游更新/删除依赖 cdc_rowid
            root.set("meta", meta);

            return MAPPER.writeValueAsString(root);
        }

        /**
         * RowKind -> Debezium 风格 op
         * INSERT -> r
         * UPDATE_AFTER -> u
         * DELETE -> d
         */
        private String toOp(RowKind kind) {
            if (kind == RowKind.INSERT) return "r";
            if (kind == RowKind.UPDATE_AFTER) return "u";
            if (kind == RowKind.DELETE) return "d";
            return null;
        }
    }

    // -------------------- schema 推导 --------------------

    /**
     * 从 Flink 输出列类型推导 meta.dateFields。
     * 仅识别“真实时间类型”列，不处理“被 cast 成字符串但语义是时间”的场景（按你的约束忽略）。
     */
    private Set<String> resolveDateFields(List<String> cols, List<DataType> colTypes) {
        Set<String> result = new LinkedHashSet<>();
        for (int i = 0; i < cols.size(); i++) {
            LogicalTypeRoot root = colTypes.get(i).getLogicalType().getTypeRoot();
            if (root == LogicalTypeRoot.DATE
                    || root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                    || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                    || root == LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) {
                result.add(cols.get(i));
            }
        }
        return result;
    }

    // -------------------- 参数解析 --------------------

    private static String resolveBootstrapServers(Map<String, Object> params, PipelineInitContext initContext) {
        String fromParams = nullableString(params, "bootstrapServers");
        if (fromParams != null) {
            return fromParams;
        }

        String fromInit = initContext.getString(PipelineInitContext.KAFKA_BOOTSTRAP_SERVERS);
        if (fromInit != null) {
            return fromInit;
        }

        throw new IllegalArgumentException("SINK_TABLE missing bootstrapServers (params/initContext)");
    }

    /**
     * 从 params.target 解析目标 topic 列表。
     * 兼容你当前配置：target = [{data_source_id, table_name}, ...]
     */
    @SuppressWarnings("unchecked")
    private void resolveTopicsFromNodeValue(Map<String, Object> params) {
        if (params == null) {
            throw new IllegalArgumentException("SINK_TABLE params is null");
        }

        Object targetObj = params.get("target");
        if (!(targetObj instanceof List)) {
            throw new IllegalArgumentException("SINK_TABLE param `target` must be a List");
        }

        List<?> list = (List<?>) targetObj;
        for (Object item : list) {
            if (!(item instanceof Map)) {
                continue;
            }
            Map<String, Object> map = (Map<String, Object>) item;

            String dataSourceId = defaultIfBlank(String.valueOf(map.get("data_source_id")), "");
            String tableName = defaultIfBlank(String.valueOf(map.get("table_name")), "");

            if (!tableName.isEmpty()) {
                topics.add(TopicNameFactory.generateAssocTopic(tableName, dataSourceId));
            }
        }

        if (topics.isEmpty()) {
            throw new IllegalArgumentException("SINK_TABLE param `target` has no valid `table_name`");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapParam(Map<String, Object> params, String... keys) {
        if (params == null) return Collections.emptyMap();
        for (String key : keys) {
            Object v = params.get(key);
            if (v instanceof Map) {
                return new LinkedHashMap<>((Map<String, Object>) v);
            }
        }
        return Collections.emptyMap();
    }

    private static String nullableString(Map<String, Object> params, String... keys) {
        if (params == null) return null;
        for (String key : keys) {
            Object v = params.get(key);
            if (v == null) continue;
            String s = String.valueOf(v).trim();
            if (!s.isEmpty()) return s;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String extractFirstTargetTable(Map<String, Object> params) {
        if (params == null) return null;
        Object targetObj = params.get("target");
        if (!(targetObj instanceof List)) return null;

        List<?> list = (List<?>) targetObj;
        for (Object item : list) {
            if (!(item instanceof Map)) continue;
            Object v = ((Map<String, Object>) item).get("table_name");
            if (v != null) {
                String s = String.valueOf(v).trim();
                if (!s.isEmpty()) return s;
            }
        }
        return null;
    }

    private static String defaultIfBlank(String value, String defaultValue) {
        if (value == null) return defaultValue;
        String s = value.trim();
        return s.isEmpty() ? defaultValue : s;
    }

    private static Map<String, Object> buildDefaultSourceTemplate(String schema, String table) {
        Map<String, Object> source = new LinkedHashMap<>();
        // 你可以按需在节点参数里覆盖这些字段
        source.put("version", "pipeline-generated");
        source.put("connector", "flink");
        source.put("name", "kafka_pipeline");
        source.put("snapshot", "false");
        source.put("schema", schema);
        source.put("table", table);
        return source;
    }


}