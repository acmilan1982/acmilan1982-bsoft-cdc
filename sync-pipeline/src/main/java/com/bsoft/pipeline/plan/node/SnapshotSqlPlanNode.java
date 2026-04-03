package com.bsoft.pipeline.plan.node;


import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.BusinessDataSourceConfigRegistry;
import com.bsoft.common.dbmeta.OracleColumnsQueryResult;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.k2k.JdbcSqlSnapshotSourceFunction;
import com.bsoft.pipeline.k2k.RuntimeTableSchema;
import com.bsoft.pipeline.k2k.RuntimeTableSchemaBuilder;
import com.bsoft.pipeline.k2k.SqlNodeSchemaRegistry;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * ClassName: SnapshotSqlPlanNode
 * Package:   com.bsoft.pipeline.plan.node
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/3 - 14:55
 */
public class SnapshotSqlPlanNode extends AbstractPlanNode {

    public static final String CURRENT_PLAN_ID_RUNTIME_ARG = "pipeline.current.planId";

    public SnapshotSqlPlanNode(String id,
                               List<String> inputs,
                               String output,
                               Map<String, Object> params,
                               PipelineInitContext initContext) {
        super(id, PlanNodeType.SQL, inputs, output, params, initContext);
    }

    @Override
    public Table apply(PlanContext context) {
        String sql = getRequiredParam("sql", "select");
        String schema = getRequiredParam("schema");
        String dataSourceId = getRequiredParam("data_source_id", "dataFromSourceId");
        String planId = requireCurrentPlanId(context);

        BusinessDataSourceConfigRegistry sourceRegistry =
                requireService(context, BusinessDataSourceConfigRegistry.class);
        SqlNodeSchemaRegistry sqlNodeSchemaRegistry =
                requireService(context, SqlNodeSchemaRegistry.class);

        BusinessDataSourceConfig sourceConfig =
                sourceRegistry.findBusinessDataSourceConfigByDataSourceId(dataSourceId);
        if (sourceConfig == null) {
            throw new IllegalStateException("sourceConfig not found, dataSourceId=" + dataSourceId);
        }

        if (sourceConfig.getDataSourceType() != DatabaseType.ORACLE) {
            throw new IllegalStateException(
                    "SnapshotSqlPlanNode currently only supports ORACLE source, nodeId=" + id
                            + ", dataSourceId=" + dataSourceId
                            + ", dbType=" + sourceConfig.getDataSourceType()
            );
        }

        OracleColumnsQueryResult sqlSchemaResult = sqlNodeSchemaRegistry.resolveRequired(planId, id);
        List<OracleColumnMetadata> columns = sqlSchemaResult.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(
                    "sql schema columns are empty, planId=" + planId + ", nodeId=" + id
            );
        }

        RuntimeTableSchema runtimeTableSchema = RuntimeTableSchemaBuilder.build(columns);

        JdbcSqlSnapshotSourceFunction sourceFunction = new JdbcSqlSnapshotSourceFunction(
                sourceConfig,
                schema,
                sql,
                runtimeTableSchema.getColumns(),
                runtimeTableSchema.getRowTypeInfo()
        );

        DataStream<Row> snapshotStream = context.env()
                .addSource(sourceFunction)
                .name("snapshot-sql-" + safeName(planId) + "-" + safeName(id))
                .returns(runtimeTableSchema.getRowTypeInfo());

        return context.tableEnv().fromChangelogStream(
                snapshotStream,
                runtimeTableSchema.getSchema(),
                ChangelogMode.insertOnly()
        );
    }

    private String requireCurrentPlanId(PlanContext context) {
        String planId = context.getRuntimeArgAsString(CURRENT_PLAN_ID_RUNTIME_ARG);
        if (planId == null || planId.trim().isEmpty()) {
            throw new IllegalStateException("missing runtime arg: " + CURRENT_PLAN_ID_RUNTIME_ARG);
        }
        return planId.trim();
    }

    private <T> T requireService(PlanContext context, Class<T> type) {
        Object service = context.getService(type.getName());
        if (service == null) {
            throw new IllegalStateException("service not found: " + type.getName());
        }
        if (!type.isInstance(service)) {
            throw new IllegalStateException(
                    "service type mismatch, key=" + type.getName()
                            + ", actual=" + service.getClass().getName()
            );
        }
        return type.cast(service);
    }

    private String getRequiredParam(String... keys) {
        if (params == null || params.isEmpty()) {
            throw new IllegalArgumentException("node params is empty, nodeId=" + id);
        }

        for (String key : keys) {
            Object value = params.get(key);
            if (value != null) {
                String s = String.valueOf(value).trim();
                if (!s.isEmpty()) {
                    return s;
                }
            }
        }

        throw new IllegalArgumentException(
                "node requires non-empty param " + String.join("/", keys) + ", nodeId=" + id
        );
    }

    private String safeName(String raw) {
        String s = raw == null ? "null" : raw.trim().toLowerCase(Locale.ROOT);
        s = s.replaceAll("[^a-z0-9_]", "_");
        if (s.isEmpty() || Character.isDigit(s.charAt(0))) {
            return "n_" + s;
        }
        return s;
    }
}