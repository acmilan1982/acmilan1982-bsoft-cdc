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
import com.bsoft.pipeline.plan.PlanContext;
import org.apache.flink.streaming.api.datastream.DataStream;
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

    // 当前 planId runtime arg
    public static final String CURRENT_PLAN_ID_RUNTIME_ARG = "pipeline.current.planId";

    // 当前 snapshot scn runtime arg
    public static final String SNAPSHOT_SCN_RUNTIME_ARG = "pipeline.snapshot.scn";

    public SnapshotSqlPlanNode(String id,
                               List<String> inputs,
                               String output,
                               Map<String, Object> params,
                               PlanContext planContext) {
        super(id, PlanNodeType.SQL, inputs, output,params, planContext);
    }

    @Override
    public Table apply(PlanContext context) {
        // 读取 SQL 文本
        String sql = getRequiredParam("sql", "select");

        // 读取当前 SQL 所属 schema
        String schema = getRequiredParam("schema");

        // 读取业务数据源 ID
        String dataSourceId = getRequiredParam("data_source_id", "dataFromSourceId");

        // 读取当前 planId
        String planId = requireCurrentPlanId(context);

        // 读取当前 snapshotScn
        Long snapshotScn = requireSnapshotScn(context);

        // 获取业务数据源注册表
        BusinessDataSourceConfigRegistry sourceRegistry = requireService(context, BusinessDataSourceConfigRegistry.class);

        // 获取 SQL 节点 schema 注册表
        SqlNodeSchemaRegistry sqlNodeSchemaRegistry = requireService(context, SqlNodeSchemaRegistry.class);

        // 根据 dataSourceId 获取源库配置
        BusinessDataSourceConfig sourceConfig = sourceRegistry.findBusinessDataSourceConfigByDataSourceId(dataSourceId);
        if (sourceConfig == null) {
            throw new IllegalStateException("sourceConfig not found, dataSourceId=" + dataSourceId);
        }

        // 当前 snapshot SQL 只支持 Oracle 源库
        if (sourceConfig.getDataSourceType() != DatabaseType.ORACLE) {
            throw new IllegalStateException(
                    "SnapshotSqlPlanNode currently only supports ORACLE source, nodeId=" + id
                            + ", dataSourceId=" + dataSourceId
                            + ", dbType=" + sourceConfig.getDataSourceType()
            );
        }

        // 获取当前 SQL 节点的输出 schema
        OracleColumnsQueryResult sqlSchemaResult = sqlNodeSchemaRegistry.resolveRequired(planId, id);

        // 获取字段列表
        List<OracleColumnMetadata> columns = sqlSchemaResult.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException(
                    "sql schema columns are empty, planId=" + planId + ", nodeId=" + id
            );
        }

        // 将 Oracle 字段元数据转换成 Flink 运行时 schema
        RuntimeTableSchema runtimeTableSchema = RuntimeTableSchemaBuilder.build(columns);

        // 创建 JDBC snapshot source，并传入 snapshotScn
        JdbcSqlSnapshotSourceFunction sourceFunction = new JdbcSqlSnapshotSourceFunction(
                sourceConfig,
                schema,
                sql,
                snapshotScn,
                runtimeTableSchema.getColumns(),
                runtimeTableSchema.getRowTypeInfo()
        );

        // 将 JDBC source 注册到当前 env
        DataStream<Row> snapshotStream = context.env()
                .addSource(sourceFunction)
                .name("snapshot-sql-" + safeName(planId) + "-" + safeName(id))
                .returns(runtimeTableSchema.getRowTypeInfo());

        // 将 DataStream 转成 Flink Table
        return context.tableEnv().fromChangelogStream(
                snapshotStream,
                runtimeTableSchema.getSchema(),
                ChangelogMode.insertOnly()
        );
    }

    private Long requireSnapshotScn(PlanContext context) {
        // 从 PlanContext runtime args 中读取 snapshotScn
        String value = context.getRuntimeArgAsString(SNAPSHOT_SCN_RUNTIME_ARG);

        // snapshotScn 必须存在
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("missing runtime arg: " + SNAPSHOT_SCN_RUNTIME_ARG);
        }

        try {
            // 将字符串解析为 long
            long scn = Long.parseLong(value.trim());

            // SCN 必须为正数
            if (scn <= 0) {
                throw new IllegalStateException("snapshotScn must be positive, value=" + value);
            }

            return scn;
        } catch (NumberFormatException e) {
            // snapshotScn 格式非法时直接失败
            throw new IllegalStateException("invalid snapshotScn, value=" + value, e);
        }
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