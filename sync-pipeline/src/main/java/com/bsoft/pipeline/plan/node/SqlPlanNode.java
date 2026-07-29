package com.bsoft.pipeline.plan.node;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.sql.SqlScopeAliasRegistry;
import org.apache.flink.table.api.Table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlPlanNode extends AbstractPlanNode implements Serializable {


    public SqlPlanNode() {
    }

    public SqlPlanNode(String id, List<String> inputs, String output, Map<String, Object> params, PlanContext planContext) {
        super(id, PlanNodeType.SQL, inputs, output, params, planContext);
    }

    @Override
    public Table apply(PlanContext context) {
        String sql = getSql();
        String scopeDsId = getParam("data_source_id", "dataFromSourceId");
        String scopeSchema = getParam("schema");

        SqlScopeAliasRegistry aliasRegistry =
                (SqlScopeAliasRegistry) context.getService(SqlScopeAliasRegistry.class.getName());

        List<String> createdAliases = new ArrayList<>();
        try {
            if (aliasRegistry != null) {
                Map<String, String> aliasMap = aliasRegistry.findAliases(scopeDsId, scopeSchema);
                for (Map.Entry<String, String> e : aliasMap.entrySet()) {
                    String shortName = e.getKey();
                    String uniqueView = e.getValue();
                    context.tableEnv().createTemporaryView(shortName, context.tableEnv().from(uniqueView));
                    createdAliases.add(shortName);
                }
            }

            if (inputs != null && !inputs.isEmpty()) {
                String inputKey = inputs.get(0);
                Table input = context.getDataset(inputKey);
                if (input == null) {
                    throw new IllegalArgumentException("SQL input not found: " + inputKey);
                }
                String inputView = "__sql_in_" + id;
                context.tableEnv().createTemporaryView(inputView, input);
                if (sql.contains("${input}")) {
                    return context.tableEnv().sqlQuery(sql.replace("${input}", inputView));
                }
            }

            return context.tableEnv().sqlQuery(sql);
        } finally {
            for (String alias : createdAliases) {
                context.tableEnv().dropTemporaryView(alias);
            }
        }
    }

    private String getParam(String... keys) {
        if (params == null) return null;
        for (String k : keys) {
            Object v = params.get(k);
            if (v != null && !String.valueOf(v).trim().isEmpty()) {
                return String.valueOf(v).trim();
            }
        }
        return null;
    }

    private String getSql() {
        Object v = params.get("sql");
        if (v == null || String.valueOf(v).trim().isEmpty()) {
            v = params.get("select");
        }
        if (v == null || String.valueOf(v).trim().isEmpty()) {
            throw new IllegalArgumentException("SQL node requires `sql` or `select`");
        }
        return String.valueOf(v).trim();
    }


}

