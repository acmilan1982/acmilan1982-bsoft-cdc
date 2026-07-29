package com.bsoft.server.k2db;

/**
 * ClassName: PipelineSqlSinkTopicResolver
 * Package:   com.bsoft.server.k2db
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/16 - 13:37
 */

import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.enums.*;
import com.bsoft.common.factory.TableNameGenerator;
import com.bsoft.common.factory.TopicNameFactory;
import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.common.util.DatasourceConfigFetcher;
import org.springframework.stereotype.Component;

import java.util.*;


public class PipelineSqlSinkTopicResolver {

    // 读取配置库
    private  DatasourceConfigFetcher configFetcher;

    // 所有目标库的配置，目标库可能有多个
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 目标表命名工具
    private TableNameGenerator tableNameGenerator;

    public PipelineSqlSinkTopicResolver(DatasourceConfigFetcher configFetcher,
                                        TargetDataSourceConfigRegistry targetDataSourceConfigRegistry,
                                        TableNameGenerator tableNameGenerator) {
        this.configFetcher = configFetcher;
        this.targetDataSourceConfigRegistry = targetDataSourceConfigRegistry;
        this.tableNameGenerator = tableNameGenerator;
    }

    public List<ServerTopicInfo> resolve(List<RulePlan> rulePlans) {
        if (rulePlans == null || rulePlans.isEmpty()) {
            return Collections.emptyList();
        }

        List<ServerTopicInfo> out = new ArrayList<>();

        // 处理所有plan
        for (RulePlan plan : rulePlans) {
            List<PlanNodeSpec> nodes = plan.getNodes();
            if (nodes == null || nodes.isEmpty()) {
                continue;
            }

            // key: 每个 node 的输出id
            // value: node
            Map<String, PlanNodeSpec> producerByOutput = new HashMap<>();
            for (PlanNodeSpec n : nodes) {
                producerByOutput.put(n.getOutput(), n);
            }

            for (PlanNodeSpec sink : nodes) {
                if (sink.getType() != PlanNodeType.SINK_TABLE) {
                    continue;
                }

                // 找到当前plan的sql输入
                PlanNodeSpec sqlNode = findNearestSqlNode(sink, producerByOutput);
                if (sqlNode == null) {
                    continue;
                }

                // 当前 plan 的sql
                String sql = getRequired(sqlNode, "sql", "select");
                // 数据源id与schema
                String sqlSchema = getRequired(sqlNode, "schema");
                String sourceDataSourceId = getRequired(sqlNode, "data_source_id", "dataFromSourceId");

                // 数据源所属的 探针
                SimpleClientInfo simpleClientInfo = configFetcher.requireActiveClientInfoByDataSourceId(sourceDataSourceId);


                for (SinkTargetSpec target : parseTargets(sink.getParams())) {
                    String targetDataSourceId = safe(target.getDataSourceId());
                    // 自定义sql，配置中指定的表名，就是最终表名
                    String finalTargetTableName = safe(target.getTableName());


                    TargetDataSourceConfig targetDataSourceConfig =
                            targetDataSourceConfigRegistry.findTargetDataSourceConfigByDataSourceId(targetDataSourceId);
                    if (targetDataSourceConfig == null) {
                        continue;
                    }



                    // 表级 Doris 参数直接挂到对应目标库配置上，
                    if (targetDataSourceConfig.getDataSourceType() == DatabaseType.DORIS
                            && target.getBizAttr() != null) {
                        targetDataSourceConfig.putTableLevelDdlBizAttr(finalTargetTableName, target.getBizAttr());
                    }

                    ServerTopicInfo t = new ServerTopicInfo();
                    t.setSourceKind(TopicSourceKind.PIPELINE_SQL_RESULT);

                    // 生成主题名
                    t.setTopic(TopicNameFactory.generateAssocTopic("",finalTargetTableName,targetDataSourceId));

                    t.setBusinessDataSourceId(sourceDataSourceId);
                    t.setTargetDataSourceId(targetDataSourceId);
                    t.setTargetTableName(finalTargetTableName);
                    t.setSchemaName(sqlSchema);
                    t.setTableName(finalTargetTableName);
                    t.setClientInfo(simpleClientInfo);

                    t.setRulePlanId(plan.getPlanId());
                    t.setSinkNodeId(sink.getId());
                    t.setSqlNodeId(sqlNode.getId());
                    t.setSqlQuery(sql);
                    t.setSqlSchemaName(sqlSchema);

                    out.add(t);
                }
            }
        }

        return out;
    }

    private PlanNodeSpec findNearestSqlNode(PlanNodeSpec sink, Map<String, PlanNodeSpec> producerByOutput) {
        if (sink.getInputs() == null || sink.getInputs().isEmpty()) return null;
        PlanNodeSpec cur = producerByOutput.get(sink.getInputs().get(0));

        while (cur != null) {
            if (cur.getType() == PlanNodeType.SQL) {
                return cur;
            }
            if (cur.getInputs() == null || cur.getInputs().isEmpty()) {
                return null;
            }
            cur = producerByOutput.get(cur.getInputs().get(0));
        }
        return null;
    }

    // 解析单个 SINK_TABLE 配置，可指定多个目标库
    private List<SinkTargetSpec> parseTargets(Map<String, Object> params) {
        if (params == null) return Collections.emptyList();
        Object v = params.get("target");
        if (!(v instanceof List)) return Collections.emptyList();

        List<SinkTargetSpec> out = new ArrayList<>();
        for (Object o : (List<?>) v) {
            if (!(o instanceof Map)) continue;

            Map<String, Object> m = (Map<String, Object>) o;
            SinkTargetSpec one = new SinkTargetSpec();
            one.setDataSourceId(safeObj(m.get("data_source_id")));
            one.setTableName(safeObj(m.get("table_name")));

            Object dorisObj = m.get("doris");
            if (dorisObj instanceof Map) {
                one.setBizAttr(toBizAttr((Map<String, Object>) dorisObj));
            }

            out.add(one);
        }
        return out;
    }


    // 这里不变，target 里仍然只解析 doris，但返回接口类型
    private DdlBizAttr toBizAttr(Map<String, Object> dorisMap) {
        DorisBizAttr biz = new DorisBizAttr();
        if (dorisMap == null || dorisMap.isEmpty()) return biz;

        Object tableModel = dorisMap.get("table_model");
        if (tableModel != null && !String.valueOf(tableModel).trim().isEmpty()) {
            biz.setTableModel(DorisTableModel.valueOf(String.valueOf(tableModel).trim().toUpperCase(Locale.ROOT)));
        }

        Object columnModel = dorisMap.get("column_model");
        if (columnModel != null && !String.valueOf(columnModel).trim().isEmpty()) {
            biz.setColumnModel(DorisColumnModel.valueOf(String.valueOf(columnModel).trim().toUpperCase(Locale.ROOT)));
        }

        Object keyColumns = dorisMap.get("key_columns");
        if (keyColumns instanceof List) {
            biz.setKeyColumns((List<String>) keyColumns);
        }

        Object hashColumns = dorisMap.get("distributed_hash_columns");
        if (hashColumns instanceof List) {
            biz.setDistributedHashColumns((List<String>) hashColumns);
        }

        Object customMapping = dorisMap.get("custom_column_mapping");
        if (customMapping instanceof Map) {
            biz.setCustomColumnMapping((Map<String, String>) customMapping);
        }

        Object buckets = dorisMap.get("buckets");
        if (buckets instanceof Number) {
            biz.setBuckets(((Number) buckets).intValue());
        }

        Object replicationNum = dorisMap.get("replication_num");
        if (replicationNum instanceof Number) {
            biz.setReplicationNum(((Number) replicationNum).intValue());
        }

        return biz;
    }

    private String getRequired(PlanNodeSpec node, String... keys) {
        Map<String, Object> params = node.getParams();
        if (params == null) {
            throw new IllegalArgumentException("node params is null, nodeId=" + node.getId());
        }
        for (String k : keys) {
            Object v = params.get(k);
            if (v != null && !String.valueOf(v).trim().isEmpty()) {
                return String.valueOf(v).trim();
            }
        }
        throw new IllegalArgumentException("missing required param in nodeId=" + node.getId());
    }

    private String safe(String s) { return s == null ? "" : s.trim(); }
    private String safeObj(Object o) { return o == null ? "" : String.valueOf(o).trim(); }

    // SINK_TABLE中单个目标库表的配置
    private static class SinkTargetSpec {
        // 目标库数据源id
        private String dataSourceId;
        // 在目标库中的最终表名
        private String tableName;
        // 表级建表属性
        private DdlBizAttr ddlBizAttr;

        public String getDataSourceId() {
            return dataSourceId;
        }

        public void setDataSourceId(String dataSourceId) {
            this.dataSourceId = dataSourceId;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public DdlBizAttr getBizAttr() {
            return ddlBizAttr;
        }

        public void setBizAttr(DdlBizAttr ddlBizAttr) {
            this.ddlBizAttr = ddlBizAttr;
        }
    }




}
