package com.bsoft.pipeline.plan.runtime;


import com.bsoft.common.bean.BusinessDataSourceConfigRegistry;
import com.bsoft.pipeline.k2k.PipelineExecutionMode;
import com.bsoft.pipeline.k2k.SqlNodeSchemaRegistry;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.sql.SqlScopeAliasRegistry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 统一创建 PlanContext，避免 snapshot / incremental 两阶段各自散落地拼 runtime args 和 services。
 *
 * 当前职责：
 * 1. 构建 PlanContext
 * 2. 注入通用 runtime args
 * 3. 按执行阶段注入所需 service
 *
 * 不负责：
 * - 创建 Flink env / tableEnv
 * - 注册 Kafka source / temporary view
 * - 执行具体 RulePlan
 */
public class PlanContextFactory {

    public static final String RUNTIME_ARG_EXECUTION_MODE = "pipeline.execution.mode";
    public static final String RUNTIME_ARG_SNAPSHOT_ENABLED = "pipeline.snapshot.enabled";
    public static final String RUNTIME_ARG_CURRENT_PLAN_ID = "pipeline.current.planId";
    public static final String RUNTIME_ARG_CURRENT_RUN_ID = "pipeline.current.runId";
    public static final String RUNTIME_ARG_SNAPSHOT_SCN = "pipeline.snapshot.scn";


    public PlanContextFactory() {
    }

    public PlanContext createSnapshotContext(SnapshotContextSpec spec) {
        validateCommon(spec);

        PlanContext context = new PlanContext(spec.getEnv(), spec.getTableEnv());
        putCommonArgs(context, spec);
        context.putRuntimeArg(RUNTIME_ARG_EXECUTION_MODE, PipelineExecutionMode.SNAPSHOT.name());
        context.putRuntimeArg(RUNTIME_ARG_SNAPSHOT_ENABLED, "true");

        registerIfPresent(context, BusinessDataSourceConfigRegistry.class, spec.getBusinessDataSourceConfigRegistry());
        registerIfPresent(context, SqlNodeSchemaRegistry.class, spec.getSqlNodeSchemaRegistry());

        putExtraRuntimeArgs(context, spec.getExtraRuntimeArgs());
        registerExtraServices(context, spec.getExtraServices());

        return context;
    }

    public PlanContext createIncrementalContext(IncrementalContextSpec spec) {
        validateCommon(spec);

        PlanContext context = new PlanContext(spec.getEnv(), spec.getTableEnv());
        putCommonArgs(context, spec);
        context.putRuntimeArg(RUNTIME_ARG_EXECUTION_MODE, PipelineExecutionMode.INCREMENTAL.name());
        context.putRuntimeArg(RUNTIME_ARG_SNAPSHOT_ENABLED, "false");

        registerIfPresent(context, SqlScopeAliasRegistry.class, spec.getSqlScopeAliasRegistry());
        registerIfPresent(context, SqlNodeSchemaRegistry.class, spec.getSqlNodeSchemaRegistry());

        putExtraRuntimeArgs(context, spec.getExtraRuntimeArgs());
        registerExtraServices(context, spec.getExtraServices());

        return context;
    }

    private void validateCommon(BaseContextSpec spec) {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(spec.getEnv(), "env");
        Objects.requireNonNull(spec.getTableEnv(), "tableEnv");

        requireText(spec.getKafkaBootstrapServers(), "kafkaBootstrapServers");
        requireText(spec.getPlanId(), "planId");
        requireText(spec.getRunId(), "runId");
    }

    private void putCommonArgs(PlanContext context, BaseContextSpec spec) {
        context.setKafkaBootstrapServers(spec.getKafkaBootstrapServers());
        context.putRuntimeArg(RUNTIME_ARG_CURRENT_PLAN_ID, spec.getPlanId());
        context.putRuntimeArg(RUNTIME_ARG_CURRENT_RUN_ID, spec.getRunId());

        if (spec.getSnapshotScn() != null) {
            context.putRuntimeArg(RUNTIME_ARG_SNAPSHOT_SCN, String.valueOf(spec.getSnapshotScn()));
        }
    }

    private void putExtraRuntimeArgs(PlanContext context, Map<String, Object> args) {
        if (args == null || args.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Object> entry : args.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.trim().isEmpty()) {
                continue;
            }
            context.putRuntimeArg(key.trim(), entry.getValue());
        }
    }

    private void registerExtraServices(PlanContext context, Map<String, Object> services) {
        if (services == null || services.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Object> entry : services.entrySet()) {
            String key = entry.getKey();
            Object service = entry.getValue();
            if (key == null || key.trim().isEmpty() || service == null) {
                continue;
            }
            context.registerService(key.trim(), service);
        }
    }

    private <T> void registerIfPresent(PlanContext context, Class<T> type, T service) {
        if (service != null) {
            context.registerService(type.getName(), service);
        }
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }

    public abstract static class BaseContextSpec {
        private final StreamExecutionEnvironment env;
        private final StreamTableEnvironment tableEnv;
        private final String kafkaBootstrapServers;
        private final String planId;
        private final String runId;
        private final Long snapshotScn;
        private final Map<String, Object> extraRuntimeArgs;
        private final Map<String, Object> extraServices;

        protected BaseContextSpec(StreamExecutionEnvironment env,
                                  StreamTableEnvironment tableEnv,
                                  String kafkaBootstrapServers,
                                  String planId,
                                  String runId,
                                  Long snapshotScn,
                                  Map<String, Object> extraRuntimeArgs,
                                  Map<String, Object> extraServices) {
            this.env = env;
            this.tableEnv = tableEnv;
            this.kafkaBootstrapServers = trim(kafkaBootstrapServers);
            this.planId = trim(planId);
            this.runId = trim(runId);
            this.snapshotScn = snapshotScn;
            this.extraRuntimeArgs = copy(extraRuntimeArgs);
            this.extraServices = copy(extraServices);
        }

        public StreamExecutionEnvironment getEnv() {
            return env;
        }

        public StreamTableEnvironment getTableEnv() {
            return tableEnv;
        }

        public String getKafkaBootstrapServers() {
            return kafkaBootstrapServers;
        }

        public String getPlanId() {
            return planId;
        }

        public String getRunId() {
            return runId;
        }

        public Long getSnapshotScn() {
            return snapshotScn;
        }

        public Map<String, Object> getExtraRuntimeArgs() {
            return extraRuntimeArgs;
        }

        public Map<String, Object> getExtraServices() {
            return extraServices;
        }

        private static String trim(String value) {
            return value == null ? null : value.trim();
        }

        private static Map<String, Object> copy(Map<String, Object> source) {
            return source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
        }
    }

    public static final class SnapshotContextSpec extends BaseContextSpec {
        private final BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;
        private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry;

        private SnapshotContextSpec(Builder builder) {
            super(
                    builder.env,
                    builder.tableEnv,
                    builder.kafkaBootstrapServers,
                    builder.planId,
                    builder.runId,
                    builder.snapshotScn,
                    builder.extraRuntimeArgs,
                    builder.extraServices
            );
            this.businessDataSourceConfigRegistry = builder.businessDataSourceConfigRegistry;
            this.sqlNodeSchemaRegistry = builder.sqlNodeSchemaRegistry;
        }

        public BusinessDataSourceConfigRegistry getBusinessDataSourceConfigRegistry() {
            return businessDataSourceConfigRegistry;
        }

        public SqlNodeSchemaRegistry getSqlNodeSchemaRegistry() {
            return sqlNodeSchemaRegistry;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private StreamExecutionEnvironment env;
            private StreamTableEnvironment tableEnv;
            private String kafkaBootstrapServers;
            private String planId;
            private String runId;
            private Long snapshotScn;
            private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;
            private SqlNodeSchemaRegistry sqlNodeSchemaRegistry;
            private Map<String, Object> extraRuntimeArgs = new LinkedHashMap<>();
            private Map<String, Object> extraServices = new LinkedHashMap<>();

            public Builder env(StreamExecutionEnvironment env) {
                this.env = env;
                return this;
            }

            public Builder tableEnv(StreamTableEnvironment tableEnv) {
                this.tableEnv = tableEnv;
                return this;
            }

            public Builder kafkaBootstrapServers(String kafkaBootstrapServers) {
                this.kafkaBootstrapServers = kafkaBootstrapServers;
                return this;
            }

            public Builder planId(String planId) {
                this.planId = planId;
                return this;
            }

            public Builder runId(String runId) {
                this.runId = runId;
                return this;
            }

            public Builder snapshotScn(Long snapshotScn) {
                this.snapshotScn = snapshotScn;
                return this;
            }

            public Builder businessDataSourceConfigRegistry(BusinessDataSourceConfigRegistry registry) {
                this.businessDataSourceConfigRegistry = registry;
                return this;
            }

            public Builder sqlNodeSchemaRegistry(SqlNodeSchemaRegistry registry) {
                this.sqlNodeSchemaRegistry = registry;
                return this;
            }

            public Builder putRuntimeArg(String key, Object value) {
                if (key != null && !key.trim().isEmpty()) {
                    this.extraRuntimeArgs.put(key.trim(), value);
                }
                return this;
            }

            public Builder registerService(String key, Object service) {
                if (key != null && !key.trim().isEmpty() && service != null) {
                    this.extraServices.put(key.trim(), service);
                }
                return this;
            }

            public SnapshotContextSpec build() {
                return new SnapshotContextSpec(this);
            }
        }
    }

    public static final class IncrementalContextSpec extends BaseContextSpec {
        private final SqlScopeAliasRegistry sqlScopeAliasRegistry;
        private final SqlNodeSchemaRegistry sqlNodeSchemaRegistry;

        private IncrementalContextSpec(Builder builder) {
            super(
                    builder.env,
                    builder.tableEnv,
                    builder.kafkaBootstrapServers,
                    builder.planId,
                    builder.runId,
                    builder.snapshotScn,
                    builder.extraRuntimeArgs,
                    builder.extraServices
            );
            this.sqlScopeAliasRegistry = builder.sqlScopeAliasRegistry;
            this.sqlNodeSchemaRegistry = builder.sqlNodeSchemaRegistry;
        }

        public SqlScopeAliasRegistry getSqlScopeAliasRegistry() {
            return sqlScopeAliasRegistry;
        }

        public SqlNodeSchemaRegistry getSqlNodeSchemaRegistry() {
            return sqlNodeSchemaRegistry;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private StreamExecutionEnvironment env;
            private StreamTableEnvironment tableEnv;
            private String kafkaBootstrapServers;
            private String planId;
            private String runId;
            private Long snapshotScn;
            private SqlScopeAliasRegistry sqlScopeAliasRegistry;
            private SqlNodeSchemaRegistry sqlNodeSchemaRegistry;
            private Map<String, Object> extraRuntimeArgs = new LinkedHashMap<>();
            private Map<String, Object> extraServices = new LinkedHashMap<>();

            public Builder env(StreamExecutionEnvironment env) {
                this.env = env;
                return this;
            }

            public Builder tableEnv(StreamTableEnvironment tableEnv) {
                this.tableEnv = tableEnv;
                return this;
            }

            public Builder kafkaBootstrapServers(String kafkaBootstrapServers) {
                this.kafkaBootstrapServers = kafkaBootstrapServers;
                return this;
            }

            public Builder planId(String planId) {
                this.planId = planId;
                return this;
            }

            public Builder runId(String runId) {
                this.runId = runId;
                return this;
            }

            public Builder snapshotScn(Long snapshotScn) {
                this.snapshotScn = snapshotScn;
                return this;
            }

            public Builder sqlScopeAliasRegistry(SqlScopeAliasRegistry registry) {
                this.sqlScopeAliasRegistry = registry;
                return this;
            }

            public Builder sqlNodeSchemaRegistry(SqlNodeSchemaRegistry registry) {
                this.sqlNodeSchemaRegistry = registry;
                return this;
            }

            public Builder putRuntimeArg(String key, Object value) {
                if (key != null && !key.trim().isEmpty()) {
                    this.extraRuntimeArgs.put(key.trim(), value);
                }
                return this;
            }

            public Builder registerService(String key, Object service) {
                if (key != null && !key.trim().isEmpty() && service != null) {
                    this.extraServices.put(key.trim(), service);
                }
                return this;
            }

            public IncrementalContextSpec build() {
                return new IncrementalContextSpec(this);
            }
        }
    }
}
