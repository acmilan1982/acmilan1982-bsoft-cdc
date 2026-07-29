package com.bsoft.pipeline.plan.runtime;

/**
 * Central runtime-arg keys shared by pipeline runtime components.
 */
public final class PlanRuntimeArgs {

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrapServers";
    public static final String EXECUTION_MODE = "pipeline.execution.mode";
    public static final String SNAPSHOT_ENABLED = "pipeline.snapshot.enabled";
    public static final String CURRENT_PLAN_ID = "pipeline.current.planId";
    public static final String CURRENT_RUN_ID = "pipeline.current.runId";
    public static final String SNAPSHOT_SCN = "pipeline.snapshot.scn";

    private PlanRuntimeArgs() {
    }
}
