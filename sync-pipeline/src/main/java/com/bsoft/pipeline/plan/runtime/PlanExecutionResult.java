package com.bsoft.pipeline.plan.runtime;

import com.bsoft.pipeline.plan.state.PipelinePlanPhase;

/**
 * ClassName: PlanExecutionResult
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:17
 */
public class PlanExecutionResult {
    private final String planId;
    private final String runId;
    private final PipelinePlanPhase finalPhase;
    private final Long snapshotScn;

    public PlanExecutionResult(String planId, String runId, PipelinePlanPhase finalPhase, Long snapshotScn) {
        this.planId = planId;
        this.runId = runId;
        this.finalPhase = finalPhase;
        this.snapshotScn = snapshotScn;
    }

    public String getPlanId() {
        return planId;
    }

    public String getRunId() {
        return runId;
    }

    public PipelinePlanPhase getFinalPhase() {
        return finalPhase;
    }

    public Long getSnapshotScn() {
        return snapshotScn;
    }
}