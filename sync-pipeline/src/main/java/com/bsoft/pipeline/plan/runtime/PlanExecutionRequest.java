package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.RulePlan;

/**
 * ClassName: PlanExecutionRequest
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:17
 */
public class PlanExecutionRequest {
    private final RulePlan plan;
    private final String runId;

    public PlanExecutionRequest(RulePlan plan, String runId) {
        if (plan == null) {
            throw new IllegalArgumentException("plan is null");
        }
        if (plan.getPlanId() == null || plan.getPlanId().trim().isEmpty()) {
            throw new IllegalArgumentException("planId is blank");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId is blank");
        }
        this.plan = plan;
        this.runId = runId.trim();
    }

    public RulePlan getPlan() {
        return plan;
    }

    public String getRunId() {
        return runId;
    }
}