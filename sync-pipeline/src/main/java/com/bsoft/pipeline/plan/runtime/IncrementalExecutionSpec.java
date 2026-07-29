package com.bsoft.pipeline.plan.runtime;

/**
 * ClassName: IncrementalExecutionSpec
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:16
 */
public class IncrementalExecutionSpec {
    private final String planId;
    private final String runId;
    private final long startAfterScn;

    public IncrementalExecutionSpec(String planId, String runId, long startAfterScn) {
        this.planId = require(planId, "planId");
        this.runId = require(runId, "runId");
        this.startAfterScn = startAfterScn;
    }

    public String getPlanId() {
        return planId;
    }

    public String getRunId() {
        return runId;
    }

    public long getStartAfterScn() {
        return startAfterScn;
    }

    private static String require(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }
}
