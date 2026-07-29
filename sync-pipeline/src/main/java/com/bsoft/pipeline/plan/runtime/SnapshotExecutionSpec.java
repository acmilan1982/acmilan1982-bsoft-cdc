package com.bsoft.pipeline.plan.runtime;

/**
 * ClassName: SnapshotExecutionSpec
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:16
 */
public class SnapshotExecutionSpec {
    private final String planId;
    private final String runId;
    private final long snapshotScn;

    public SnapshotExecutionSpec(String planId, String runId, long snapshotScn) {
        this.planId = require(planId, "planId");
        this.runId = require(runId, "runId");
        this.snapshotScn = snapshotScn;
    }

    public String getPlanId() {
        return planId;
    }

    public String getRunId() {
        return runId;
    }

    public long getSnapshotScn() {
        return snapshotScn;
    }

    private static String require(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }
}