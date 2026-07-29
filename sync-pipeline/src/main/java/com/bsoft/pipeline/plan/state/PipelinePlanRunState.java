package com.bsoft.pipeline.plan.state;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Objects;

public class PipelinePlanRunState implements Serializable {
    private String planId;
    private String runId;
    private PipelinePlanPhase phase;
    private Long snapshotScn;
    private LocalDateTime snapshotStartTime;
    private LocalDateTime snapshotEndTime;
    private LocalDateTime incrementalStartTime;
    private String lastError;

    public static PipelinePlanRunState initSnapshotRunning(String planId, String runId) {
        // 首次插入记录时，直接进入 SNAPSHOT_RUNNING
        PipelinePlanRunState state = new PipelinePlanRunState();
        state.setPlanId(planId);
        state.setRunId(runId);
        state.setPhase(PipelinePlanPhase.SNAPSHOT_RUNNING);
        state.setSnapshotScn(null);
        state.setSnapshotStartTime(null);
        state.setSnapshotEndTime(null);
        state.setIncrementalStartTime(null);
        state.setLastError(null);
        return state;
    }

    public boolean shouldRunSnapshot() {
        // RUNNING 表示上次可能中断，FAILED 表示上次失败，都需要重新快照
        return phase == PipelinePlanPhase.SNAPSHOT_RUNNING
                || phase == PipelinePlanPhase.SNAPSHOT_FAILED;
    }

    public boolean isEligibleForIncremental() {
        // 已完成快照或已经进入过增量，都不再回退到快照
        return phase == PipelinePlanPhase.SNAPSHOT_DONE
                || phase == PipelinePlanPhase.INCREMENTAL_RUNNING
                || phase == PipelinePlanPhase.INCREMENTAL_FAILED;
    }

    public void beginSnapshotAttempt(long snapshotScn) {
        // 每次快照尝试都覆盖旧 SCN，不复用旧 snapshotScn
        setPhase(PipelinePlanPhase.SNAPSHOT_RUNNING);
        setSnapshotScn(snapshotScn);
        setSnapshotStartTime(LocalDateTime.now());
        setSnapshotEndTime(null);
        setLastError(null);
    }

    public void markSnapshotDone() {
        // 快照完成后，snapshotScn 才成为有效增量基线
        setPhase(PipelinePlanPhase.SNAPSHOT_DONE);
        setSnapshotEndTime(LocalDateTime.now());
        setLastError(null);
    }

    public void markSnapshotFailed(String error) {
        // 快照失败后保留本次尝试 SCN，但下次启动会覆盖它
        setPhase(PipelinePlanPhase.SNAPSHOT_FAILED);
        setLastError(error);
    }

    public void markIncrementalRunning() {
        // 进入增量后，不再回退到快照
        setPhase(PipelinePlanPhase.INCREMENTAL_RUNNING);
        setIncrementalStartTime(LocalDateTime.now());
        setLastError(null);
    }

    public void markIncrementalFailed(String error) {
        // 增量失败后，后续按增量消费 SCN 恢复
        setPhase(PipelinePlanPhase.INCREMENTAL_FAILED);
        setLastError(error);
    }

    public String getPlanId() {
        return planId;
    }

    public void setPlanId(String planId) {
        this.planId = trim(planId);
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = trim(runId);
    }

    public PipelinePlanPhase getPhase() {
        return phase;
    }

    public void setPhase(PipelinePlanPhase phase) {
        this.phase = Objects.requireNonNull(phase, "phase");
    }

    public Long getSnapshotScn() {
        return snapshotScn;
    }

    public void setSnapshotScn(Long snapshotScn) {
        this.snapshotScn = snapshotScn;
    }

    public LocalDateTime getSnapshotStartTime() {
        return snapshotStartTime;
    }

    public void setSnapshotStartTime(LocalDateTime snapshotStartTime) {
        this.snapshotStartTime = snapshotStartTime;
    }

    public LocalDateTime getSnapshotEndTime() {
        return snapshotEndTime;
    }

    public void setSnapshotEndTime(LocalDateTime snapshotEndTime) {
        this.snapshotEndTime = snapshotEndTime;
    }

    public LocalDateTime getIncrementalStartTime() {
        return incrementalStartTime;
    }

    public void setIncrementalStartTime(LocalDateTime incrementalStartTime) {
        this.incrementalStartTime = incrementalStartTime;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = trim(lastError);
    }

    private String trim(String value) {
        return value == null ? null : value.trim();
    }
}