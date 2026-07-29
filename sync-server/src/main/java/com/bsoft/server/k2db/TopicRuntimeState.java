package com.bsoft.server.k2db;

// 单个 topic 在目标表 writer 内的运行态
public class TopicRuntimeState {

    // 当前 topic 的快照阶段是否已经结束
    private boolean snapshotPhaseEnded;

    // 当前 topic 最近一次可提交的最大 offset，该offset(包括)之前的数据，都已经被持久化
    private long latestOffsetCommitCandidate = -1L;

    // 当前 topic 最近一次已经输出到 offset side output 的最大 offset
    private long lastReportedOffsetCommitCandidate = -1L;

    // 判断快照阶段是否已经结束
    public boolean isSnapshotPhaseEnded() {
        // 返回快照阶段结束标记
        return snapshotPhaseEnded;
    }

    // 设置快照阶段结束标记
    public void setSnapshotPhaseEnded(boolean snapshotPhaseEnded) {
        // 保存快照阶段结束标记
        this.snapshotPhaseEnded = snapshotPhaseEnded;
    }

    // 获取最近一次可提交的最大 offset
    public long getLatestOffsetCommitCandidate() {
        // 返回可提交 offset
        return latestOffsetCommitCandidate;
    }

    // 推进当前 topic 的可提交 offset
    public void advanceLatestOffsetCommitCandidate(Long rowOffset) {
        // 空 offset 不参与推进
        if (rowOffset == null) {
            return;
        }

        // 只允许 offset 向前推进
        this.latestOffsetCommitCandidate = Math.max(this.latestOffsetCommitCandidate, rowOffset);
    }

    // 判断是否需要输出 offset commit 事件
    public boolean hasNewOffsetToReport() {
        // 有新 offset 时才需要输出
        return latestOffsetCommitCandidate >= 0L
                && latestOffsetCommitCandidate > lastReportedOffsetCommitCandidate;
    }

    // 获取下一次应该消费的 offset
    public long getNextOffsetToCommit() {
        // nextOffset 等于已处理最大 offset 加一
        return latestOffsetCommitCandidate + 1L;
    }

    // 标记当前 offset 已经输出到 side output
    public void markOffsetReported() {
        // 保存已经输出的最大 offset
        this.lastReportedOffsetCommitCandidate = this.latestOffsetCommitCandidate;
    }
}