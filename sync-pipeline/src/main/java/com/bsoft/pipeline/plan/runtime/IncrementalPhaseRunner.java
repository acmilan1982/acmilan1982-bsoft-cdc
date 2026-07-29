package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;

import java.util.List;

/**
 * 增量阶段执行器。
 *
 * 新语义：
 * - 不再负责单个 plan 的增量执行
 * - 而是一次性接收一批已经完成 snapshot 的 plan
 * - 在同一个 Flink env / tableEnv 中组装并提交一个总 incremental job
 *
 * 这样可以满足：
 * 1. 老 plan 重启后继续断点续传
 * 2. 新 plan snapshot 完成后并入总增量
 * 3. 整个 sync-pipeline 只有一个长期运行的 incremental job
 */
public interface IncrementalPhaseRunner {

    /**
     * 在一个总 incremental job 中执行全部可增量 plan。
     *
     * 约束：
     * - plans 和 states 一一对应
     * - 每个 state 都应该已有 snapshotScn
     * - 每个 state 至少应处于可进入增量的 phase
     */
    void run(List<RulePlan> plans, List<PipelinePlanRunState> states) throws Exception;
}