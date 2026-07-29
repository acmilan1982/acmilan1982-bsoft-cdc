package com.bsoft.pipeline.plan.state;

import java.util.Optional;

public interface PipelinePlanRunStateRepository {

    // 按 planId 查询当前状态
    Optional<PipelinePlanRunState> findByPlanId(String planId);

    // 首次注册 plan 状态，只 insert，不做 delete
    PipelinePlanRunState insert(PipelinePlanRunState state);

    // 后续状态推进只 update，不做 delete + insert
    PipelinePlanRunState update(PipelinePlanRunState state);
}