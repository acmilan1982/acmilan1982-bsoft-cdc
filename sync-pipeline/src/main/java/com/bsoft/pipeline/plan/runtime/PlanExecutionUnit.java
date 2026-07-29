package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;

import java.util.Objects;

/**
 * ClassName: PlanExecutionUnit
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/5/13 - 13:33
 */

/**
 * plan 和它对应的运行状态绑定在一起，后面 snapshot / incremental 都按这个单元处理。
 */
public class PlanExecutionUnit {

    private final RulePlan plan;
    private final PipelinePlanRunState state;

    public PlanExecutionUnit(RulePlan plan, PipelinePlanRunState state) {
        this.plan = Objects.requireNonNull(plan, "plan");
        this.state = Objects.requireNonNull(state, "state");
    }

    public RulePlan getPlan() {
        return plan;
    }

    public PipelinePlanRunState getState() {
        return state;
    }

    public String getPlanId() {
        return state.getPlanId();
    }

    public String getRunId() {
        return state.getRunId();
    }
}
