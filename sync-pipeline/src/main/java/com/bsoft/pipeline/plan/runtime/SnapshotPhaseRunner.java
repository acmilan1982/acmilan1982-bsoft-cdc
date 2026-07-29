package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.plan.state.PipelinePlanRunState;

/**
 * ClassName: SnapshotPhaseRunner
 * Package:   com.bsoft.pipeline.plan.runtime
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:18
 */
public interface SnapshotPhaseRunner {
    void run(RulePlan plan, PipelinePlanRunState state) throws Exception;
}
