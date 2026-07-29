package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanNode;

import com.bsoft.pipeline.plan.node.FunctionCallingPlanNode;

public class FunctionCallingPlanNodeCreator extends AbstractPlanNodeCreator{

    public FunctionCallingPlanNodeCreator(PlanContext planContext) {
        super(planContext);
    }
    @Override
    public PlanNode create(PlanNodeSpec spec) {
        return new FunctionCallingPlanNode(spec.getId(), spec.getInputs(), spec.getOutput(), spec.getParams(), this.getContext());
    }
}

