package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanNode;
import com.bsoft.pipeline.plan.node.SqlPlanNode;

public class SqlPlanNodeCreator  extends AbstractPlanNodeCreator{
    public SqlPlanNodeCreator(PlanContext planContext) {
        super(planContext);
    }

    @Override
    public PlanNode create(PlanNodeSpec spec) {
        return new SqlPlanNode(spec.getId(), spec.getInputs(), spec.getOutput(), spec.getParams(), this.getContext());
    }
}

