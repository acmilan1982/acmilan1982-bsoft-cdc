package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanNode;
import com.bsoft.pipeline.plan.node.SqlPlanNode;
import com.bsoft.pipeline.plan.spi.PlanNodeCreator;

public class SqlPlanNodeCreator  extends AbstractPlanNodeCreator{
    public SqlPlanNodeCreator(PipelineInitContext initContext) {
        super(initContext);
    }

    @Override
    public PlanNode create(PlanNodeSpec spec) {
        return new SqlPlanNode(spec.getId(), spec.getInputs(), spec.getOutput(), spec.getParams(), initContext);
    }
}

