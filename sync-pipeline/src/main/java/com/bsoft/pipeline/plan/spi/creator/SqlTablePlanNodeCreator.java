package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanNode;
import com.bsoft.pipeline.plan.node.SinkTablePlanNode;
import com.bsoft.pipeline.plan.node.SqlPlanNode;

public class SqlTablePlanNodeCreator extends AbstractPlanNodeCreator{
    public SqlTablePlanNodeCreator(PipelineInitContext initContext) {
        super(initContext);
    }

    @Override
    public PlanNode create(PlanNodeSpec spec) {
        return new SinkTablePlanNode(spec.getId(), spec.getInputs(), spec.getOutput(), spec.getParams(), initContext);
    }
}

