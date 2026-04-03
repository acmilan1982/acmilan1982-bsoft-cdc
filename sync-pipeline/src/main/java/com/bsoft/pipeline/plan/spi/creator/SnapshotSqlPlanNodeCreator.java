package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanNode;
import com.bsoft.pipeline.plan.node.SnapshotSqlPlanNode;

/**
 * ClassName: SnapshotSqlPlanNodeCreator
 * Package:   com.bsoft.pipeline.plan.spi.creator
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/3 - 17:33
 */
public class SnapshotSqlPlanNodeCreator extends AbstractPlanNodeCreator {

    public SnapshotSqlPlanNodeCreator(PipelineInitContext initContext) {
        super(initContext);
    }

    @Override
    public PlanNode create(PlanNodeSpec spec) {
        return new SnapshotSqlPlanNode(
                spec.getId(),
                spec.getInputs(),
                spec.getOutput(),
                spec.getParams(),
                initContext
        );
    }
}