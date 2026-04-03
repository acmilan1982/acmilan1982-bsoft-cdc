package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.spi.PlanNodeCreator;

/**
 * ClassName: AbstractPlanNodeCreator
 * Package:   com.bsoft.pipeline.plan.spi.creator
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/12 - 18:38
 */
public abstract class AbstractPlanNodeCreator implements PlanNodeCreator {
    protected final PipelineInitContext initContext;

    protected AbstractPlanNodeCreator(PipelineInitContext initContext) {
        this.initContext = (initContext == null) ? PipelineInitContext.empty() : initContext;
    }
}