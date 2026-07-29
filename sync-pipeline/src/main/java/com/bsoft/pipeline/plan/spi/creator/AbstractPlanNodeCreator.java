package com.bsoft.pipeline.plan.spi.creator;

import com.bsoft.pipeline.plan.PlanContext;
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
    protected  PlanContext context ;

    protected AbstractPlanNodeCreator(PlanContext context ) {
        this.context = context;
    }


    public PlanContext getContext() {
        return context;
    }

    public void setContext(PlanContext context) {
        this.context = context;
    }
}