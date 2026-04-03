package com.bsoft.pipeline.plan.spi;

import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.PlanNode;


public interface PlanNodeCreator {
    PlanNode create(PlanNodeSpec spec);
}

