package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanNodeFactory;
import com.bsoft.pipeline.plan.spi.creator.FunctionCallingPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SnapshotSqlPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SqlPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SqlTablePlanNodeCreator;

public final class PipelinePlanNodeFactoryBuilder {
    private PipelinePlanNodeFactoryBuilder() {
    }



    public static PlanNodeFactory build(PlanContext planContext) {


        PlanNodeFactory factory = new PlanNodeFactory();
        factory.register(PlanNodeType.SQL, new SqlPlanNodeCreator(planContext));
        factory.register(PlanNodeType.FUNCTION_CALLING, new FunctionCallingPlanNodeCreator(planContext));
        factory.register(PlanNodeType.SINK_TABLE, new SqlTablePlanNodeCreator(planContext));
        return factory;
    }

    public static PlanNodeFactory buildSnapshotFactory(PlanContext planContext) {


        PlanNodeFactory factory = new PlanNodeFactory();
        factory.register(PlanNodeType.SQL, new SnapshotSqlPlanNodeCreator(planContext));
        factory.register(PlanNodeType.FUNCTION_CALLING, new FunctionCallingPlanNodeCreator(planContext));
        factory.register(PlanNodeType.SINK_TABLE, new SqlTablePlanNodeCreator(planContext));
        return factory;
    }
}

