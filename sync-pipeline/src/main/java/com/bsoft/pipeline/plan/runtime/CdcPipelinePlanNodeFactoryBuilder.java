package com.bsoft.pipeline.plan.runtime;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.plan.PipelineInitContext;
import com.bsoft.pipeline.plan.PlanNodeFactory;
import com.bsoft.pipeline.plan.spi.creator.FunctionCallingPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SnapshotSqlPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SqlPlanNodeCreator;
import com.bsoft.pipeline.plan.spi.creator.SqlTablePlanNodeCreator;

public final class CdcPipelinePlanNodeFactoryBuilder {
    private CdcPipelinePlanNodeFactoryBuilder() {
    }

    public static PlanNodeFactory build() {
        return build(PipelineInitContext.empty());
    }

    public static PlanNodeFactory build(PipelineInitContext initContext) {
        PipelineInitContext ctx = (initContext == null) ? PipelineInitContext.empty() : initContext;

        PlanNodeFactory factory = new PlanNodeFactory();
        factory.register(PlanNodeType.SQL, new SqlPlanNodeCreator(ctx));
        factory.register(PlanNodeType.FUNCTION_CALLING, new FunctionCallingPlanNodeCreator(ctx));
        factory.register(PlanNodeType.SINK_TABLE, new SqlTablePlanNodeCreator(ctx));
        return factory;
    }

    public static PlanNodeFactory buildSnapshotFactory(PipelineInitContext initContext) {
        PipelineInitContext ctx = (initContext == null) ? PipelineInitContext.empty() : initContext;

        PlanNodeFactory factory = new PlanNodeFactory();
        factory.register(PlanNodeType.SQL, new SnapshotSqlPlanNodeCreator(ctx));
        factory.register(PlanNodeType.FUNCTION_CALLING, new FunctionCallingPlanNodeCreator(ctx));
        factory.register(PlanNodeType.SINK_TABLE, new SqlTablePlanNodeCreator(ctx));
        return factory;
    }
}

