package com.bsoft.pipeline.plan;


import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.pipeline.plan.node.SnapshotSqlPlanNode;

public class PlanExecutor {
    private final PlanNodeFactory factory;

    public PlanExecutor(PlanNodeFactory factory) {
        this.factory = factory;
    }

    public void execute(RulePlan plan, PlanContext context) {
        if (plan == null || plan.getNodes() == null) {
            throw new IllegalArgumentException("RulePlan or nodes is null");
        }

        context.putRuntimeArg(SnapshotSqlPlanNode.CURRENT_PLAN_ID_RUNTIME_ARG, plan.getPlanId());

        // 遍历plan中的所有 Node
        for (PlanNodeSpec spec : plan.getNodes()) {
            // 使用 PlanNodeFactory 创建每个节点
            PlanNode node = factory.create(spec);
            node.validate(context);
            // 执行节点并存储结果
            context.putDataset(node.output(), node.apply(context));
        }
    }
}

