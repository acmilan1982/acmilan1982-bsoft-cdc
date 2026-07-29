package com.bsoft.pipeline.plan.node;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.plan.PlanContext;
import com.bsoft.pipeline.plan.PlanNode;
import org.apache.flink.table.api.Table;

import java.util.List;
import java.util.Map;

public abstract class AbstractPlanNode implements PlanNode {
    protected  String id;
    protected  PlanNodeType type;
    protected  List<String> inputs;
    protected  String output;
    protected  Map<String, Object> params;
    protected  PlanContext planContext;


    public AbstractPlanNode() {

    }

    protected AbstractPlanNode(String id,
                               PlanNodeType type,
                               List<String> inputs,
                               String output,
                               Map<String, Object> params,
                               PlanContext planContext) {
        this.id = id;
        this.type = type;
        this.inputs = inputs;
        this.output = output;
        this.params = params;
        this.planContext = planContext;
    }

    @Override
    public String id() { return id; }

    @Override
    public PlanNodeType type() { return type; }

    @Override
    public List<String> inputs() { return inputs; }

    @Override
    public String output() { return output; }

    @Override
    public void validate(PlanContext context) { }

    @Override
    public abstract Table apply(PlanContext context);
}
