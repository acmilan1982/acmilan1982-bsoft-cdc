package com.bsoft.pipeline.plan;

import com.bsoft.common.pipeline.PlanNodeType;
import org.apache.flink.table.api.Table;

import java.util.List;

public interface PlanNode {
    String id();

    PlanNodeType type();

    List<String> inputs();

    String output();

    void validate(PlanContext context);

    Table apply(PlanContext context);
}

