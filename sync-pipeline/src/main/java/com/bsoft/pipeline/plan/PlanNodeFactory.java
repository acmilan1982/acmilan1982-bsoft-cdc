package com.bsoft.pipeline.plan;


import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.pipeline.plan.spi.PlanNodeCreator;

import java.util.EnumMap;
import java.util.Map;

public class PlanNodeFactory {
    private final Map<PlanNodeType, PlanNodeCreator> creators = new EnumMap<>(PlanNodeType.class);

    public void register(PlanNodeType type, PlanNodeCreator creator) {
        creators.put(type, creator);
    }

    public PlanNode create(PlanNodeSpec spec) {
        PlanNodeCreator creator = creators.get(spec.getType());
        if (creator == null) {
            throw new IllegalArgumentException("Unsupported PlanNodeType: " + spec.getType());
        }
        return creator.create(spec);
    }
}

