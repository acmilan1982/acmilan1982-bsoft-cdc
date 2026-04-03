package com.bsoft.common.pipeline.model;

import java.util.List;

/**
 * ClassName: RulePlan
 * Package:   com.bsoft.common.pipeline.model
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 17:23
 */
public class RulePlan {
    private String planId;
    private String outputTopic;
    private List<PlanNodeSpec> nodes;

    public String getPlanId() { return planId; }
    public void setPlanId(String planId) { this.planId = planId; }

    public String getOutputTopic() { return outputTopic; }
    public void setOutputTopic(String outputTopic) { this.outputTopic = outputTopic; }

    public List<PlanNodeSpec> getNodes() { return nodes; }
    public void setNodes(List<PlanNodeSpec> nodes) { this.nodes = nodes; }
}
