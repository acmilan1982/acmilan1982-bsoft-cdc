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
    // plan id
    private String planId;


    // 当前 plan 最终输出到的目标表名，可有多个
    private List<String> outputTableNames;

    // 组成当前plan的节点
    private List<PlanNodeSpec> nodes;

    public String getPlanId() {
        return planId;
    }

    public void setPlanId(String planId) {
        this.planId = planId;
    }

    public List<String> getOutputTableNames() {
        return outputTableNames;
    }

    public void setOutputTableNames(List<String> outputTableNames) {
        this.outputTableNames = outputTableNames;
    }

    public List<PlanNodeSpec> getNodes() {
        return nodes;
    }

    public void setNodes(List<PlanNodeSpec> nodes) {
        this.nodes = nodes;
    }


}
