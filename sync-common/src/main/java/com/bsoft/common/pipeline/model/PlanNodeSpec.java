package com.bsoft.common.pipeline.model;

import com.bsoft.common.pipeline.PlanNodeType;

import java.util.List;
import java.util.Map;

/**
 * ClassName: PlanNodeSpec
 * Package:   com.bsoft.common.pipeline.model
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 17:23
 */

// 单个节点
public class PlanNodeSpec {
    // 节点id
    private String id;
    // 节点类型
    private PlanNodeType type;
    // 节点的输入id
    private List<String> inputs;
    // 节点的输出id
    private String output;
    // 节点的参数
    private Map<String, Object> params;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public PlanNodeType getType() { return type; }
    public void setType(PlanNodeType type) { this.type = type; }

    public List<String> getInputs() { return inputs; }
    public void setInputs(List<String> inputs) { this.inputs = inputs; }

    public String getOutput() { return output; }
    public void setOutput(String output) { this.output = output; }

    public Map<String, Object> getParams() { return params; }
    public void setParams(Map<String, Object> params) { this.params = params; }
}
