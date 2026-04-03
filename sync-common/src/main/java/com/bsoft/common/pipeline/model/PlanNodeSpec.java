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
public class PlanNodeSpec {
    private String id;
    private PlanNodeType type;
    private List<String> inputs;
    private String output;
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
