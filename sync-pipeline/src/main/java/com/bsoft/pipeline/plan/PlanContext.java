package com.bsoft.pipeline.plan;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;

public class PlanContext {
    private final StreamExecutionEnvironment env;
    private final StreamTableEnvironment tableEnv;

    /*
    1. Node之间的数据交换，通过 datasetMap 来实现，
    2. datasetMap 用于存储每个节点的输出数据，以便后续节点可以访问和处理。
    3. 后继节点可以根据 datasetMap 获取前驱节点的输出数据，进行进一步的处理和计算。
 */
    private final Map<String, Table> datasetMap = new HashMap<>();
    private final Map<String, Object> serviceRegistry = new HashMap<>();
    private final Map<String, Object> runtimeArgs = new HashMap<>();

    public PlanContext(StreamTableEnvironment tableEnv) {
        this(null, tableEnv);
    }

    public PlanContext(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        this.env = env;
        this.tableEnv = tableEnv;
    }

    public StreamExecutionEnvironment env() {
        return env;
    }

    public StreamTableEnvironment tableEnv() {
        return tableEnv;
    }

    public Table getDataset(String nodeId) {
        return datasetMap.get(nodeId);
    }

    public void putDataset(String nodeId, Table table) {
        datasetMap.put(nodeId, table);
    }

    public void registerService(String key, Object service) {
        serviceRegistry.put(key, service);
    }

    public Object getService(String key) {
        return serviceRegistry.get(key);
    }

    public void putRuntimeArg(String key, Object value) {
        runtimeArgs.put(key, value);
    }

    public Object getRuntimeArg(String key) {
        return runtimeArgs.get(key);
    }

    public String getRuntimeArgAsString(String key) {
        Object v = runtimeArgs.get(key);
        if (v == null) {
            return null;
        }
        String s = String.valueOf(v).trim();
        return s.isEmpty() ? null : s;
    }
}