package com.bsoft.pipeline.functions;

/**
 * ClassName: FunctionCallingMapFunction
 * Package:   com.bsoft.pipeline.functions
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 10:24
 */
import com.bsoft.pipeline.plan.runtime.HttpJsonClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将输入的 Flink Row 转为 HTTP JSON 请求，调用外部函数后，把响应写回新列 function_result。
 *
 * 设计目的：
 * 1. 把 FunctionCallingPlanNode 中较长的 map 逻辑抽离，降低节点类复杂度。
 * 2. 让“行转换 + 外部调用”逻辑可单独单元测试。
 * 3. 为后续扩展（超时、重试、打点、异常处理策略）预留清晰位置。
 *
 * 输入约定：
 * - 输入 Row 的字段顺序与 inCols 一一对应。
 * - 每个字段统一转成字符串（null 保持 null）后放入请求体 data。
 *
 * 输出约定：
 * - 输出 Row 会保留原 RowKind（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE）。
 * - 输出字段 = 原字段 + function_result（HTTP 返回字符串）。
 */
public class FunctionCallingMapFunction implements MapFunction<Row, Row> {


    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<String> inCols;
    private final String httpUrl;

    public FunctionCallingMapFunction(List<String> inCols, String httpUrl) {
        this.inCols = new ArrayList<>(inCols);
        this.httpUrl = httpUrl;
    }

    @Override
    public Row map(Row row) throws Exception {
        // 1) 组装请求（不把字段强转为 String）
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("rowKind", row.getKind().shortString());

        Map<String, Object> data = new LinkedHashMap<>();
        for (int i = 0; i < inCols.size(); i++) {
            data.put(inCols.get(i), row.getField(i)); // 保持原始类型
        }
        payload.put("data", data);

        // 2) 调外部 HTTP（只做副作用；如果需要可记录 resp）
        String reqBody = MAPPER.writeValueAsString(payload);
        HttpJsonClient.postJson(httpUrl, reqBody);

        // 3) 输出与输入完全同构（字段数/顺序/类型一致）
        Row out = new Row(row.getKind(), row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            out.setField(i, row.getField(i)); // 保持原值与类型
        }
        return out;
    }
}