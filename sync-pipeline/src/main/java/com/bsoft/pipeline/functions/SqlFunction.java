package com.bsoft.pipeline.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.List;
/**
 * ClassName: SqlFunction
 * Package:   com.bsoft.pipeline.functions
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 18:32
 */


public class SqlFunction implements MapFunction<Row, String> {

    private final String nodeId;
    private final List<String> cols;

    public SqlFunction(String nodeId, List<String> cols) {
        this.nodeId = nodeId;
        this.cols = cols;
    }

    @Override
    public String map(Row r) {
        StringBuilder sb = new StringBuilder();
        sb.append("node=").append(nodeId)
                .append(", kind=").append(r.getKind().shortString())
                .append(", data={");
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(cols.get(i)).append("=").append(r.getField(i));
        }
        sb.append("}");
        return sb.toString();
    }
}