package com.bsoft.pipeline.functions;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
/**
 * ClassName: RowToKafkaJsonMapFunction
 * Package:   com.bsoft.pipeline.functions
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 18:10
 */
public class RowToKafkaJsonMapFunction implements MapFunction<Row, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final List<String> cols;

    public RowToKafkaJsonMapFunction(List<String> cols) {
        this.cols = cols;
    }

    @Override
    public String map(Row r) throws Exception {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("rowKind", r.getKind().shortString());
        for (int i = 0; i < cols.size(); i++) {
            obj.put(cols.get(i), r.getField(i));
        }
        return MAPPER.writeValueAsString(obj);
    }
}