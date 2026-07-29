package com.bsoft.pipeline.plan.node;

import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.pipeline.functions.FunctionCallingMapFunction;
import com.bsoft.pipeline.plan.PlanContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

public class FunctionCallingPlanNode extends AbstractPlanNode {

    public FunctionCallingPlanNode(String id, List<String> inputs, String output,
                                   Map<String, Object> params, PlanContext planContext) {
        super(id, PlanNodeType.FUNCTION_CALLING, inputs, output, params, planContext);
    }

    @Override
    public Table apply(PlanContext context) {
        if (inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("FUNCTION_CALLING requires one input");
        }

        Table input = context.getDataset(inputs.get(0));
        if (input == null) {
            throw new IllegalArgumentException("FUNCTION_CALLING input not found: " + inputs.get(0));
        }

        String httpUrl = required("httpUrl");
        List<String> inCols = input.getResolvedSchema().getColumnNames();

        DataStream<Row> inStream = context.tableEnv().toChangelogStream(input);

        SingleOutputStreamOperator<Row> outStream = inStream
                .map(new FunctionCallingMapFunction(inCols, httpUrl))
                .returns(inStream.getType());

        Schema outSchema = Schema.newBuilder()
                .fromResolvedSchema(input.getResolvedSchema())
                .build();

        return context.tableEnv().fromChangelogStream(outStream, outSchema, ChangelogMode.all());
    }

    private String required(String key) {
        Object v = params.get(key);
        if (v == null || String.valueOf(v).trim().isEmpty()) {
            throw new IllegalArgumentException("FUNCTION_CALLING missing param: " + key);
        }
        return String.valueOf(v).trim();
    }
}

