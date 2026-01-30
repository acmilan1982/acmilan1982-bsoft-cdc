package com.bsoft.client.function;

/**
 * ClassName: SnapshotSignalSplitProcessFunction
 * Package:   com.bsoft.client.function
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/13 - 19:42
 */


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 快照信号分离处理函数
 * 用于将包含快照完成控制信号的数据流分离到侧输出流
 */
public class SnapshotSignalSplitProcessFunction extends ProcessFunction<String, String> {

    private static final Logger logger = LogManager.getLogger(SnapshotSignalSplitProcessFunction.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 侧输出标签，用于输出快照完成控制信号
    private final OutputTag<String> controlOutputTag;

    public SnapshotSignalSplitProcessFunction(OutputTag<String> controlOutputTag) {
        this.controlOutputTag = controlOutputTag;
    }

    @Override
    public void processElement(String json, Context ctx, Collector<String> out) throws Exception {
        try {
            JsonNode root = objectMapper.readTree(json);
            // 出现了控制信号，通常表示当前表的快照已经完成
            if(root.get("control_type") != null){
                // 输出到侧输出流（控制流）
                ctx.output(controlOutputTag, json);
                logger.debug("检测到快照完成信号，已分离到控制流");
            }else {
                // 输出到主流（数据流）
                out.collect(json);
            }
        } catch (Exception e) {
            logger.error(" 处理快照信号分离时出错: {}", e.getMessage(), e);
            // 发生异常时，仍然输出到主流，确保数据不丢失
            out.collect(json);
        }
    }
}