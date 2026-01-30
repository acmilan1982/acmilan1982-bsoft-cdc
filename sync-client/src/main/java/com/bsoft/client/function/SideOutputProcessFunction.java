package com.bsoft.client.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: SideOutputProcessFunction
 * Package:   com.bsoft.client.o2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/13 - 13:52
 */
public class SideOutputProcessFunction extends ProcessFunction<String, String> {

    private static final Logger logger = LogManager.getLogger(SideOutputProcessFunction.class);

    // 侧输出标签映射
    Map<String, List<OutputTag<String>>> outputTagMap = new HashMap<>();

    public SideOutputProcessFunction(Map<String, List<OutputTag<String>>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }



    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(value);
            String fullyQualifiedTableName = rootNode.path("schema").asText() + "." + rootNode.path("table").asText();

            // 根据表名分发到对应的侧输出流
            List<OutputTag<String>> outputTags = outputTagMap.get(fullyQualifiedTableName);
            if (!outputTags.isEmpty()) {
                outputTags.stream().forEach(outputTag -> ctx.output(outputTag, value));
            } else {
                // 未匹配的数据输出到主流（可选）
                logger.debug("未找到匹配的表配置: {}", fullyQualifiedTableName);
            }
        } catch (Exception e) {
            logger.error("处理侧输出流数据时出错: {}", e.getMessage(), e);
        }
    }
}
