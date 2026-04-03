package com.bsoft.client.function;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * ClassName: OracleSourceLog
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/5/7 - 14:26
 */
public class OracleSourceLogRichMapFunction extends RichMapFunction<String,String> {

    private static final Logger logger = LogManager.getLogger(OracleSourceLogRichMapFunction.class);

    private transient long lastPrintTime;
    private transient long lastEventTime;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final ClientInfo clientInfo;

    // 全局 ZooKeeper 节点管理器
    private transient GlobalZooKeeperManager globalZooKeeperManager;

    private String scn = "-1";

    private long recordCount = 0L;

    public OracleSourceLogRichMapFunction(ClientInfo clientInfo) throws Exception {
        this.clientInfo = clientInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(clientInfo);
    }



    // scn更新到zk上
    @Override
    public String map(String value) throws Exception {



        logger.info(" 输出到kafka消息数量: {}", ++recordCount);
        if(recordCount % 10000 == 0){

            logger.info(" [Kafka Sink Preview]: {}", value);
        }
//
//        ++recordCount;

        JsonNode root = mapper.readTree(value);
        JsonNode sourceNode = root.path("source");
        if (sourceNode.has("scn")) {
            scn = sourceNode.get("scn").asText();
        }
        globalZooKeeperManager.updateScn("scn",Long.parseLong(scn));
        return value;
    }



}
