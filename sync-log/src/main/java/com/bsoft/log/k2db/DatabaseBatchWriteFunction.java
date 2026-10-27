package com.bsoft.log.k2db;
import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.LogInfo;


import com.bsoft.log.function.LogCollector;
import com.bsoft.log.global.GlobalLogCollectorManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// 每一个主题，即每一个源表，对应一个 DatabaseRichProcessFunction 实例
public class DatabaseBatchWriteFunction extends ProcessFunction<LogKafkaMessage, Void> {


    private static final Logger logger = LogManager.getLogger(DatabaseBatchWriteFunction.class);


    private LogInfo logInfo;

    private transient LogCollector logCollector;



    // 每个DatabaseRichProcessFunction，只写入指定目标库中的指定表(只会写入一个表)
    public DatabaseBatchWriteFunction(LogInfo logInfo) {
        this.logInfo = logInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        // 获取当前 logId 对应的全局 LogCollector。
        this.logCollector = GlobalLogCollectorManager.getInstance(logInfo);

    }




    @Override
    public void processElement(LogKafkaMessage message, Context ctx, Collector<Void> out) throws Exception {
        this.logCollector.add(message);
    }



}
