package com.bsoft.log.k2db;
import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.LogServerInfo;
import com.bsoft.common.global.GlobalLogCollectorManager;
import com.bsoft.common.log.LogCollector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


// 每一个主题，即每一个源表，对应一个 DatabaseRichProcessFunction 实例
public class DatabaseBatchWriteFunction extends ProcessFunction<SyncLogEntry, Void> {


    private static final Logger logger = LogManager.getLogger(DatabaseBatchWriteFunction.class);


    private LogServerInfo logServerInfo;

//    // 移除原有的 LogCollector logCollector 字段
    private transient LogCollector logCollector;



    // 每个DatabaseRichProcessFunction，只写入指定目标库中的指定表(只会写入一个表)
    public DatabaseBatchWriteFunction(LogServerInfo logServerInfo) {
        this.logServerInfo = logServerInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        // 11.使用全局共享的 LogCollector
        this.logCollector = GlobalLogCollectorManager.getInstance(
                this.logServerInfo.getConfigDataSourceConfig()
        );

    }




    @Override
    public void processElement(SyncLogEntry syncLogEntry, Context ctx, Collector<Void> out) throws Exception {
        this.logCollector.add(syncLogEntry);
    }



}
