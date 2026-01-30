
package com.bsoft.client.signal;

import com.bsoft.common.bean.ClientInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 集群感知的停止信号 Sink
 * 运行在 TaskManager 中，负责将停止信号发送到外部存储
 */
public class ClusterAwareStopSignalSink extends RichSinkFunction<String> {

    private static final Logger logger = LogManager.getLogger(ClusterAwareStopSignalSink.class);

    private transient DistributedStopSignalManager signalManager;

    private ClientInfo clientInfo;


    public ClusterAwareStopSignalSink(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
        logger.info("ClusterAwareStopSignalSink");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 初始化信号管理器
        signalManager = new DistributedStopSignalManager( clientInfo);

        logger.info(" TaskManager 中的停止信号 Sink 已初始化");
        logger.info(" TaskManager 信息: {}", EnvironmentInformation.getTemporaryFileDirectory());
//        logger.info("🏷️ 作业ID: {}", jobId);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        logger.warn(" TaskManager 收到作业停止信号: {}", value);

        // 解析停止信号类型并确定原因
        String reason = determineStopReason(value);

        // 发送停止信号到外部存储
        signalManager.sendStopSignal(reason);

        logger.info(" 停止信号已成功发送到 ZooKeeper");
        logger.info(" 等待客户端检测并停止作业...");
    }

    /**
     * 根据信号值确定停止原因
     */
    private String determineStopReason(String signalValue) {
        if (signalValue == null) {
            return "未知原因";
        }

        switch (signalValue.toUpperCase()) {
            case "SNAPSHOT_COMPLETE":
                return "所有表快照已完成";
            case "EXTERNAL_STOP_SIGNAL":
                return "接收到外部停止信号";
            case "STOP_JOB":
                return "作业请求停止";
            default:
                return "停止信号: " + signalValue;
        }
    }

    @Override
    public void close() throws Exception {
        if (signalManager != null) {
            signalManager.close();
            logger.info("🔒 停止信号管理器资源已清理");
        }
        super.close();
    }
}