package com.bsoft.client.dispatcher;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.client.o2k.OracleToKafka;
import com.bsoft.common.enums.JobStartupMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * ClassName: FlinkJobManager
 * Package:   com.bsoft.client.manager
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 18:11
 */
@Component
public class ClientJobDispatcher implements CommandLineRunner {
    private static final Logger logger = LogManager.getLogger(ClientJobDispatcher.class);


    // 客户端信息
    @Autowired
    private ClientInfo clientInfo;


    @Autowired
    private OracleToKafka oracleToKafka;



    // 程序启动时，执行当前方法
    public void run(String... args)  {
        try {

            debug();
            oracleToKafka.init(this);
            oracleToKafka.startJob();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("重启 CDC 任务失败", e);
        }
    }


    public void debug( )  {
        logger.info("CLASSPATH = {}", System.getProperty("java.class.path"));
        logWhere(io.debezium.connector.oracle.logminer.LogMinerAdapter.class, "Which-LogMinerAdapter");
        logWhere(io.debezium.connector.oracle.StreamingAdapter.class, "Which-StreamingAdapter");
        logWhere(io.debezium.pipeline.EventDispatcher.class, "Which-EventDispatcher");

    }
    private void logWhere(Class<?> c, String tag){
        try {
            logger.info("{} -> {} @ {}", tag, c.getName(), c.getProtectionDomain().getCodeSource().getLocation());
        } catch (Throwable t) {
            System.out.println(tag + " -> " + c.getName() + " @ <unknown>");
        }
    }


    /**
     * 重启 CDC 作业 - 支持多种触发方式
     * @param reason 重启原因
     * @throws Exception
     */
    public synchronized void restartJob(String reason) throws Exception {
        logger.info("🔄 开始重启作业 - 原因: {}", reason);


        try {
            // 1. 停止当前作业
            stopCurrentJob();

            // 2. 等待作业完全停止
            waitForJobTermination();

            // 3. 清理状态
            clearJobState();

            // 4. 重新启动作业
            logger.info("重新启动作业...");
            run();

            logger.info("作业重启成功 - {}", reason);

        } catch (Exception e) {
            logger.error("作业重启失败: {}", e.getMessage(), e);
            throw new RuntimeException("重启 CDC 任务失败: " + reason, e);
        }
    }

    /**
     * 停止当前作业
     */
    private void stopCurrentJob() throws Exception {
        JobClient jobClient = oracleToKafka.getJobClient();

        if (jobClient != null) {
            try {
                // 检查作业状态
                org.apache.flink.api.common.JobStatus jobStatus =
                        jobClient.getJobStatus().get(5, java.util.concurrent.TimeUnit.SECONDS);

                if (!jobStatus.isGloballyTerminalState()) {
                    logger.info("正在停止当前作业...");

                    // 优雅停止作业
                    if (clientInfo.getJobStartupMode() == JobStartupMode.NORMAL) {
                        // 主任务使用 savepoint 停止
                        jobClient.stopWithSavepoint(true, null).get(60, java.util.concurrent.TimeUnit.SECONDS);
                        logger.info("主任务已通过 savepoint 停止");
                    } else {
                        // 快照任务直接取消
                        jobClient.cancel().get(30, java.util.concurrent.TimeUnit.SECONDS);
                        logger.info("快照任务已取消");
                    }
                } else {
                    logger.info("作业已处于终止状态: {}", jobStatus);
                }

            } catch (Exception e) {
                logger.warn("优雅停止失败，尝试强制取消: {}", e.getMessage());
                jobClient.cancel().get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
    }

    /**
     * 等待作业完全终止
     */
    private void waitForJobTermination() throws Exception {
        JobClient jobClient = oracleToKafka.getJobClient();

        if (jobClient == null) {
            return;
        }

        int maxWaitSeconds = 60;
        int waitInterval = 2;
        int waitCount = 0;

        while (waitCount < maxWaitSeconds / waitInterval) {
            try {
                org.apache.flink.api.common.JobStatus jobStatus =
                        jobClient.getJobStatus().get(5, java.util.concurrent.TimeUnit.SECONDS);

                if (jobStatus.isGloballyTerminalState()) {
                    logger.info("作业已完全停止，状态: {}", jobStatus);
                    return;
                }

                Thread.sleep(waitInterval * 1000);
                waitCount++;

            } catch (Exception e) {
                logger.warn("检查作业状态失败，假设已停止: {}", e.getMessage());
                return;
            }
        }

        logger.warn(" 等待作业停止超时，继续执行重启流程");
    }



    public synchronized void reloadJob() {
        try {
            JobClient jobClient = oracleToKafka.getJobClient();
            // cancel 当前 Flink job
            if (jobClient != null) {
                jobClient.cancel().get(); // 等待任务取消完成
            }

            // 清除当前job的内存状态
            clearJobState();
            // 重新运行当前job
            run();

        } catch (Exception e) {
            throw new RuntimeException("重启 CDC 任务失败", e);
        }
    }

    public synchronized void cancelJob() {
        try {
            JobClient jobClient = oracleToKafka.getJobClient();
            // cancel 当前 Flink job
            if (jobClient != null) {
                jobClient.cancel().get(); // 等待任务取消完成
            }

            // 清除当前job的内存状态
            clearJobState();
            // 重新运行当前job
            run();

        } catch (Exception e) {
            throw new RuntimeException("重启 CDC 任务失败", e);
        }
    }


    public ClientInfo getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }

    public  void clearJobState() throws Exception {
//        configFetcher.clearState();
        oracleToKafka.clearState();
    }

    public OracleToKafka getOracleToKafka() {
        return oracleToKafka;
    }

    public void setOracleToKafka(OracleToKafka oracleToKafka) {
        this.oracleToKafka = oracleToKafka;
    }
}
