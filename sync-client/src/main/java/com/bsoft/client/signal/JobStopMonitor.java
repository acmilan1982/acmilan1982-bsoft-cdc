
package com.bsoft.client.signal;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.client.dispatcher.ClientJobDispatcher;
import org.apache.flink.core.execution.JobClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 作业停止监控器
 * 运行在客户端 JVM 中，负责监控外部停止信号并控制作业生命周期
 */
public class JobStopMonitor {

    private static final Logger logger = LogManager.getLogger(JobStopMonitor.class);

    private final ClientInfo clientInfo;

    private final String jobId;
    private JobClient jobClient;

    private ScheduledExecutorService monitorService;
    private DistributedStopSignalManager signalManager;
    private volatile boolean isRunning = false;

    private ClientJobDispatcher clientJobDispatcher;

    public JobStopMonitor(ClientInfo clientInfo, ClientJobDispatcher clientJobDispatcher) {
        this.clientInfo = clientInfo;

        this.jobId = clientInfo.getClientId();
        this.clientJobDispatcher = clientJobDispatcher;
    }

    /**
     * 启动监控
     */
    public void startMonitoring(JobClient jobClient) {
        this.jobClient = jobClient;

        if (isRunning) {
            return;
        }

        try {
            // 初始化信号管理器
            signalManager = new DistributedStopSignalManager(clientInfo);


            // 创建监控服务线程
            monitorService = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "job-stop-monitor-" + jobId);
                t.setDaemon(true);
                return t;
            });

            // 启动定期检查
            monitorService.scheduleWithFixedDelay(
                    this::checkAndHandleStopSignal,
                    10, // 初始延迟
                    3,  // 检查间隔
                    TimeUnit.SECONDS
            );

            isRunning = true;
            logger.info("作业停止监控器已启动 (JobID: {})", jobId);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("启动作业监控器失败", e);
        }
    }

    /**
     * 检查并处理停止信号
     */
    private void checkAndHandleStopSignal() {
        try {
            if (!isRunning || jobClient == null) {
                return;
            }

            // 检查作业是否还在运行
            org.apache.flink.api.common.JobStatus jobStatus =
                    jobClient.getJobStatus().get(2, TimeUnit.SECONDS);

            if (jobStatus.isGloballyTerminalState()) {
                logger.info("作业已自然结束，状态: {}", jobStatus);
                handleStopSignal("作业自然结束: " + jobStatus);
                return;
            }

            // 检查外部停止信号
            if (signalManager.checkStopSignal()) {
                String signalDetails = signalManager.getStopSignalDetails();
                logger.info("检测到停止信号: {}", signalDetails);
                handleStopSignal("检测到外部停止信号: " + signalDetails);
            }

        } catch (Exception e) {
            logger.error("检查停止信号失败: {}", e.getMessage(), e);
        }
    }


    /**
     * 处理停止信号并重启任务
     */
    private void handleStopSignal(String reason) {
        try {
            logger.info("📡 收到停止信号: {}，准备重启任务", reason);

            // 清理停止信号
            if (signalManager != null) {
                signalManager.clearStopSignal();
            }

            // 停止监控
            stopMonitoring();

            // 快照完成后处理
            clientJobDispatcher.getOracleToKafka().afterSnapshotCompletion();

            // 调用 ClientJobDispatcher 重启任务
            clientJobDispatcher.restartJob(reason);

            logger.info("✅ 任务重启请求已提交");

        } catch (Exception e) {
            logger.error("❌ 处理停止信号失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 停止监控
     */
    public void stopMonitoring() {
        if (!isRunning) {
            return;
        }

        logger.info("🛑 正在停止作业监控器...");

        isRunning = false;

        // 关闭监控服务
        if (monitorService != null && !monitorService.isShutdown()) {
            monitorService.shutdown();
            try {
                if (!monitorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    monitorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                monitorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 关闭信号管理器
        if (signalManager != null) {
            signalManager.close();
        }

        logger.info("🔒 作业监控器已停止");
    }
}