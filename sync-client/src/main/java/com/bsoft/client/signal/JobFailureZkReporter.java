package com.bsoft.client.signal;

import com.bsoft.common.config.properties.ClientInfo;
import org.apache.flink.core.execution.JobClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

// 负责监听 Flink Job 失败，并把失败事件交给上层调度器处理。
public class JobFailureZkReporter {

    // 日志对象。
    private static final Logger logger = LogManager.getLogger(JobFailureZkReporter.class);

    // 当前客户端配置。
    private final ClientInfo clientInfo;

    // Job 失败事件监听器，负责把失败事件交给调度层。
    private final JobFailureListener failureListener;

    // 创建 Job 失败监听器。
    public JobFailureZkReporter(ClientInfo clientInfo, JobFailureListener failureListener) {
        // 保存客户端配置。
        this.clientInfo = clientInfo;

        // 保存失败事件监听器。
        this.failureListener = failureListener;
    }

    // 为指定 job 注册失败监听。
    public void watch(String dataSourceId, JobClient jobClient) {
        // 记录当前 watch 绑定的 jobId，后续用于旧 job 身份校验。
        String watchedJobId = jobClient.getJobID().toString();

        // 监听 Flink job 的最终执行结果。
        jobClient.getJobExecutionResult().whenComplete((result, throwable) -> {
            // 正常结束时不触发失败处理。
            if (throwable == null) {
                return;
            }

            // 只有 Flink job 明确 FAILED 时才触发失败处理。
            if (!isFailedJob(dataSourceId, jobClient)) {
                return;
            }

            // 记录失败事件日志。
            logger.error("Flink job failed, notify failure listener. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, watchedJobId, throwable);



            // 通知上层调度器处理失败重启。
            notifyFailureListener(dataSourceId, watchedJobId, throwable);
        });
    }

    // 通知失败监听器，监听器异常只记录日志，不向 Flink 回调线程外抛。
    private void notifyFailureListener(String dataSourceId, String failedJobId, Throwable throwable) {
        // 没有监听器时只记录日志并返回。
        if (failureListener == null) {
            logger.warn("Job failure listener is null, skip restart notification. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);
            return;
        }

        try {
            // 把失败事件交给调度层，调度层负责 backoff、重启和 ZK 状态写入。
            failureListener.onJobFailed(dataSourceId, failedJobId, throwable);
        } catch (Exception e) {
            // 防止调度层异常污染 Flink callback 线程。
            logger.error("Failed to notify job failure listener. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId, e);
        }
    }

    // 判断当前 Flink job 是否确认 FAILED。
    private boolean isFailedJob(String dataSourceId, JobClient jobClient) {
        try {
            // 查询 Flink job 当前状态。
            org.apache.flink.api.common.JobStatus status =
                    jobClient.getJobStatus().get(5, TimeUnit.SECONDS);

            // 只接受 FAILED 状态。
            return status == org.apache.flink.api.common.JobStatus.FAILED;
        } catch (Exception e) {
            // 无法确认 FAILED 时不触发重启，避免误判。
            logger.error("Failed to check Flink job status, skip failure handling. clientId={}, dataSourceId={}",
                    clientInfo.getClientId(), dataSourceId, e);
            return false;
        }
    }
}