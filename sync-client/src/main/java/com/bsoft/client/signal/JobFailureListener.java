package com.bsoft.client.signal;

// Job 失败事件监听器，只表达“某个 dataSourceId 对应的 Flink job 已经失败”这个事实。
public interface JobFailureListener {

    // 当 Flink job 确认 FAILED 后触发，failedJobId 用于避免旧回调误清新 job。
    void onJobFailed(String dataSourceId, String failedJobId, Throwable throwable);
}