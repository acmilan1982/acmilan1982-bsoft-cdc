package com.bsoft.common.config.properties;

import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.enums.JobStatus;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.io.Serializable;


@Component
@ConfigurationProperties(prefix = "log-info")
public class LogInfo extends BaseInfo implements Serializable {

    private String logId;

    private String logTopic = "cdc.sync.server.logs.v1";

    private JobStatus jobStatus;

    /**
     * 原始消息存储策略
     配置来源: CDC_SERVER_CONFIG.raw-message-storage-strategy
     * 可选值: NONE(不插入) | PLAIN(不压缩) | COMPRESS(压缩,默认)
     */
    private RawMessageStorageStrategy rawMessageStorageStrategy = RawMessageStorageStrategy.NONE;


    public LogInfo() {
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public String getLogTopic() {
        return logTopic;
    }

    public void setLogTopic(String logTopic) {
        this.logTopic = logTopic;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }


    public RawMessageStorageStrategy getRawMessageStorageStrategy() {
        return rawMessageStorageStrategy;
    }

    public void setRawMessageStorageStrategy(RawMessageStorageStrategy rawMessageStorageStrategy) {
        this.rawMessageStorageStrategy = rawMessageStorageStrategy;
    }
}
