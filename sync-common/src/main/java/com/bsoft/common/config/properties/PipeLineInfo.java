package com.bsoft.common.config.properties;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.SubscriberMappingRegistry;
import com.bsoft.common.bean.TargetDataSourceConfigRegistry;
import com.bsoft.common.enums.JobStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;


@Component
@ConfigurationProperties(prefix = "pipeline-info")
public class PipeLineInfo extends BaseInfo implements Serializable {

    private static final Logger logger = LogManager.getLogger(PipeLineInfo.class);
    // 服务端ID，每个服务端都有唯一的id
    private String pipelineId;


    // 配置库数据源
    @Autowired
    private ConfigDataSourceConfig configDataSourceConfig;

    // 目标库数据源
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;



    // 订阅关系
    private SubscriberMappingRegistry subscriberMappingRegistry;



    private JobStatus jobStatus;

    // snapshot MiniCluster Web UI 端口
    private int snapshotFlinkUiPort;

    // incremental MiniCluster Web UI 端口
    private int incrementalFlinkUiPort;

    public PipeLineInfo() {
        logger.info("PipeLineInfo------------->实例化");
    }


    @PostConstruct
    private void initZKPath(){

    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public ConfigDataSourceConfig getConfigDataSourceConfig() {
        return configDataSourceConfig;
    }

    public void setConfigDataSourceConfig(ConfigDataSourceConfig configDataSourceConfig) {
        this.configDataSourceConfig = configDataSourceConfig;
    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry() {
        return targetDataSourceConfigRegistry;
    }

    public void setTargetDataSourceConfigRegistry(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
        this.targetDataSourceConfigRegistry = targetDataSourceConfigRegistry;
    }

    public SubscriberMappingRegistry getSubscriberMappingRegistry() {
        return subscriberMappingRegistry;
    }

    public void setSubscriberMappingRegistry(SubscriberMappingRegistry subscriberMappingRegistry) {
        this.subscriberMappingRegistry = subscriberMappingRegistry;
    }


    public int getSnapshotFlinkUiPort() {
        return snapshotFlinkUiPort;
    }

    public void setSnapshotFlinkUiPort(int snapshotFlinkUiPort) {
        this.snapshotFlinkUiPort = snapshotFlinkUiPort;
    }

    public int getIncrementalFlinkUiPort() {
        return incrementalFlinkUiPort;
    }

    public void setIncrementalFlinkUiPort(int incrementalFlinkUiPort) {
        this.incrementalFlinkUiPort = incrementalFlinkUiPort;
    }
}
