package com.bsoft.common.bean;

import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;


@Component
@ConfigurationProperties(prefix = "log-server-info")
public class LogServerInfo extends  BaseInfo  implements Serializable {

    private static final Logger logger = LogManager.getLogger(LogServerInfo.class);
    // 服务端ID，每个服务端都有唯一的id
    private String logServerId;


    // 配置库数据源
    @Autowired
    private ConfigDataSourceConfig configDataSourceConfig;


    private JobStatus jobStatus;


    public LogServerInfo() {

    }


    @PostConstruct
    private void initZKPath(){

    }


    public String getLogServerId() {
        return logServerId;
    }

    public void setLogServerId(String logServerId) {
        this.logServerId = logServerId;
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
}
