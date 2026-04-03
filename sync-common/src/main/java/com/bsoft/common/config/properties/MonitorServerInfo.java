package com.bsoft.common.config.properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;


@Component
@ConfigurationProperties(prefix = "monitor-server-info")
public class MonitorServerInfo extends  ServerInfo  implements Serializable {

    private static final Logger logger = LogManager.getLogger(MonitorServerInfo.class);
    // 服务端ID，每个服务端都有唯一的id
    private String monitorServerId;



    public MonitorServerInfo() {
        logger.info("MonitorServerInfo-------------->实例化");
    }


    @PostConstruct
    private void initZKPath(){
       setMonitorDataPath( "/servers/%s/monitor_data");
    }


    public String getMonitorServerId() {
        return monitorServerId;
    }

    public void setMonitorServerId(String monitorServerId) {
        this.monitorServerId = monitorServerId;
        // Keep base serverId in sync, so common logic that depends on ServerInfo works.
        super.setServerId(monitorServerId);
    }
}
