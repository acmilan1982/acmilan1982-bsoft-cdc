package com.bsoft.common.bean;

import com.bsoft.common.enums.JobStartupMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


/**
 * ClassName: ClientInfo
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/7 - 15:51
 */
@Component
@ConfigurationProperties(prefix = "client-info")
public class ClientInfo extends  SimpleClientInfo implements Serializable {

    private static final Logger logger = LogManager.getLogger(ClientInfo.class);


    private JobStartupMode jobStartupMode;

    // zk中job状态路径
    private String clientStatusPath;

    // zk中job信号路径
    private String stopSignalPath;

    // zk中客户端ip路径
    private String clientIpPath;

    // zk中客户端当前scn路径
    private String clientScnPath;

    // 检查当前业务库是否满足CDC的先决条件
    private boolean oracleCdcPreRequisites;

    // 检查当前业务库是否满足CDC的先决条件
    private boolean tableNamingStrategy;

    public ClientInfo()  {
        logger.info("ClientInfo实例化");
    }

    @PostConstruct
    public void init() {
        // job状态路径
        clientStatusPath = "/clients/" + getClientId() + "/status";
        // job信号路径
        stopSignalPath = "/clients/" + getClientId() + "/signals";
        // 客户端ip路径
        clientIpPath = "/clients/" + getClientId() + "/ip";
        // 客户端当前scn路径
        clientScnPath = "/clients/" + getClientId() + "/scn";


    }




    public JobStartupMode getJobStartupMode() {
        return jobStartupMode;
    }

    public void setJobStartupMode(JobStartupMode jobStartupMode) {
        this.jobStartupMode = jobStartupMode;
    }




    public String getStopSignalPath() {
        return stopSignalPath;
    }

    public void setStopSignalPath(String stopSignalPath) {
        this.stopSignalPath = stopSignalPath;
    }

    public String getClientStatusPath() {
        return clientStatusPath;
    }

    public void setClientStatusPath(String clientStatusPath) {
        this.clientStatusPath = clientStatusPath;
    }

    public String getClientIpPath() {
        return clientIpPath;
    }

    public void setClientIpPath(String clientIpPath) {
        this.clientIpPath = clientIpPath;
    }





    public String getClientScnPath() {
        return clientScnPath;
    }

    public void setClientScnPath(String clientScnPath) {
        this.clientScnPath = clientScnPath;
    }

    public boolean isOracleCdcPreRequisites() {
        return oracleCdcPreRequisites;
    }

    public void setOracleCdcPreRequisites(boolean oracleCdcPreRequisites) {
        this.oracleCdcPreRequisites = oracleCdcPreRequisites;
    }


}
