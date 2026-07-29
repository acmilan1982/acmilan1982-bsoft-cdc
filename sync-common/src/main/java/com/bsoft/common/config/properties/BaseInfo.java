package com.bsoft.common.config.properties;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.ZooKeeperConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ClassName: BaseInfo
 * Package:   com.bsoft.bean
 * Description:
 *
 * 说明：
 * 1. BaseInfo 必须实现 Serializable
 * 2. 否则子类在 Flink 中被序列化后，父类字段不会被带到 TaskManager 侧
 * 3. 当前 zooKeeperConfig / ip / serverPort / flinkUiPort 都定义在这里，因此必须可序列化
 */
public class BaseInfo implements Serializable {

    // 客户端/服务端的版本号
    protected String version;
    // 客户端/服务端的描述
    protected String description;
    // 客户端/服务端所在机器的ip地址(可能有多个网卡)，这个地址目前不起作业，仅作记录
    protected String[] ipAddresses;
    // 客户端/服务端的启动时间
    protected LocalDateTime startTime;
    // 客户端/服务端的ip地址(对外提供服务)
    protected String ip;
    // 客户端/服务端的端口号(对外提供服务)
    @Value("${server.port}")
    protected int serverPort;
    // 客户端/服务端虚拟集群ui端口号
    protected int flinkUiPort;

    @Autowired
    protected ZooKeeperConfig zooKeeperConfig;

    // 配置库数据源
    @Autowired
    protected ConfigDataSourceConfig configDataSourceConfig;

    /**
     * SCN 刷新到 ZK 的时间间隔，单位：秒
     * 默认 60 秒
     */
    protected long scnFlushIntervalSeconds = 60L;

    // 通知客户端启动，startTime设置为当前时间
    public void notifyClientStart() {
        this.startTime = LocalDateTime.now();
    }

    public  String printStartTime() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = this.startTime.format(formatter);
        return formattedDateTime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String[] getIpAddresses() {
        return ipAddresses;
    }

    public void setIpAddresses(String[] ipAddresses) {
        this.ipAddresses = ipAddresses;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public int getFlinkUiPort() {
        return flinkUiPort;
    }

    public void setFlinkUiPort(int flinkUiPort) {
        this.flinkUiPort = flinkUiPort;
    }

    public ZooKeeperConfig getZooKeeperConfig() {
        return zooKeeperConfig;
    }

    public void setZooKeeperConfig(ZooKeeperConfig zooKeeperConfig) {
        this.zooKeeperConfig = zooKeeperConfig;
    }

    public long getScnFlushIntervalSeconds() {
        return scnFlushIntervalSeconds;
    }

    public void setScnFlushIntervalSeconds(long scnFlushIntervalSeconds) {
        this.scnFlushIntervalSeconds = scnFlushIntervalSeconds;
    }



    public ConfigDataSourceConfig getConfigDataSourceConfig() {
        return configDataSourceConfig;
    }

    public void setConfigDataSourceConfig(ConfigDataSourceConfig configDataSourceConfig) {
        this.configDataSourceConfig = configDataSourceConfig;
    }



}
