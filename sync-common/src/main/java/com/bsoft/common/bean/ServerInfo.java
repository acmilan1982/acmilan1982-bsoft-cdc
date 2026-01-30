package com.bsoft.common.bean;

import com.bsoft.common.enums.JobStatus;
//import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.enums.TableRowDeleteStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@Component
@ConfigurationProperties(prefix = "server-info")
public class ServerInfo extends  BaseInfo  implements Serializable {

    private static final Logger logger = LogManager.getLogger(ServerInfo.class);

    // 服务端ID，每个服务端都有唯一的id
    private String serverId;

    // 目标库数据源
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;

    // 配置库数据源
    private ConfigDataSourceConfig configDataSourceConfig;

    // 订阅关系
    private SubscriberMappingRegistry subscriberMappingRegistry;



    private JobStatus jobStatus;

    // zk中job状态路径
    private String serverStatusPath;

    // zk中客户端ip路径
    private String serverIpPath;

    // zk中监控数据路径
    private String monitorDataPath;

//    // 目标表命名策略
//    private TableNamingStrategy tableNamingStrategy;
    // 目标表删除行策略
    private TableRowDeleteStrategy tableRowDeleteStrategy;

    // 全量快照每批次处理的记录数
    private int snapshotBatchSize = 500;

    /**
     * 多表合并时是否自动扩充字段长度
     * true：自动对比源表和目标表字段长度，如果源表字段更长则扩充目标表字段
     * false：不进行字段长度对比和扩充
     */
    private boolean autoExpandColumnLength = true;

    /**
     * 原始消息存储策略
     * 配置项: server-info.raw-message-storage-strategy
     * 可选值: NONE(不插入) | PLAIN(不压缩) | COMPRESS(压缩,默认)
     */
    private RawMessageStorageStrategy rawMessageStorageStrategy = RawMessageStorageStrategy.COMPRESS;


    /**
     * 是否自动创建目标表
     * true: 如果目标表不存在则自动创建
     * false: 不自动创建,表不存在时抛出异常
     */
    private boolean autoCreateTable = true;

    public ServerInfo() {

    }


    @PostConstruct
    private void initZKPath(){
        // job状态路径
        serverStatusPath = "/servers/" + getServerId() + "/status";

        // 客户端ip路径
        serverIpPath = "/servers/" + getServerId() + "/ip";

        // zk中监控数据路径
        monitorDataPath = "/servers/" + getServerId() + "/monitor_data";
    }

   public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry() {
        return targetDataSourceConfigRegistry;
    }

    public void setTargetDataSourceConfigRegistry(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
        this.targetDataSourceConfigRegistry = targetDataSourceConfigRegistry;
    }


    public ConfigDataSourceConfig getConfigDataSourceConfig() {
        return configDataSourceConfig;
    }

    public void setConfigDataSourceConfig(ConfigDataSourceConfig configDataSourceConfig) {
        this.configDataSourceConfig = configDataSourceConfig;
    }

    public SubscriberMappingRegistry getSubscriberMappingRegistry() {
        return subscriberMappingRegistry;
    }

    public void setSubscriberMappingRegistry(SubscriberMappingRegistry subscriberMappingRegistry) {
        this.subscriberMappingRegistry = subscriberMappingRegistry;
    }

//    public TableNamingStrategy getTableNamingStrategy() {
//        return tableNamingStrategy;
//    }
//
//    public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
//        this.tableNamingStrategy = tableNamingStrategy;
//    }

    public JobStatus getJobStatus() {
        return jobStatus;
    }

    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    public String getServerStatusPath() {
        return serverStatusPath;
    }

    public void setServerStatusPath(String serverStatusPath) {
        this.serverStatusPath = serverStatusPath;
    }

    public String getServerIpPath() {
        return serverIpPath;
    }

    public void setServerIpPath(String serverIpPath) {
        this.serverIpPath = serverIpPath;
    }



    public TableRowDeleteStrategy getTableRowDeleteStrategy() {
        return tableRowDeleteStrategy;
    }

    public void setTableRowDeleteStrategy(TableRowDeleteStrategy tableRowDeleteStrategy) {
        this.tableRowDeleteStrategy = tableRowDeleteStrategy;
    }

    public String getMonitorDataPath() {
        return monitorDataPath;
    }

    public void setMonitorDataPath(String monitorDataPath) {
        this.monitorDataPath = monitorDataPath;
    }

    public int getSnapshotBatchSize() {
        return snapshotBatchSize;
    }

    public void setSnapshotBatchSize(int snapshotBatchSize) {
        this.snapshotBatchSize = (snapshotBatchSize > 500 || snapshotBatchSize <= 0) ? 500 : snapshotBatchSize;
    }

    public RawMessageStorageStrategy getRawMessageStorageStrategy() {
        return rawMessageStorageStrategy;
    }

    public void setRawMessageStorageStrategy(RawMessageStorageStrategy rawMessageStorageStrategy) {
        this.rawMessageStorageStrategy = rawMessageStorageStrategy;
    }

    public boolean isAutoExpandColumnLength() {
        return autoExpandColumnLength;
    }

    public void setAutoExpandColumnLength(boolean autoExpandColumnLength) {
        this.autoExpandColumnLength = autoExpandColumnLength;
    }


    public boolean isAutoCreateTable() {
        return autoCreateTable;
    }

    public void setAutoCreateTable(boolean autoCreateTable) {
        this.autoCreateTable = autoCreateTable;
    }
}
