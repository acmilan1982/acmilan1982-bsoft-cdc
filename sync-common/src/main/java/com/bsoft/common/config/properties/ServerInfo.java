package com.bsoft.common.config.properties;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.bean.SubscriberMappingRegistry;
import com.bsoft.common.bean.TargetDataSourceConfigRegistry;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.JobStatus;
//import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.enums.TableRowDeleteStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@Component
@ConfigurationProperties(prefix = "server-info")
public class ServerInfo extends  BaseInfo  implements Serializable {

    private static final Logger logger = LogManager.getLogger(ServerInfo.class);

    // 服务端ID，每个服务端都有唯一的id
    private String serverId;

    // 目标库数据源
    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;



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

    private String serverLogTopic = "cdc.sync.server.logs.v1";

    private String monitorMetricTopic = "cdc.sync.monitor.metrics.v1";



    // 启用实时增删改批量处理的目标库类型。
    // 未配置时开启 Oracle、MySQL、Doris；显式空列表表示全部关闭。
    private List<DatabaseType> realtimeBatchEnabledDatabaseTypes =
            new ArrayList<>(Arrays.asList(
                    DatabaseType.ORACLE,
                    DatabaseType.MYSQL,
                    DatabaseType.DORIS));

    public ServerInfo() {

    }


    @PostConstruct
    private void initZKPath(){
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

    public String getServerLogTopic() {
        return serverLogTopic;
    }

    public void setServerLogTopic(String serverLogTopic) {
        this.serverLogTopic = serverLogTopic;
    }

    public String getMonitorMetricTopic() {
        return monitorMetricTopic;
    }

    public void setMonitorMetricTopic(String monitorMetricTopic) {
        this.monitorMetricTopic = monitorMetricTopic;
    }

    // 获取实时增删改启用批量处理的目标库类型列表。
    public List<DatabaseType> getRealtimeBatchEnabledDatabaseTypes() {
        // 保留普通可修改集合，供配置绑定使用。
        return realtimeBatchEnabledDatabaseTypes;
    }

    // 设置实时增删改批量处理的目标库类型列表。
    public void setRealtimeBatchEnabledDatabaseTypes(
            List<DatabaseType> databaseTypes) {
        // null 按未配置处理，开启当前支持的全部目标库类型。
        if (databaseTypes == null) {
            this.realtimeBatchEnabledDatabaseTypes =
                    new ArrayList<>(Arrays.asList(
                            DatabaseType.ORACLE,
                            DatabaseType.MYSQL,
                            DatabaseType.DORIS));
            return;
        }

        // 复制并去重，显式空列表保持为空。
        List<DatabaseType> configuredTypes = new ArrayList<>();
        for (DatabaseType databaseType : databaseTypes) {
            // 配置中的无效元素明确报错。
            if (databaseType == null) {
                throw new IllegalArgumentException(
                        "实时批量处理的数据库类型不能为 null");
            }

            // 同一数据库类型仅保存一次。
            if (!configuredTypes.contains(databaseType)) {
                configuredTypes.add(databaseType);
            }
        }

        // 已配置时严格采用配置结果，不为未列出的数据库类型开启批量。
        this.realtimeBatchEnabledDatabaseTypes = configuredTypes;
    }

    // 判断指定目标库类型是否启用实时增删改批量处理。
// writer 在 open 中调用一次，运行期间使用保存的结果。
    public boolean isRealtimeBatchEnabled(DatabaseType databaseType) {
        // 未指定目标库类型时不启用批量。
        if (databaseType == null) {
            return false;
        }

        // 防御性处理未初始化配置，默认开启当前支持的全部目标库类型。
        if (realtimeBatchEnabledDatabaseTypes == null) {
            return databaseType == DatabaseType.ORACLE
                    || databaseType == DatabaseType.MYSQL
                    || databaseType == DatabaseType.DORIS;
        }

        // 空列表返回 false；非空列表仅开启明确列出的数据库类型。
        return realtimeBatchEnabledDatabaseTypes.contains(databaseType);
    }

}
