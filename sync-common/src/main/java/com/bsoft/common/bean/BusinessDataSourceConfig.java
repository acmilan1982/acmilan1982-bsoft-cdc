// BusinessDataSourceConfig.java
package com.bsoft.common.bean;

import com.bsoft.common.dbmeta.OracleTableMetadataReader;
import com.bsoft.common.enums.DatabaseType;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;

/**
 * @author snoopy
 */
public class BusinessDataSourceConfig implements Serializable {

    private static final Logger logger = LogManager.getLogger(BusinessDataSourceConfig.class);

    private String dataSourceId;             // 业务库主键
    private String dataSourceOrg;            // 数据源机构
    private String dataSourceHost;           // 数据库主机名
    private int dataSourcePort;              // 数据库端口
    private String dataSourceUserName;       // 数据库用户名
    private String dataSourcePassword;       // 数据库密码
    private DatabaseType dataSourceType;           // oracle/mysql/doris
    private String dataSourceServiceName;    // 服务名
    private LocalDateTime insertTime;        // 插入时间
    private LocalDateTime updateTime;        // 更新时间
    private LocalDateTime deleteTime;        // 删除时间
    private String fgActive;                 // 是否启用（1=是）
    private String dataSourceDomain;         // 域名
    private String dataSourceCategory;       // 源表还是目标表
    private String sourceApp;                // 源应用
    private String dataSourceName;           // 数据源名称

    // 当前业务库中，所有需要同步的表
    private List<TableMappingConfig> tableMappingConfigList;
    // 所有需要同步的表 ，以表的全限定名为key分组
    private Map<String,TableMappingConfig> tableMappingConfigMapByFullyQualifiedTableName;
    // 所有需要同步的表 ，以schema为key分组
    private Map<String,List<TableMappingConfig>> tableMappingConfigBySchema;

    private String clientId;         // 客户端id
    private List<String>  schemaList;         // 客户端id

    // 当前业务库的订阅计划，由于每个业务库可能有多个目标库，因此用 List 保存
    private List<SubscriberMapping> subscriberMappingList = new ArrayList<>();

    private List<TableMappingConfig> snapShotTableMappingConfigList = new ArrayList<>();

//    // 当前业务库的订阅关系(一对一)
//    private SubscriberMappingRegistry subscriberMappingRegistry;
//
//    // 当前业务库需要同步到的 所有目标库
//    private TargetDataSourceConfigRegistry targetDataSourceConfigRegistry;


    // 目标表命名规则，来源于 CDC_DATA_SOURCE_EXTEND
    private Map<String, TargetTableNamingRule> targetTableNamingRuleMap;


    public BusinessDataSourceConfig() {

    }

    // 客户端初始化专用
    // 初始化,为每个源表，创建对应的 TableMappingConfig
    public void init() {
        this.tableMappingConfigList = buildTableMappingConfigList();
        convertListToMapBySourceTableName();
        convertListToMapBySchemaName();
    }


    // 客户端执行当前操作，中心端不执行
    // 批量创建源表的TableMappingConfig，一个源表只对应一个TableMappingConfig
    private List<TableMappingConfig> buildTableMappingConfigList(){
        // 收集所有需要同步的表，订阅关系中，一个源表可能需要同步到多个目标库，这里去重
        HashSet<Table> tableSet = new HashSet<>();

        // 通过订阅关系，获取所有需要同步的表
        for (SubscriberMapping subscriberMapping : subscriberMappingList) {
            tableSet.addAll(subscriberMapping.getSourceTableNameList());
        }


        // 为每个表，创建对应的TableMappingConfig
        return tableSet.stream()
                .map(this::buildTableMappingConfigForClient)
                .collect(Collectors.toList());
    }

    // 创建单个TableMappingConfig
    private TableMappingConfig buildTableMappingConfigForClient(Table table){
        TableMappingConfig tableMappingConfig = new TableMappingConfig();
        tableMappingConfig.setBusinessDataSourceConfig(this);
        tableMappingConfig.setSchemaName(table.getSchemaName());
        tableMappingConfig.setTableName(table.getTableName());
        tableMappingConfig.setFullyQualifiedTableName(table.getFullyQualifiedTableName());
        return tableMappingConfig;

    }

    public void convertListToMapBySourceTableName(){
        this.tableMappingConfigMapByFullyQualifiedTableName = this.tableMappingConfigList.stream()
                .collect(Collectors.toMap(
                        TableMappingConfig::getFullyQualifiedTableName,
                        Function.identity()
                ));
    }

    public void convertListToMapBySchemaName(){
        // 以schema为key，value为该schema下所有TableMappingConfig的列表
        this.tableMappingConfigBySchema = this.tableMappingConfigList.stream()
                .collect(Collectors.groupingBy(TableMappingConfig::getSchemaName));
    }



    public DataSource getDataSource(){
        try {
            HikariConfig hikariConfig = new HikariConfig();
//        hikariConfig.setDriverClassName(this.driverClassName);
            hikariConfig.setJdbcUrl(this.getUrl());
            hikariConfig.setUsername(this.getDataSourceUserName());
            hikariConfig.setPassword(this.getDataSourcePassword());
            hikariConfig.setMaximumPoolSize(10);
            hikariConfig.setMinimumIdle(2);
            hikariConfig.setConnectionTimeout(10000);
            hikariConfig.setIdleTimeout(60000);
            hikariConfig.setPoolName(this.getDataSourceId() + "-Business-db-pool");

            return new HikariDataSource(hikariConfig);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(" 创建数据源发生异常: DATA_SOURCE_ID： {} -> DATA_SOURCE_ORG：{} ",this.dataSourceId,this.dataSourceOrg);
            throw new RuntimeException(e);
        }
    }


    // 为每个源表，分配主键信息
    public void assignPrimaryKeysOrUniqueKeysForAllTables(){
        // oracle元数据reader
        OracleTableMetadataReader oracleTableMetadataReader = new OracleTableMetadataReader(this);
        for (TableMappingConfig tableMappingConfig : tableMappingConfigList) {
            // 获取所有表的主键/唯一约束/唯一索引 信息
            TableKeys tableKeys = oracleTableMetadataReader.getTableKeys(tableMappingConfig);
            // 只要主键或唯一键
            KeyDef keyDef = tableKeys.getPrimaryOrUniqueKey();
            if (keyDef != null) {
                tableMappingConfig.setPrimaryKey(keyDef.getColumns().stream().map(column -> column.toLowerCase()).collect(Collectors.toList()));
            }
        }
    }



    // 根据表的全限定名，获取该表的主键列
    public List<String> getKeyColumnsByFullyQualifiedTableName(String fullyQualifiedTableName) {
        TableMappingConfig config = tableMappingConfigMapByFullyQualifiedTableName != null ? tableMappingConfigMapByFullyQualifiedTableName.get(fullyQualifiedTableName) : null;
        if (config != null && config.getPrimaryKey() != null) {
            return config.getPrimaryKey();
        }
        return Collections.emptyList();
    }


    public String getUrl(){
         return "jdbc:oracle:thin:@//" + getDataSourceHost() + ":" + getDataSourcePort() + "/" + getDataSourceServiceName();

    }

//
//    public SubscriberMappingRegistry getSubscriberMappingRegistry() {
//        return subscriberMappingRegistry;
//    }
//
//    public void setSubscriberMappingRegistry(SubscriberMappingRegistry subscriberMappingRegistry) {
//        this.subscriberMappingRegistry = subscriberMappingRegistry;
//    }



    public BusinessDataSourceConfig(String dataSourceId, String dataSourceOrg, String dataSourceHost, int dataSourcePort,
                                    String dataSourceUserName, String dataSourcePassword, DatabaseType dataSourceType,
                                    String dataSourceServiceName, LocalDateTime insertTime, LocalDateTime updateTime,
                                    LocalDateTime deleteTime, String fgActive, String dataSourceDomain,
                                    String dataSourceCategory, String sourceApp, String dataSourceName) {
        this.dataSourceId = dataSourceId;
        this.dataSourceOrg = dataSourceOrg;
        this.dataSourceHost = dataSourceHost;
        this.dataSourcePort = dataSourcePort;
        this.dataSourceUserName = dataSourceUserName;
        this.dataSourcePassword = dataSourcePassword;
        this.dataSourceType = dataSourceType;
        this.dataSourceServiceName = dataSourceServiceName;
        this.insertTime = insertTime;
        this.updateTime = updateTime;
        this.deleteTime = deleteTime;
        this.fgActive = fgActive;
        this.dataSourceDomain = dataSourceDomain;
        this.dataSourceCategory = dataSourceCategory;
        this.sourceApp = sourceApp;
        this.dataSourceName = dataSourceName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final BusinessDataSourceConfig instance = new BusinessDataSourceConfig();

        public Builder dataSourceId(String val) { instance.setDataSourceId(val); return this; }
        public Builder dataSourceOrg(String val) { instance.setDataSourceOrg(val); return this; }
        public Builder dataSourceHost(String val) { instance.setDataSourceHost(val); return this; }
        public Builder dataSourcePort(int val) { instance.setDataSourcePort(val); return this; }
        public Builder dataSourceUserName(String val) { instance.setDataSourceUserName(val); return this; }
        public Builder dataSourcePassword(String val) { instance.setDataSourcePassword(val); return this; }
        public Builder dataSourceType(DatabaseType val) { instance.setDataSourceType(val); return this; }
        public Builder dataSourceServiceName(String val) { instance.setDataSourceServiceName(val); return this; }
        public Builder insertTime(LocalDateTime val) { instance.setInsertTime(val); return this; }
        public Builder updateTime(LocalDateTime val) { instance.setUpdateTime(val); return this; }
        public Builder deleteTime(LocalDateTime val) { instance.setDeleteTime(val); return this; }
        public Builder fgActive(String val) { instance.setFgActive(val); return this; }
        public Builder dataSourceDomain(String val) { instance.setDataSourceDomain(val); return this; }
        public Builder dataSourceCategory(String val) { instance.setDataSourceCategory(val); return this; }
        public Builder sourceApp(String val) { instance.setSourceApp(val); return this; }
        public Builder dataSourceName(String val) { instance.setDataSourceName(val); return this; }
        public Builder clientId(String val) { instance.setClientId(val); return this; }
        public Builder targetTableNamingRule(Map<String, TargetTableNamingRule>  val) {
            instance.setTargetTableNamingRule(val);
            return this;
        }
        public BusinessDataSourceConfig build() { return instance; }
    }

    // Getters and Setters
    // （原有 Getter/Setter 保持不变）

    public String getDataSourceId() { return dataSourceId; }
    public void setDataSourceId(String dataSourceId) { this.dataSourceId = dataSourceId; }
    public String getDataSourceOrg() { return dataSourceOrg; }
    public void setDataSourceOrg(String dataSourceOrg) { this.dataSourceOrg = dataSourceOrg; }
    public String getDataSourceHost() { return dataSourceHost; }
    public void setDataSourceHost(String dataSourceHost) { this.dataSourceHost = dataSourceHost; }
    public int getDataSourcePort() { return dataSourcePort; }
    public void setDataSourcePort(int dataSourcePort) { this.dataSourcePort = dataSourcePort; }
    public String getDataSourceUserName() { return dataSourceUserName; }
    public void setDataSourceUserName(String dataSourceUserName) { this.dataSourceUserName = dataSourceUserName; }
    public String getDataSourcePassword() { return dataSourcePassword; }
    public void setDataSourcePassword(String dataSourcePassword) { this.dataSourcePassword = dataSourcePassword; }
    public DatabaseType getDataSourceType() { return dataSourceType; }
    public void setDataSourceType(DatabaseType dataSourceType) { this.dataSourceType = dataSourceType; }
    public String getDataSourceServiceName() { return dataSourceServiceName; }
    public void setDataSourceServiceName(String dataSourceServiceName) { this.dataSourceServiceName = dataSourceServiceName; }
    public LocalDateTime getInsertTime() { return insertTime; }
    public void setInsertTime(LocalDateTime insertTime) { this.insertTime = insertTime; }
    public LocalDateTime getUpdateTime() { return updateTime; }
    public void setUpdateTime(LocalDateTime updateTime) { this.updateTime = updateTime; }
    public LocalDateTime getDeleteTime() { return deleteTime; }
    public void setDeleteTime(LocalDateTime deleteTime) { this.deleteTime = deleteTime; }
    public String getFgActive() { return fgActive; }
    public void setFgActive(String fgActive) { this.fgActive = fgActive; }
    public String getDataSourceDomain() { return dataSourceDomain; }
    public void setDataSourceDomain(String dataSourceDomain) { this.dataSourceDomain = dataSourceDomain; }
    public String getDataSourceCategory() { return dataSourceCategory; }
    public void setDataSourceCategory(String dataSourceCategory) { this.dataSourceCategory = dataSourceCategory; }
    public String getSourceApp() { return sourceApp; }
    public void setSourceApp(String sourceApp) { this.sourceApp = sourceApp; }
    public String getDataSourceName() { return dataSourceName; }
    public void setDataSourceName(String dataSourceName) { this.dataSourceName = dataSourceName; }



    public List<TableMappingConfig> getTableMappingConfigList() {
        return tableMappingConfigList;
    }

    public void setTableMappingConfigList(List<TableMappingConfig> tableMappingConfigList) {
        this.tableMappingConfigList = tableMappingConfigList;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public Map<String, TableMappingConfig> getTableMappingConfigMapByFullyQualifiedTableName() {
        return tableMappingConfigMapByFullyQualifiedTableName;
    }

    public void setTableMappingConfigMapByFullyQualifiedTableName(Map<String, TableMappingConfig> tableMappingConfigMapByFullyQualifiedTableName) {
        this.tableMappingConfigMapByFullyQualifiedTableName = tableMappingConfigMapByFullyQualifiedTableName;
    }

    public List<TableMappingConfig> getSnapShotTableMappingConfigList() {
        return snapShotTableMappingConfigList;
    }

    public void setSnapShotTableMappingConfigList(List<TableMappingConfig> snapShotTableMappingConfigList) {
        this.snapShotTableMappingConfigList = snapShotTableMappingConfigList;
    }

    public List<String> getSchemaList() {
        return schemaList;
    }

    public void setSchemaList(List<String> schemaList) {
        this.schemaList = schemaList;
    }

//    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry() {
//        return targetDataSourceConfigRegistry;
//    }
//
//    public void setTargetDataSourceConfigRegistry(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
//        this.targetDataSourceConfigRegistry = targetDataSourceConfigRegistry;
//    }


//
//    public List<ClientTopicInfo> getClientTopicInfoList(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
//
//
//        return tableMappingConfigList.stream().map(TableMappingConfig::getNormalClientTopicInfoList).flatMap(List::stream).collect(Collectors.toList());
//    }

    @Override
    public String toString() {
        return "BusinessDataSourceConfig{" +
                "dataSourceId='" + dataSourceId + '\'' +
                ", dataSourceOrg='" + dataSourceOrg + '\'' +
                ", dataSourceHost='" + dataSourceHost + '\'' +
                ", dataSourcePort=" + dataSourcePort +
                ", dataSourceUserName='" + dataSourceUserName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        BusinessDataSourceConfig that = (BusinessDataSourceConfig) o;
        return Objects.equals(dataSourceId, that.dataSourceId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataSourceId);
    }

    public Map<String, List<TableMappingConfig>> getTableMappingConfigBySchema() {
        return tableMappingConfigBySchema;
    }

    public void setTableMappingConfigBySchema(Map<String, List<TableMappingConfig>> tableMappingConfigBySchema) {
        this.tableMappingConfigBySchema = tableMappingConfigBySchema;
    }

    public List<SubscriberMapping> getSubscriberMappingList() {
        return subscriberMappingList;
    }

    public void setSubscriberMappingList(List<SubscriberMapping> subscriberMappingList) {
        this.subscriberMappingList = subscriberMappingList;
    }
    public Map<String, TargetTableNamingRule> getTargetTableNamingRule() {
        return this.targetTableNamingRuleMap;
    }

    public void setTargetTableNamingRule(Map<String, TargetTableNamingRule> targetTableNamingRuleMap) {
        this.targetTableNamingRuleMap = targetTableNamingRuleMap;
    }


    public TargetTableNamingRule getTargetTableNamingRuleByTargetDataSourceId(String targetDataSourceId) {
        return this.targetTableNamingRuleMap.get(targetDataSourceId);
    }
}