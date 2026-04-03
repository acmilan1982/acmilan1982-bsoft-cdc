// BusinessDataSourceConfig.java
package com.bsoft.common.bean;

import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.DorisTableModel;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @author snoopy
 */
// 目标库配置
public class TargetDataSourceConfig implements Serializable {

    private String dataSourceId;             // 主键
    private String dataSourceOrg;            // 数据源机构
    private String dataSourceHost;           // 数据库主机名
    private int dataSourcePort;              // 数据库端口
    private String dataSourceUserName;       // 数据库用户名
    private String dataSourcePassword;       // 数据库密码
    private DatabaseType dataSourceType;     // 数据库类型：oracle/mysql/doris
    private String dataSourceServiceName;    // 服务名
    private LocalDateTime insertTime;        // 插入时间
    private LocalDateTime updateTime;        // 更新时间
    private LocalDateTime deleteTime;        // 删除时间
    private String fgActive;                 // 是否启用（1=是）
    private String dataSourceDomain;         // 域名
    private String dataSourceCategory;       // 源表还是目标表
    private String sourceApp;                // 源应用
    private String dataSourceName;           // 数据源名称

    private String  serverId;         // 服务端id


//    private DataSource dataSource;

    // 数据库的详细信息，包括类型和版本
    private DatabaseInfo databaseInfo;


    private static final ObjectMapper BIZ_ATTR_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);


    // 目标库自定义建表属性，DATA_SOURCE_BIZ_ATTR(JSON)
    private String dataSourceBizAttr;

    // 库级建表属性
    private DdlBizAttr systemLevelDdlBizAttr;

    // 表级建表属性，key=最终目标表名 targetTableName
    private Map<String, DdlBizAttr> tableLevelDdlBizAttrMap = new HashMap<>();




    public TargetDataSourceConfig() {

    }


    /**
     * 判断当前 Doris 版本是否支持 DATETIMEV2(>= 3.2)
     * @return true 表示支持 DATETIMEV2,false 表示使用 DATETIME
     */
    public boolean shouldEnableDateTimeV2() {
        if (this.databaseInfo == null
                || this.databaseInfo.getMajorVersion() == null
                || this.databaseInfo.getMinorVersion() == null) {
            return false;
        }

        int major = this.databaseInfo.getMajorVersion();
        int minor = this.databaseInfo.getMinorVersion();

        return major > 3 || (major == 3 && minor >= 2);
    }

    public String getUrl(){

        String jdbcUrl;

        switch (dataSourceType) {
            case MYSQL:
                jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai",
                        getDataSourceHost(),
                        getDataSourcePort(),
                        getDataSourceServiceName());

                break;
            case ORACLE:
                jdbcUrl = String.format(
                        "jdbc:oracle:thin:@%s:%d/%s",
                        getDataSourceHost(),
                        getDataSourcePort(),
                        getDataSourceServiceName()
                );
                break;
            case DORIS:
                jdbcUrl = String.format(
                        "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai",
                        getDataSourceHost(),
                        getDataSourcePort(),       // 通常是 9030
                        getDataSourceServiceName() // Doris 里的库名
                );
                break;
            default:
                throw new IllegalStateException("Unsupported db type mode: " + dataSourceType);
        }

        return jdbcUrl;
    }

    public DataSource initDataSource(ConfigDataSourceConfig configDataSourceConfig){
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(configDataSourceConfig.getDriverClassName());
        hikariConfig.setJdbcUrl(configDataSourceConfig.getUrl());
        hikariConfig.setUsername(configDataSourceConfig.getUsername());
        hikariConfig.setPassword(configDataSourceConfig.getPassword());
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setIdleTimeout(60000);
        hikariConfig.setPoolName(configDataSourceConfig.getUsername() + "-config-db-pool");

        return  new HikariDataSource(hikariConfig);
    }

    public DataSource initTargetDataSource( ){
        HikariConfig hikariConfig = new HikariConfig();
//        hikariConfig.setDriverClassName(configDataSourceConfig.getDriverClassName());
        hikariConfig.setJdbcUrl(this.getUrl());
        hikariConfig.setUsername(this.dataSourceUserName);
        hikariConfig.setPassword(this.dataSourcePassword);
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setConnectionTimeout(10000);
        hikariConfig.setIdleTimeout(60000);
        hikariConfig.setPoolName(this.dataSourceUserName + "-config-db-pool");

        return  new HikariDataSource(hikariConfig);
    }


    public TargetDataSourceConfig(String dataSourceId, String dataSourceOrg, String dataSourceHost, int dataSourcePort,
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
        private final TargetDataSourceConfig instance = new TargetDataSourceConfig();

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
        public Builder serverId(String val) { instance.setServerId(val); return this; }
        public Builder dataSourceBizAttr(String val) { instance.setDataSourceBizAttr(val); return this; }
        public TargetDataSourceConfig build() { return instance; }
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

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }

    public void setDatabaseInfo(DatabaseInfo databaseInfo) {
        this.databaseInfo = databaseInfo;
    }




    public String getDataSourceBizAttr() {
        return dataSourceBizAttr;
    }

    public void setDataSourceBizAttr(String dataSourceBizAttr) {
        this.dataSourceBizAttr = dataSourceBizAttr;
        this.systemLevelDdlBizAttr = parseSystemLevelDdlBizAttr(dataSourceBizAttr);
    }

    // 获取 目标库级 建表参数
    public DdlBizAttr getSystemLevelDdlBizAttr() {
        return systemLevelDdlBizAttr;
    }

    public void setSystemLevelDdlBizAttr(DdlBizAttr systemLevelDdlBizAttr) {
        this.systemLevelDdlBizAttr = systemLevelDdlBizAttr;
    }

    private DdlBizAttr parseSystemLevelDdlBizAttr(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            // 当前先按 Doris 解析，返回类型提升为 DdlBizAttr
            return BIZ_ATTR_MAPPER.readValue(json, DorisBizAttr.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DATA_SOURCE_BIZ_ATTR: " + json, e);
        }
    }

    // 获取 表级 建表参数
    public Map<String, DdlBizAttr> getTableLevelDdlBizAttrMap() {
        return tableLevelDdlBizAttrMap;
    }

    public void setTableLevelDdlBizAttrMap(Map<String, DdlBizAttr> tableLevelDdlBizAttrMap) {
        this.tableLevelDdlBizAttrMap = (tableLevelDdlBizAttrMap == null)
                ? new HashMap<>()
                : tableLevelDdlBizAttrMap;
    }

    public void putTableLevelDdlBizAttr(String targetTableName, DdlBizAttr bizAttr) {
        if (targetTableName == null || targetTableName.trim().isEmpty() || bizAttr == null) {
            return;
        }
        this.tableLevelDdlBizAttrMap.put(targetTableName.trim(), bizAttr);
    }

    public DdlBizAttr getTableLevelDdlBizAttr(String targetTableName) {
        if (targetTableName == null || targetTableName.trim().isEmpty()) {
            return null;
        }
        return this.tableLevelDdlBizAttrMap.get(targetTableName.trim());
    }



    /**
     * 按表名获取 Doris 表模型。
     * 非 Doris 数据源，或未配置表模型时返回 null。
     */
    public DorisTableModel getTableModel(String targetTableName) {
        if (this.dataSourceType != DatabaseType.DORIS) {
            return null;
        }

        DdlBizAttr ddlBizAttr = getEffectiveDdlBizAttr(targetTableName);
        if (!(ddlBizAttr instanceof DorisBizAttr)) {
            return null;
        }

        return ((DorisBizAttr) ddlBizAttr).getTableModel();
    }


    /**
     * 按表名获取最终生效的建表属性。
     * 规则：
     * 1. 先取表级 DdlBizAttr
     * 2. 表级没有则回退到系统级 DdlBizAttr
     * 3. 都没有则返回 null
     */
    public DdlBizAttr getEffectiveDdlBizAttr(String targetTableName) {
        DdlBizAttr ddlBizAttr = getTableLevelDdlBizAttr(targetTableName);
        if (ddlBizAttr != null) {
            return ddlBizAttr;
        }
        return getSystemLevelDdlBizAttr();
    }


}

