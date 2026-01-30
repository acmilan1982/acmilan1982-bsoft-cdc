package com.bsoft.common.bean;

import com.bsoft.common.enums.TableNamingStrategy;

/**
 * ClassName: RequestTableMetadata
 * Package:   com.bsoft.common.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/3 - 23:50
 */
public class TableDDLRequest {
    private TargetDataSourceConfig targetDataSourceConfig;
    private String clientId;
    private String schemaName;
    private String tableName;
    private TableNamingStrategy tableNamingStrategy;

    public TableDDLRequest() {
    }

    public TableDDLRequest(TargetDataSourceConfig targetDataSourceConfig,String clientId, String schemaName, String tableName,TableNamingStrategy tableNamingStrategy) {
        this.targetDataSourceConfig = targetDataSourceConfig;
        this.clientId = clientId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableNamingStrategy = tableNamingStrategy;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TargetDataSourceConfig getTargetDataSourceConfig() {
        return targetDataSourceConfig;
    }

    public void setTargetDataSourceConfig(TargetDataSourceConfig targetDataSourceConfig) {
        this.targetDataSourceConfig = targetDataSourceConfig;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
        this.tableNamingStrategy = tableNamingStrategy;
    }
}
