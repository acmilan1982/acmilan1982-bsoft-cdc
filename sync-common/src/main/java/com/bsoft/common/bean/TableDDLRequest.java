package com.bsoft.common.bean;

import com.bsoft.common.config.properties.SimpleClientInfo;
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


    private String schemaName;
    private String tableName;

    private SimpleClientInfo clientInfo;

    // 业务库 dataSourceId
    private String businessDataSourceId;

    // 目标库 dataSourceId
    private String targetDataSourceId;

    public TableDDLRequest() {
    }

    public TableDDLRequest(String businessDataSourceId,
                           String targetDataSourceId,
                           SimpleClientInfo clientInfo,
                           String schemaName,
                           String tableName) {
        this.businessDataSourceId = businessDataSourceId;
        this.targetDataSourceId = targetDataSourceId;
        this.clientInfo = clientInfo;
        this.schemaName = schemaName;
        this.tableName = tableName;
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

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    public SimpleClientInfo getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(SimpleClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }

    // 兼容旧调用方
    public String getClientId() {
        return clientInfo == null ? null : clientInfo.getClientId();
    }


    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }
}
