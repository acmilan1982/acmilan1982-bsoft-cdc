package com.bsoft.common.dbmeta;

/**
 * ClassName: OracleColumnsQueryRequest
 * Package:   com.bsoft.common.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 13:27
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Oracle列元数据查询条件
 */
public class OracleColumnsQueryRequest implements Serializable {

    private String clientId;
    private String businessDataSourceId;
    private String owner;
    private List<String> tableNames = new ArrayList<>();

    public OracleColumnsQueryRequest() {
    }

    public OracleColumnsQueryRequest(String clientId, String owner, List<String> tableNames) {
        this.clientId = clientId;
        this.owner = owner;
        this.tableNames = tableNames;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public void addTableName(String tableName) {
        if (tableName == null) {
            return;
        }
        String v = tableName.trim();
        if (v.isEmpty()) {
            return;
        }
        if (this.tableNames == null) {
            this.tableNames = new ArrayList<>();
        }
        this.tableNames.add(v);
    }

    public void addTableNames(String... tableNames) {
        if (tableNames == null || tableNames.length == 0) {
            return;
        }
        for (String tableName : tableNames) {
            addTableName(tableName);
        }
    }

}