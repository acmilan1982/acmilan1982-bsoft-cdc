package com.bsoft.common.dbmeta;

/**
 * ClassName: TableExistsCheckRequest
 * Package:   com.bsoft.common.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/12 - 20:30
 */
public class TableExistsCheckRequest {

    private String dataSourceId;
    private String tableName;


    public TableExistsCheckRequest() {
    }

    public TableExistsCheckRequest(String dataSourceId, String tableName) {
        this.dataSourceId = dataSourceId;
        this.tableName = tableName;
    }


    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}

