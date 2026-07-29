package com.bsoft.common.dbmeta;

/**
 * ClassName: OracleSqlColumnsQueryRequest
 * Package:   com.bsoft.common.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 19:37
 */

import com.bsoft.common.bean.TargetDataSourceConfig;

import java.io.Serializable;


public class OracleSqlColumnsQueryRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    // 由哪个 sync-client 执行 SQL 元数据查询
    private String clientId;

    // 业务库
    private String businessDataSourceId;

    // 需要导出列结构的 SQL
    private String sql;
    // SQL 执行上下文 schema（如 SPT_HIS_2023）
    private String schema;

    // 最终表名
    private String targetTableName;

    private String sqlNodeId;



    // 目标库 dataSourceId
    private String targetDataSourceId;



    public OracleSqlColumnsQueryRequest() {
    }


    public OracleSqlColumnsQueryRequest(String clientId,
                                        String sql,
                                        String schema,
                                        String targetDataSourceId,
                                        String targetTableName) {
        this.clientId = clientId;
        this.sql = sql;
        this.schema = schema;
        this.targetDataSourceId = targetDataSourceId;
        this.targetTableName = targetTableName;
    }


    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getSqlNodeId() {
        return sqlNodeId;
    }

    public void setSqlNodeId(String sqlNodeId) {
        this.sqlNodeId = sqlNodeId;
    }

    public String getBusinessDataSourceId() {
        return businessDataSourceId;
    }

    public void setBusinessDataSourceId(String businessDataSourceId) {
        this.businessDataSourceId = businessDataSourceId;
    }
}