package com.bsoft.common.dbmeta;

/**
 * ClassName: SqlResultCreateTableRequest
 * Package:   com.bsoft.common.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 20:46
 */

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 由 SQL 结果字段元数据驱动的建表请求
 */
public class SqlResultCreateTableRequest implements Serializable {


    // 目标库 dataSourceId
    private String targetDataSourceId;

    // 目标表名
    private String targetTableName;
    // SQL结果字段元数据（必填）
    private List<OracleColumnMetadata> columns = new ArrayList<>();


    // 客户端命名相关配置快照
    private SimpleClientInfo clientInfo;

    private String sqlNodeId;


    public SqlResultCreateTableRequest() {
    }


    public SqlResultCreateTableRequest(String targetDataSourceId,
                                       String targetTableName,
                                       List<OracleColumnMetadata> columns,
                                       SimpleClientInfo clientInfo) {
        this.targetDataSourceId = targetDataSourceId;
        this.targetTableName = targetTableName;
        this.columns = columns;
        this.clientInfo = clientInfo;
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

    public List<OracleColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<OracleColumnMetadata> columns) {
        this.columns = columns;
    }



    public String getSqlNodeId() {
        return sqlNodeId;
    }

    public void setSqlNodeId(String sqlNodeId) {
        this.sqlNodeId = sqlNodeId;
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

    public TableNamingStrategy getTableNamingStrategy() {
        return clientInfo == null ? null : clientInfo.getTableNamingStrategy();
    }

    public String getTableNamePrefix() {
        return clientInfo == null ? null : clientInfo.getTableNamePrefix();
    }

    public String getTableNameSuffix() {
        return clientInfo == null ? null : clientInfo.getTableNameSuffix();
    }
}
