package com.bsoft.server.sql.adapter;

import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.dataSource.DataSourceFactory;

import javax.sql.DataSource;

/**
 * ClassName: MySqlTableMetadata
 * Package:   com.bsoft.server.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 18:49
 */
public  class DataSourceHolder {

    // 目标库的数据源
    protected DataSource dataSource;

    // 业务库的数据源
    protected TargetDataSourceConfig targetDataSourceConfig;

    // 客户端信息
    private ServerInfo serverInfo;

    public DataSourceHolder() {
    }

    public DataSourceHolder(TargetDataSourceConfig targetDataSourceConfig) {
        this.targetDataSourceConfig = targetDataSourceConfig;
        this.dataSource = DataSourceFactory.getDataSource(targetDataSourceConfig);
    }

    public DataSourceHolder(TargetDataSourceConfig targetDataSourceConfig, ServerInfo serverInfo) {
        this.targetDataSourceConfig = targetDataSourceConfig;
        this.dataSource = DataSourceFactory.getDataSource(targetDataSourceConfig);
        this.serverInfo = serverInfo;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public TargetDataSourceConfig getTargetDataSourceConfig() {
        return targetDataSourceConfig;
    }

    public void setTargetDataSourceConfig(TargetDataSourceConfig targetDataSourceConfig) {
        this.targetDataSourceConfig = targetDataSourceConfig;
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    public void setServerInfo(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }
}

