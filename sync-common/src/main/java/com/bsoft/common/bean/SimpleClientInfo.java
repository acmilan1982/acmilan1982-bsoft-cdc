package com.bsoft.common.bean;

import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;



public class SimpleClientInfo extends  BaseInfo implements Serializable {


    // 客户端ID，每个客户端都有唯一的id
    private String clientId;

    // 客户端采集的业务库主键(CDC_DATA_SOURCE表的主键)
    private String dataSourceId;

    // 客户端在目标库的表命名策略
    private TableNamingStrategy tableNamingStrategy;

    public SimpleClientInfo()  {

    }


    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
        this.tableNamingStrategy = tableNamingStrategy;
    }
}
