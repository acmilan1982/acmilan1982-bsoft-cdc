package com.bsoft.common.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: TableMappingConfig
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/8 - 11:17
 */
public class TableMappingConfig extends Table implements Serializable {

    // 业务数据源配置
    private  BusinessDataSourceConfig businessDataSourceConfig;

    // 目标表的主键，用list是因为主键可能由多个字段组成
    // 注意，这里的字段统一为小写
    private List<String> primaryKey;

    // 建表语句
    private String ddl;

    // 表的键信息，包括主键、唯一约束、唯一索引、普通索引等
    private TableKeys tableKeys;



    public TableMappingConfig() {
    }



    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(List<String> primaryKey) {
        this.primaryKey = primaryKey;
    }



    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public BusinessDataSourceConfig getBusinessDataSourceConfig() {
        return businessDataSourceConfig;
    }

    public void setBusinessDataSourceConfig(BusinessDataSourceConfig businessDataSourceConfig) {
        this.businessDataSourceConfig = businessDataSourceConfig;
    }

    public TableKeys getTableKeys() {
        return tableKeys;
    }

    public void setTableKeys(TableKeys tableKeys) {
        this.tableKeys = tableKeys;
    }

    //    public List<ClientTopicInfo> getNormalClientTopicInfoList() {
//        return normalClientTopicInfoList;
//    }
//
//    public void setNormalClientTopicInfoList(List<ClientTopicInfo> normalClientTopicInfoList) {
//        this.normalClientTopicInfoList = normalClientTopicInfoList;
//    }
//
//    public void addClientTopicInfo(ClientTopicInfo clientTopicInfo) {
//        this.normalClientTopicInfoList.add(clientTopicInfo);
//    }


}
