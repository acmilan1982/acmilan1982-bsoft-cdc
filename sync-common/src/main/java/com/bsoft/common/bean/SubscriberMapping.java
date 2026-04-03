package com.bsoft.common.bean;

import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: SubscriberMapping
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/31 - 15:25
 */
public class SubscriberMapping implements Serializable {
    // 源库 DataSourceId
    private String fromDataSourceId;

    // 目标库 DataSourceId
    private String toDataSourceId;

    // 源库需要同步的表
    private List<Table> sourceTableNameList = new ArrayList<>();


    // 客户端相关配置直接聚合
    private SimpleClientInfo clientInfo;


    //以下属性，只在直接读取 CDC_DATA_SUBSCRIBE 表时使用，稍后会转换成多个 一对一 的SubscriberMapping
    // 业务库 DataSourceId
    private List<String> tempFromDataSourceIdList = new ArrayList<>();
    // 目标库 DataSourceId
    private List<String> tempToDataSourceIdList = new ArrayList<>();
    // 需要同步的表
    private List<Table> tempSourceTableNameList = new ArrayList<>();


    private  ServerInfo serverInfo;



    public SubscriberMapping() {
    }

    public SubscriberMapping(String fromDataSourceId,String toDataSourceId, List<Table> sourceTableNameList) {
        this.fromDataSourceId = fromDataSourceId;
        this.toDataSourceId = toDataSourceId;
        this.sourceTableNameList = sourceTableNameList;
    }

    public SubscriberMapping(String fromDataSourceId,String toDataSourceId, List<Table> sourceTableNameList, SimpleClientInfo clientInfo) {
        this.fromDataSourceId = fromDataSourceId;
        this.toDataSourceId = toDataSourceId;
        this.sourceTableNameList = sourceTableNameList;
        this.clientInfo = clientInfo;
    }

    public SubscriberMapping(String fromDataSourceId,String toDataSourceId, List<Table> sourceTableNameList, ServerInfo serverInfo) {
        this.fromDataSourceId = fromDataSourceId;
        this.toDataSourceId = toDataSourceId;
        this.sourceTableNameList = sourceTableNameList;
        this.serverInfo = serverInfo;
    }

    public String getFromDataSourceId() {
        return fromDataSourceId;
    }

    public void setFromDataSourceId(String fromDataSourceId) {
        this.fromDataSourceId = fromDataSourceId;
    }

    public List<String> getTempToDataSourceIdList() {
        return tempToDataSourceIdList;
    }

    public void setTempToDataSourceIdList(List<String> tempToDataSourceIdList) {
        this.tempToDataSourceIdList = tempToDataSourceIdList;
    }

    public List<String> getTempFromDataSourceIdList() {
        return tempFromDataSourceIdList;
    }

    public void setTempFromDataSourceIdList(List<String> tempFromDataSourceIdList) {
        this.tempFromDataSourceIdList = tempFromDataSourceIdList;
    }

    // 表中的这种数据“a31a1a6e542747ea8bcbfb12bd43b6b9,5905f1ce83024410836b40ca0ebfc477”，每个元素加入到List
    public void addTempFromDataSourceId(String fromDataSourceIdString) {
        String[] fromDataSourceIdArray = fromDataSourceIdString.split(",");
        Arrays.stream(fromDataSourceIdArray).forEach(fromDataSourceId -> tempFromDataSourceIdList.add(fromDataSourceId));
    }

    // 表中的这种数据“a31a1a6e542747ea8bcbfb12bd43b6b9,5905f1ce83024410836b40ca0ebfc477”，每个元素加入到List
    public void addTempToDataSourceId(String toDataSourceIdString) {
        String[] toDataSourceIdArray = toDataSourceIdString.split(",");
        Arrays.stream(toDataSourceIdArray).forEach(toDataSourceId -> tempToDataSourceIdList.add(toDataSourceId));
    }

    // 表中的这种数据“DEV_EHRVIEW.OPT_FEE, DEV_EHRVIEW.OPT_RECIPE, DEV_EHRVIEW.OPT_RECIPEDETAIL, DEV_EHRVIEW.OPT_RECORD, DEV_EHRVIEW.OPT_REGISTER”，每个元素加入到List
    public void addTempSourceTableName(String sourceTableNameString) {
        Set<Table> tableSet = new HashSet<>();

        String[] sourceTableNameStringArray = sourceTableNameString.split(",");
        for (String sourceFullyQualifiedTableName : sourceTableNameStringArray) {
            String[] parts = sourceFullyQualifiedTableName.trim().split("\\.");
            if (parts.length == 3) {
                tableSet.add(new Table(parts[0].trim(),                          // 业务库id
                                       parts[1].trim().toUpperCase(),            // 业务库schema
                                       parts[2].trim().toUpperCase()));          // 业务库表名
            }
        }

        tableSet.stream().forEach(table -> tempSourceTableNameList.add(table));
    }

    public List<Table> getSourceTableNameList() {
        return sourceTableNameList;
    }

    public void setSourceTableNameList(List<Table> sourceTableNameList) {
        this.sourceTableNameList = sourceTableNameList;
    }

    //-----------------------  客户端生成TopicInfo 开始---------------------------------------



    // 为 单个源库，单个目标库 生成 ClientTopicInfo，可能有多个 ClientTopicInfo
//    public List<ClientTopicInfo> generateClientTopicInfo(BusinessDataSourceConfig businessDataSourceConfig, TargetDataSourceConfig targetDataSourceConfig) {
//        List<ClientTopicInfo> clientTopicInfoList = new ArrayList<>();
//        // 业务库所有待同步的哦
//        Map<String, TableMappingConfig> tableMappingConfigMap = businessDataSourceConfig.getTableMappingConfigMap();
//        // 当前订阅关系需要同步的表
//        for (Table table : sourceTableNameList) {
//            TableMappingConfig tableMappingConfig = tableMappingConfigMap.get(table.generateFullyQualifiedTableName());
//
//            // 客户端主题信息
//            ClientTopicInfo clientTopicInfo = new ClientTopicInfo();
//            clientTopicInfo.setBusinessDataSourceId(businessDataSourceConfig.getDataSourceId());
//            clientTopicInfo.setTargetDataSourceId(targetDataSourceConfig.getDataSourceId());
//
//            // 生成主题
//            clientTopicInfo.setTopic( generateTopic(tableMappingConfig, targetDataSourceConfig));
//            clientTopicInfoList.add(clientTopicInfo);
//
//            // clientTopicInfo 设置到 TableMappingConfig
//            tableMappingConfig.addClientTopicInfo(clientTopicInfo);
//        }
//
//
//        return clientTopicInfoList;
//    }


    public List<Table> getTempSourceTableNameList() {
        return tempSourceTableNameList;
    }

    public void setTempSourceTableNameList(List<Table> tempSourceTableNameList) {
        this.tempSourceTableNameList = tempSourceTableNameList;
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    public void setServerInfo(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    public String getToDataSourceId() {
        return toDataSourceId;
    }

    public void setToDataSourceId(String toDataSourceId) {
        this.toDataSourceId = toDataSourceId;
    }



    public String getClientId() {
        return this.clientInfo.getClientId();
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return this.clientInfo.getTableNamingStrategy();
    }

    public String getTableNamePrefix() {
        return this.clientInfo.getTableNamePrefix();
    }

    public String getTableNameSuffix() {
        return this.clientInfo.getTableNameSuffix();
    }

    public SimpleClientInfo getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(SimpleClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }
}


