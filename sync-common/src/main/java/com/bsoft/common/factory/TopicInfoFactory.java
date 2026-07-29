package com.bsoft.common.factory;



import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.util.DatasourceConfigFetcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 主题信息工厂类
 * 负责创建 ClientTopicInfo 和 ServerTopicInfo 对象
 */

public class TopicInfoFactory {

//    @Autowired(required = false)
//    private ClientInfo clientInfo;
//
//    @Autowired(required = false)
//    private ServerInfo serverInfo;

    private TableNameGenerator tableNameGenerator = new TableNameGenerator();


    public TopicInfoFactory() {
    }

    //-----------------------  客户端生成TopicInfo 开始---------------------------------------
    public List<ClientTopicInfo> generateClientTopicInfo(List<ExecutionPlan.TableExecutionPlan> tableExecutionPlanList) {

        List<ClientTopicInfo> clientTopicInfoList = new ArrayList<>();
        for (ExecutionPlan.TableExecutionPlan tableExecutionPlan : tableExecutionPlanList) {
            // 生成客户端主题信息
            clientTopicInfoList.add(generateBasicTopicInfo(tableExecutionPlan));
        }
        return clientTopicInfoList;
    }


    // 生成基本的TopicInfo，主要包括topic name
    public ServerTopicInfo generateBasicTopicInfo(ExecutionPlan.TableExecutionPlan tableExecutionPlan ) {

        // ServerTopicInfo是ClientTopicInfo的子类，多一个目标表名
        ServerTopicInfo serverTopicInfo = new ServerTopicInfo();

        // 源库DataSourceId
        serverTopicInfo.setBusinessDataSourceId(tableExecutionPlan.getFromDataSourceId());
        // 目标库DataSourceId
        serverTopicInfo.setTargetDataSourceId(tableExecutionPlan.getToDataSourceId());
        // 源库schema
        serverTopicInfo.setSchemaName(tableExecutionPlan.getSchemaName());
        // 源库表名
        serverTopicInfo.setTableName(tableExecutionPlan.getTableName());

        serverTopicInfo.setClientInfo(tableExecutionPlan.getClientInfo());

        // 生成主题
        serverTopicInfo.setTopic(generateTopic(tableExecutionPlan.getClientId(),tableExecutionPlan));

        return serverTopicInfo;
    }





    // 生成kafka的topic，生成规则是固定的，不可配置
    // 客户端，中心端共用该主题生成模式
    public String generateTopic(String clientId,
                                ExecutionPlan.TableExecutionPlan tableExecutionPlan) {


        return TopicNameFactory.generateTopic(
                clientId,
                tableExecutionPlan.getFromDataSourceId(),
                tableExecutionPlan.getSchemaName(),
                tableExecutionPlan.getTableName(),
                tableExecutionPlan.getToDataSourceId());

    }





    //-----------------------  客户端生成TopicInfo 结束---------------------------------------










    //-----------------------  服务端 生成TopicInfo 开始---------------------------------------

    // 在服务端，根据当前订阅关系，生成ServerTopicInfo
    public List<ServerTopicInfo> generateServerTopicInfo(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                                         TargetDataSourceConfigRegistry targetDataSourceConfigRegistry,
                                                         List<ExecutionPlan.TableExecutionPlan> tableExecutionPlanList,
                                                         DatasourceConfigFetcher configFetcher) throws Exception {

        List<ServerTopicInfo> serverTopicInfoList = new ArrayList<>();

        // 业务库需要同步的表，每个TableExecutionPlan表示单个表的执行计划
        for (ExecutionPlan.TableExecutionPlan tableExecutionPlan : tableExecutionPlanList) {
            // 业务库id
            String fromDataSourceId = tableExecutionPlan.getFromDataSourceId();
            // 目标库id
            String toDataSourceId = tableExecutionPlan.getToDataSourceId();
            // 获取 业务库(有且仅有一个)
            BusinessDataSourceConfig businessDataSourceConfig = businessDataSourceConfigRegistry.findBusinessDataSourceConfigByDataSourceId(fromDataSourceId);
            // 获取 目标库(有且仅有一个)
            TargetDataSourceConfig targetDataSourceConfig = targetDataSourceConfigRegistry.findTargetDataSourceConfigByDataSourceId(toDataSourceId);

            // 生成 <主题名> 与 <目标表名>，注意，此处生成的表名，可能不是最后使用的表名
            // TABLE_MERGE，CLIENTID_SCHEMA_TABLE模式，现在就能确定表名
            // TABLE_PREFIX模式，目标表可能需要改名，但此时无法确定是否需要修改，因此只能先生成初始表名，后续再处理
            serverTopicInfoList.add(generateServerTopicInfo(businessDataSourceConfig,
                                    targetDataSourceConfig,
                                    tableExecutionPlan));
        }
//
//        // 如果是 TABLE_PREFIX 模式，需要处理重复的目标表名问题
//        if(tableExecutionPlan.getTableNamingStrategy() == TableNamingStrategy.TABLE_PREFIX ) {
//            // 处理重复的目标表名问题
//            tableNameGenerator.handleDuplicateTargetTableNames(serverTopicInfoList,
//                                                               targetDataSourceConfigRegistry,
//                                                               configFetcher);
//        }
//

        return serverTopicInfoList;
    }


    // 为客户端的单个执行计划，生成 ServerTopicInfo，主要内容是topic name + 目标表名
    public ServerTopicInfo generateServerTopicInfo(BusinessDataSourceConfig businessDataSourceConfig,
                                                   TargetDataSourceConfig targetDataSourceConfig,
                                                   ExecutionPlan.TableExecutionPlan tableExecutionPlan ) {
        // 获取客户端ID
        TargetTableNamingRule targetTableNamingRule = businessDataSourceConfig.getTargetTableNamingRuleByTargetDataSourceId(targetDataSourceConfig.getDataSourceId());
        // 生成基本的ServerTopicInfo
        ServerTopicInfo serverTopicInfo = generateBasicTopicInfo(tableExecutionPlan);

        // 目标表类型
        DatabaseType dataSourceType = targetDataSourceConfig.getDataSourceType();
        // 生成目标表名
        String targetTableName = generateTargetTableName(targetTableNamingRule,dataSourceType,tableExecutionPlan);
        // 设置目标表名
        serverTopicInfo.setTargetTableName(targetTableName);

        serverTopicInfo.setClientInfo(tableExecutionPlan.getClientInfo());

        return serverTopicInfo;
    }



    // 生成目标表名
    public String generateTargetTableName(TargetTableNamingRule targetTableNamingRule,
                                          DatabaseType databaseType,                                   // 目标表类型
                                          ExecutionPlan.TableExecutionPlan tableExecutionPlan) {       // 表级的执行计划
        // 1. 先根据命名策略生成表名
        String targetTableName = tableNameGenerator.generateTargetTableName(targetTableNamingRule,tableExecutionPlan);
        // 2. 再根据库的类型，选择 表名 大写还是 小写
        return tableNameGenerator.applyDatabaseSpecificFormatting(targetTableName, databaseType);
    }






//    public ClientInfo getClientInfo() {
//        return clientInfo;
//    }
//
//    public void setClientInfo(ClientInfo clientInfo) {
//        this.clientInfo = clientInfo;
//    }
//
//    public ServerInfo getServerInfo() {
//        return serverInfo;
//    }
//
//    public void setServerInfo(ServerInfo serverInfo) {
//        this.serverInfo = serverInfo;
//    }

    public TableNameGenerator getTableNameGenerator() {
        return tableNameGenerator;
    }

    public void setTableNameGenerator(TableNameGenerator tableNameGenerator) {
        this.tableNameGenerator = tableNameGenerator;
    }
}