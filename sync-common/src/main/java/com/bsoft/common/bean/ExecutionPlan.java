package com.bsoft.common.bean;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.enums.JobStartupMode;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.enums.TableStatus;
import com.bsoft.common.factory.TopicInfoFactory;
import com.bsoft.common.util.DatasourceConfigFetcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ClassName: ExecutionPlan
 * Package:   com.bsoft.bean
 * Description: 执行计划容器，包含多个执行计划项
 *
 * @Author: snoopy
 * @Create: 2025/7/27 - 13:19
 */
@Component
public class ExecutionPlan implements Serializable {

    // 上一次 表级别 执行计划
    private List<TableExecutionPlan> tableExecutionPlans = new ArrayList<>();

    // 本次新增的 表级执行计划
    List<ExecutionPlan.TableExecutionPlan> addedTableExecutionPlan = new ArrayList<>();
    // 本次删除的 表级执行计划
    List<ExecutionPlan.TableExecutionPlan> deletedTableExecutionPlan = new ArrayList<>();

    // 当前是否需要执行快照模式,只要有一个表处于快照状态，就需要执行快照模式
    boolean hasTablesInSnapshotState = false;


    @Autowired
    private TopicInfoFactory topicInfoFactory;

    public ExecutionPlan() {

    }

    public ExecutionPlan(List<TableExecutionPlan> planItems) {
        this.tableExecutionPlans = planItems != null ? planItems : new ArrayList<>();
    }

    // 如果是服务端的执行计划，此时，deletedTableExecutionPlan和tableExecutionPlans都是null
    // 基本上就是把addedTableExecutionPlan都加到tableExecutionPlans
    public void applyChanges() {
        // 删除要删除的表执行计划
        if (deletedTableExecutionPlan != null && !deletedTableExecutionPlan.isEmpty()) {
            tableExecutionPlans.removeAll(deletedTableExecutionPlan);
        }

        // 添加新增的表执行计划
        if (addedTableExecutionPlan != null && !addedTableExecutionPlan.isEmpty()) {
            tableExecutionPlans.addAll(addedTableExecutionPlan);
        }

        hasTablesInSnapshotState = tableExecutionPlans.stream()
                .anyMatch(plan -> plan.getTableStatus() == TableStatus.SNAPSHOT);

    }


    public List<TableExecutionPlan> getTableExecutionPlans() {
        return tableExecutionPlans;
    }






    // 便利方法：添加单个执行计划项
    public void addPlanItem(TableExecutionPlan item) {
        if (this.tableExecutionPlans == null) {
            this.tableExecutionPlans = new ArrayList<>();
        }
        this.tableExecutionPlans.add(item);
    }

    // 便利方法：添加执行计划项
    public void addPlanItem(String fromDataSourceId, String toDataSourceId, String schemaName, String tableName, String tableStatus, SimpleClientInfo clientInfo) {
        addPlanItem(new TableExecutionPlan(fromDataSourceId,
                                           toDataSourceId,
                                           schemaName,
                                           tableName,
                                           TableStatus.valueOf(tableStatus),
                                           clientInfo
        ));
    }

    // 便利方法：获取计划项数量
    public int size() {
        return tableExecutionPlans != null ? tableExecutionPlans.size() : 0;
    }

    // 便利方法：检查计划项是否为空（没有元素）
    public boolean hasNoPlanItems() {
        return tableExecutionPlans == null || tableExecutionPlans.isEmpty();
    }


    public boolean hasTablesInSnapshotState() {
        return hasTablesInSnapshotState;
    }



    /**
     * 将执行计划项列表转换为Map
     * @return Map<String, ExecutionPlanItem> key为toDataSourceId+schemaName+tableName，value为ExecutionPlanItem
     */
    public Map<String, TableExecutionPlan> toMap() {
        if (tableExecutionPlans == null || tableExecutionPlans.isEmpty()) {
            return new HashMap<>();
        }

        return tableExecutionPlans.stream()
                .collect(Collectors.toMap(
                        item -> item.getToDataSourceId() + "." + item.getSchemaName() + "." + item.getTableName(),
                        item -> item,
                        (existing, replacement) -> replacement // 如果有重复key，使用新的值
                ));
    }


    public List<TableExecutionPlan> getAddedTableExecutionPlan() {
        return addedTableExecutionPlan;
    }

    public void setAddedTableExecutionPlan(List<TableExecutionPlan> addedTableExecutionPlan) {
        this.addedTableExecutionPlan = addedTableExecutionPlan;
    }

    public List<TableExecutionPlan> getDeletedTableExecutionPlan() {
        return deletedTableExecutionPlan;
    }

    public void setDeletedTableExecutionPlan(List<TableExecutionPlan> deletedTableExecutionPlan) {
        this.deletedTableExecutionPlan = deletedTableExecutionPlan;
    }






    //---------------客户端，中心端生成主题的方式统一在这里，最后都会调用topicInfoFactory去生成----------------

    // 根据客户端的启动模式，生成当前客户端所需的所有主题
    public List<ClientTopicInfo> generateClientTopicInfo(ClientInfo clientInfo) {

        topicInfoFactory.setClientInfo(clientInfo);

        JobStartupMode jobStartupMode = clientInfo.getJobStartupMode();
        // 如果是正常模式，生成普通模式的主题信息
        if (jobStartupMode == JobStartupMode.NORMAL) {
            return generateNormalClientTopicInfo(clientInfo);
        }

        // 如果是快照模式，生成快照模式的主题信息
        if (jobStartupMode == JobStartupMode.SNAPSHOT) {
            return generateSnapShotClientTopicInfo(clientInfo);
        }

        return new ArrayList<>(); // 如果不是正常模式或快照模式，返回空列表
    }

    // 生成普通模式的主题信息
    public List<ClientTopicInfo> generateNormalClientTopicInfo(ClientInfo clientInfo) {
        return generateClientTopicInfo(clientInfo,tableExecutionPlans);
    }

    // 生成快照模式的主题信息
    public List<ClientTopicInfo> generateSnapShotClientTopicInfo(ClientInfo clientInfo) {
        // 处于快照模式的执行计划
        List<ExecutionPlan.TableExecutionPlan> snapshotTableExecutionPlans =
                this.tableExecutionPlans.stream().filter(ExecutionPlan.TableExecutionPlan::isInSnapshotState).collect(Collectors.toList());
        return generateClientTopicInfo(clientInfo,snapshotTableExecutionPlans);
    }

    // 根据执行计划，生成的主题信息
    public List<ClientTopicInfo> generateClientTopicInfo(ClientInfo clientInfo,
                                  List<ExecutionPlan.TableExecutionPlan> tableExecutionPlanList) {

        // 根据执行计划来生成主题
        return topicInfoFactory.generateClientTopicInfo(tableExecutionPlanList);
    }


    // 根据执行计划，生成服务端的主题信息，同时会生成每个主题对应的目标表名
    public List<ServerTopicInfo> generateServerTopicInfo(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                        TargetDataSourceConfigRegistry targetDataSourceConfigRegistry,
                                        ServerInfo serverInfo,
                                        DatasourceConfigFetcher configFetcher) throws Exception {

        topicInfoFactory.setServerInfo(serverInfo);
        return  topicInfoFactory.generateServerTopicInfo(businessDataSourceConfigRegistry,
                                                         targetDataSourceConfigRegistry,
                                                         this.tableExecutionPlans,
                                                          configFetcher);



    }



    /**
     * 执行计划项内部类，表示单个表的执行信息
     */
    public static class TableExecutionPlan implements Serializable {

        // 业务库数据源id
        private String fromDataSourceId;

        // 目标库数据源id
        private String toDataSourceId;

        // schema
        private String schemaName;
        // 表名
        private String tableName;
        // 状态
        private TableStatus tableStatus;

        // 客户端相关配置聚合到一个对象里
        private SimpleClientInfo clientInfo;

        public TableExecutionPlan() {
        }

        public TableExecutionPlan(String fromDataSourceId, String toDataSourceId, String schemaName, String tableName, TableStatus tableStatus,SimpleClientInfo clientInfo) {
            this.fromDataSourceId = fromDataSourceId;
            this.toDataSourceId = toDataSourceId;
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.tableStatus = tableStatus;
            this.clientInfo = clientInfo;
        }

        public String getFromDataSourceId() {
            return fromDataSourceId;
        }

        public void setFromDataSourceId(String fromDataSourceId) {
            this.fromDataSourceId = fromDataSourceId;
        }

        public String getToDataSourceId() {
            return toDataSourceId;
        }

        public void setToDataSourceId(String toDataSourceId) {
            this.toDataSourceId = toDataSourceId;
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

        public TableStatus getTableStatus() {
            return tableStatus;
        }

        public void setTableStatus(TableStatus tableStatus) {
            this.tableStatus = tableStatus;
        }

        public String getKey() {
            return this.toDataSourceId + "." + this.schemaName  + "." + this.tableName;
        }

        public boolean isInSnapshotState() {
            return tableStatus == TableStatus.SNAPSHOT;
        }


        // 兼容旧调用方，后续可以逐步删掉
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


}