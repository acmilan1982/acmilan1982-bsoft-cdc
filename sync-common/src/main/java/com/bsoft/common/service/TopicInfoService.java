package com.bsoft.common.service;

import com.bsoft.common.bean.*;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.JobStartupMode;
import com.bsoft.common.factory.TableNameGenerator;
import com.bsoft.common.factory.TopicNameFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 统一负责从 ExecutionPlan 派生 Topic 相关视图数据。
 *
 * 职责边界：
 * 1. 生成客户端消费/写出的 ClientTopicInfo
 * 2. 生成服务端监听/落库用的 ServerTopicInfo
 * 3. 根据业务库配置中的 TargetTableNamingRule 解析最终目标表名
 *
 * 注意：
 * - ExecutionPlan 本身只保留“计划数据”，不再持有工厂或生成器。
 * - 所有 Topic 相关派生逻辑统一收敛到这里，避免散落在模型对象和入口类中。
 */
@Component
public class TopicInfoService {

    @Autowired
    private TableNameGenerator tableNameGenerator;

    /**
     * 根据启动模式，从执行计划中生成当前需要的客户端 Topic 列表。
     *
     * NORMAL：使用全部表计划
     * SNAPSHOT：只使用处于快照阶段的表计划
     */
    public List<ClientTopicInfo> generateClientTopicInfo(ExecutionPlan executionPlan, JobStartupMode jobStartupMode) {
        if (executionPlan == null || executionPlan.getTableExecutionPlans() == null) {
            return Collections.emptyList();
        }

        // 快照模式下只保留处于 SNAPSHOT 状态的表计划
        if (jobStartupMode == JobStartupMode.SNAPSHOT) {
            List<ExecutionPlan.TableExecutionPlan> snapshotPlans = executionPlan.getTableExecutionPlans()
                    .stream()
                    .filter(ExecutionPlan.TableExecutionPlan::isInSnapshotState)
                    .collect(Collectors.toList());
            return generateClientTopicInfo(snapshotPlans);
        }

        // 其他模式默认使用完整表计划列表
        return generateClientTopicInfo(executionPlan.getTableExecutionPlans());
    }

    /**
     * 直接根据表级执行计划列表生成客户端 Topic 信息。
     */
    public List<ClientTopicInfo> generateClientTopicInfo(List<ExecutionPlan.TableExecutionPlan> tableExecutionPlanList) {
        if (tableExecutionPlanList == null || tableExecutionPlanList.isEmpty()) {
            return Collections.emptyList();
        }

        List<ClientTopicInfo> clientTopicInfoList = new ArrayList<>();
        // 每个表计划对应一个客户端 Topic 视图
        for (ExecutionPlan.TableExecutionPlan tableExecutionPlan : tableExecutionPlanList) {
            clientTopicInfoList.add(generateBasicTopicInfo(tableExecutionPlan));
        }
        return clientTopicInfoList;
    }

    /**
     * 根据完整执行计划生成服务端 Topic 信息。
     *
     * 每个 TableExecutionPlan 都会关联：
     * - 一个源业务库 BusinessDataSourceConfig
     * - 一个目标库 TargetDataSourceConfig
     *
     * 然后进一步解析出最终目标表名。
     */
    public List<ServerTopicInfo> generateServerTopicInfo(ExecutionPlan executionPlan,
                                                         BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                                         TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
        if (executionPlan == null || executionPlan.getTableExecutionPlans() == null) {
            return Collections.emptyList();
        }

        List<ServerTopicInfo> serverTopicInfoList = new ArrayList<>();
        for (ExecutionPlan.TableExecutionPlan tableExecutionPlan : executionPlan.getTableExecutionPlans()) {
            String fromDataSourceId = tableExecutionPlan.getFromDataSourceId();
            String toDataSourceId = tableExecutionPlan.getToDataSourceId();

            // 先定位当前表计划对应的源业务库和目标库配置
            BusinessDataSourceConfig businessDataSourceConfig =
                    businessDataSourceConfigRegistry.findBusinessDataSourceConfigByDataSourceId(fromDataSourceId);
            TargetDataSourceConfig targetDataSourceConfig =
                    targetDataSourceConfigRegistry.findTargetDataSourceConfigByDataSourceId(toDataSourceId);

            if (businessDataSourceConfig == null) {
                throw new IllegalStateException("未找到业务库配置, fromDataSourceId=" + fromDataSourceId);
            }
            if (targetDataSourceConfig == null) {
                throw new IllegalStateException("未找到目标库配置, toDataSourceId=" + toDataSourceId);
            }

            serverTopicInfoList.add(generateServerTopicInfo(
                    businessDataSourceConfig,
                    targetDataSourceConfig,
                    tableExecutionPlan
            ));
        }

        return serverTopicInfoList;
    }

    /**
     * 为单个表级执行计划生成服务端 Topic 信息。
     *
     * 与客户端 Topic 相比，服务端 Topic 多了“最终目标表名”这一派生结果。
     */
    public ServerTopicInfo generateServerTopicInfo(BusinessDataSourceConfig businessDataSourceConfig,
                                                   TargetDataSourceConfig targetDataSourceConfig,
                                                   ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        // 先生成 topic / schema / table 等基础信息
        ServerTopicInfo serverTopicInfo =   generateBasicTopicInfo(tableExecutionPlan);

        // 再根据命名规则解析最终目标表名
        String targetTableName = resolveTargetTableName(
                businessDataSourceConfig,
                targetDataSourceConfig,
                tableExecutionPlan
        );

        serverTopicInfo.setTargetTableName(targetTableName);
        serverTopicInfo.setClientInfo(tableExecutionPlan.getClientInfo());
        return serverTopicInfo;
    }

    /**
     * 统一解析目标表名。
     *
     * 规则来源于业务库配置中的 TargetTableNamingRule，
     * 最终格式再根据目标库类型（Oracle/MySQL/Doris）做大小写规范化。
     */
    public String resolveTargetTableName(BusinessDataSourceConfig businessDataSourceConfig,
                                         TargetDataSourceConfig targetDataSourceConfig,
                                         ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        if (businessDataSourceConfig == null) {
            throw new IllegalArgumentException("businessDataSourceConfig 不能为空");
        }
        if (targetDataSourceConfig == null) {
            throw new IllegalArgumentException("targetDataSourceConfig 不能为空");
        }
        if (tableExecutionPlan == null) {
            throw new IllegalArgumentException("tableExecutionPlan 不能为空");
        }

        // 命名规则来自源业务库配置
        TargetTableNamingRule targetTableNamingRule = requireTargetTableNamingRule(businessDataSourceConfig,targetDataSourceConfig);
        DatabaseType databaseType = targetDataSourceConfig.getDataSourceType();

        return generateTargetTableName(targetTableNamingRule, databaseType, tableExecutionPlan);
    }

    /**
     * 严格获取业务库对应的目标表命名规则。
     */
    public TargetTableNamingRule requireTargetTableNamingRule(BusinessDataSourceConfig businessDataSourceConfig,
                                                              TargetDataSourceConfig targetDataSourceConfig) {
        TargetTableNamingRule targetTableNamingRule = businessDataSourceConfig.getTargetTableNamingRuleByTargetDataSourceId(targetDataSourceConfig.getDataSourceId());
        if (targetTableNamingRule == null) {
            throw new IllegalStateException("未配置目标表命名规则, businessDataSourceId="
                    + businessDataSourceConfig.getDataSourceId());
        }
        return targetTableNamingRule;
    }

    /**
     * 生成基础 Topic 信息。
     *
     * 这里不关心目标表命名规则，只负责：
     * - 复制源/目标数据源信息
     * - 复制 schema/table
     * - 生成 Kafka topic 名
     */
    public ServerTopicInfo generateBasicTopicInfo(ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        ServerTopicInfo serverTopicInfo = new ServerTopicInfo();

        // 复制表计划中的基础上下文到 Topic 视图对象
        serverTopicInfo.setBusinessDataSourceId(tableExecutionPlan.getFromDataSourceId());
        serverTopicInfo.setTargetDataSourceId(tableExecutionPlan.getToDataSourceId());
        serverTopicInfo.setSchemaName(tableExecutionPlan.getSchemaName());
        serverTopicInfo.setTableName(tableExecutionPlan.getTableName());
        serverTopicInfo.setClientInfo(tableExecutionPlan.getClientInfo());

        // topic 名只依赖 clientId、schema、table 和目标库 id
        serverTopicInfo.setTopic(generateTopic(tableExecutionPlan.getClientId(), tableExecutionPlan));
        return serverTopicInfo;
    }

    /**
     * 按统一规则生成 Kafka topic 名。
     */
    public String generateTopic(String clientId, ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        return TopicNameFactory.generateTopic(
                clientId,
                tableExecutionPlan.getFromDataSourceId(),
                tableExecutionPlan.getSchemaName(),
                tableExecutionPlan.getTableName(),
                tableExecutionPlan.getToDataSourceId()
        );
    }

    /**
     * 根据命名规则生成目标表名，并按目标库类型应用大小写规范。
     */
    public String generateTargetTableName(TargetTableNamingRule targetTableNamingRule,
                                          DatabaseType databaseType,
                                          ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        // 先按命名策略生成逻辑表名
        String targetTableName = tableNameGenerator.generateTargetTableName(targetTableNamingRule, tableExecutionPlan);

        // 再按目标库类型统一大小写
        return tableNameGenerator.applyDatabaseSpecificFormatting(targetTableName, databaseType);
    }
}