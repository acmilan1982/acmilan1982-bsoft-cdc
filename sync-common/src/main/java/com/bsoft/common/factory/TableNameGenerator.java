package com.bsoft.common.factory;


import com.bsoft.common.bean.*;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.util.DatasourceConfigFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * ClassName: TableNameGenerator
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/4 - 18:00
 */
@Component
public class TableNameGenerator {
    private static final Logger logger = LogManager.getLogger(TableNameGenerator.class);



    // 生成目标表名
    public String generateTargetTableName(TargetTableNamingRule targetTableNamingRule,
                                          String clientId,
                                          TableMappingConfig tableMappingConfig,
                                          DatabaseType databaseType) {
        // 1. 先根据命名策略生成表名
        String targetTableName = generateTargetTableName(targetTableNamingRule,clientId,tableMappingConfig.getSchemaName(), tableMappingConfig.getTableName());
        // 2. 再根据库的类型，选择 表名 大写还是 小写
        return applyDatabaseSpecificFormatting(targetTableName, databaseType);
    }




    // 生成目标表名,通常是因为是有同名表存在，且当前的表命名策略是 PREFIX_ON_CONFLICT(前缀)
    public String generateTargetTableName(String clientId,
                                          TableMappingConfig tableMappingConfig,
                                          DatabaseType databaseType,
                                          TableNamingStrategy tableNamingStrategy) {
        ;
        String targetTableName = generateTargetTableName(TargetTableNamingRule.builder().tableNamingStrategy(tableNamingStrategy).build(),
                                       clientId,tableMappingConfig.getSchemaName(), tableMappingConfig.getTableName());
        return applyDatabaseSpecificFormatting(targetTableName, databaseType);
    }



    // 根据 目标表的命名策略 生成表名
    public String  generateTargetTableName(TargetTableNamingRule targetTableNamingRule,
                                           ExecutionPlan.TableExecutionPlan tableExecutionPlan) {
        return generateTargetTableName(targetTableNamingRule,
                                       tableExecutionPlan.getClientId(),
                                       tableExecutionPlan.getSchemaName(),
                                       tableExecutionPlan.getTableName());
    }

    // 根据 目标表的命名策略 生成表名
    public String generateTargetTableName(TargetTableNamingRule targetTableNamingRule,
                                          String clientId,
                                          String sourceSchema,
                                          String sourceTableName) {
        // 命名策略
        TableNamingStrategy tableNamingStrategy = targetTableNamingRule.getTableNamingStrategy();
        // 目标表名
        String targetTableName;
        // 目标表的类型
        switch (tableNamingStrategy) {
            case TABLE_MERGE:
            case PREFIX_ON_CONFLICT:
                targetTableName = sourceTableName;
                break;
            case CUSTOM_PREFIX_SUFFIX:      // 自定义前缀,后缀
                targetTableName =  targetTableNamingRule.getTableNamePrefix() + sourceTableName + targetTableNamingRule.getTableNameSuffix();
                break;
            case CLIENTID_SCHEMA_TABLE:
                targetTableName = generateUniqueTableName(clientId, sourceSchema, sourceTableName);
                break;
            default:
                throw new IllegalStateException("不支持的目标表表名生成策略: " + tableNamingStrategy);
        }
        return targetTableName;
    }

    // 绝对唯一的表名生成方式
    public String generateUniqueTableName(String clientId, String schema, String tableName) {
        return clientId + "_" + schema + "_" + tableName;
    }

    // 根据 根据库的类型，选择 表名 大写还是 小写
    public String applyDatabaseSpecificFormatting(String tableName, DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
            case DORIS:
                return tableName.toLowerCase();
            case ORACLE:
                return tableName.toUpperCase();
            default:
                throw new IllegalStateException("不支持的数据库类型: " + databaseType);
        }
    }




    /**
     * tableNamingStrategy:TABLE_PREFIX模式下，处理重复的目标表名问题
     * 规则：重复的表名会添加前缀进行重命名
     * @param serverTopicInfoList ServerTopicInfo列表
     */
    public void handleDuplicateTargetTableNames(List<ServerTopicInfo> serverTopicInfoList,
                                                TargetDataSourceConfigRegistry targetDataSourceConfigRegistry,
                                                DatasourceConfigFetcher configFetcher) throws Exception {
        if (serverTopicInfoList == null || serverTopicInfoList.isEmpty()) {
            return;
        }

        // 按照targetDataSourceId分组,因为不同目标库可以有相同的表名
        Map<String, List<ServerTopicInfo>> tagetDataSourceGroups = new HashMap<>();
        for (ServerTopicInfo serverTopicInfo : serverTopicInfoList) {
            String targetDataSourceId = serverTopicInfo.getTargetDataSourceId();
            tagetDataSourceGroups.computeIfAbsent(targetDataSourceId, k -> new ArrayList<>()).add(serverTopicInfo);
        }

        // 对每个目标库分别处理重复表名
        for (Map.Entry<String, List<ServerTopicInfo>> dataSourceEntry : tagetDataSourceGroups.entrySet()) {
            // 当前目标库
            List<ServerTopicInfo> serverTopicInfos = dataSourceEntry.getValue();
            // 在同一数据源内,统计表名重复情况
            Map<String, List<ServerTopicInfo>> tableNameGroups = new HashMap<>();
            for (ServerTopicInfo topicInfo : serverTopicInfos) {
                String targetTableName = topicInfo.getTargetTableName();
                if (targetTableName != null && !targetTableName.trim().isEmpty()) {
                    tableNameGroups.computeIfAbsent(targetTableName, k -> new ArrayList<>()).add(topicInfo);
                }
            }

            // 处理重复的表名
            for (Map.Entry<String, List<ServerTopicInfo>> entry : tableNameGroups.entrySet()) {
                List<ServerTopicInfo> duplicateTopics = entry.getValue();

                if (duplicateTopics.size() > 1) {
                    logger.warn("⚠️ 数据源{}中发现重复的目标表名: {}，共{}个重复项，将进行重命名处理",
                            dataSourceEntry.getKey(), entry.getKey(), duplicateTopics.size());

                    for (ServerTopicInfo serverTopicInfo : duplicateTopics) {
                        TargetDataSourceConfig targetDataSourceConfig = targetDataSourceConfigRegistry.findTargetDataSourceConfigByDataSourceId(serverTopicInfo.getTargetDataSourceId());

                        // 查询目标表名，如果查询结果为null则每5秒重试一次，直到获得结果
                        String targetTableName = queryTargetTableNameWithRetry(serverTopicInfo, targetDataSourceConfig, configFetcher);

                        serverTopicInfo.setTargetTableName(targetTableName);
                    }
                }
            }
        }
    }

    /**
     * 查询目标表名，如果查询结果为null则每5秒重试一次，直到获得结果
     * @param serverTopicInfo 服务端主题信息
     * @param targetDataSourceConfig 目标数据源配置
     * @param configFetcher 配置获取器
     * @return 目标表名
     */
    private String queryTargetTableNameWithRetry(ServerTopicInfo serverTopicInfo,
                                                 TargetDataSourceConfig targetDataSourceConfig,
                                                 DatasourceConfigFetcher configFetcher) throws Exception {
        String targetTableName = null;
        int retryCount = 0;

        while (targetTableName == null) {
            targetTableName = configFetcher.queryTargetTableNameFromCreationLog(serverTopicInfo, targetDataSourceConfig);

            if (targetTableName == null) {
                retryCount++;
                logger.info("第{}次查询目标表名为null，5秒后重试... Topic: {}",
                        retryCount, serverTopicInfo.getTopic());

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("查询目标表名时被中断", e);
                    throw new RuntimeException("查询目标表名被中断", e);
                }
            }
        }

        logger.info("查询成功，目标表名: {}，重试次数: {}", targetTableName, retryCount);
        return targetTableName;
    }



}
