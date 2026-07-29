package com.bsoft.server.api;

import com.bsoft.common.bean.DDLBuildContext;
import com.bsoft.common.bean.TargetTableNamingRule;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.server.sql.adapter.meta.TableMetadataReaderWriterPair;

import java.util.Objects;

public class CreateTableContext {
    private final SimpleClientInfo clientInfo;
    private final TableMappingConfigDTO tableMappingConfigDTO;
    private final TableMetadataReaderWriterPair tableMetadataReaderWriterPair;
    private final DatabaseType databaseType;
    private final DDLBuildContext ddlBuildContext;
    private final TargetTableNamingRule targetTableNamingRule;

    // 为 null 表示按源表 + 命名策略推导目标表名
    // 非 null 表示目标表名已经确定，直接使用
    private final String fixedTargetTableName;

    private CreateTableContext(SimpleClientInfo clientInfo,
                               TableMappingConfigDTO tableMappingConfigDTO,
                               TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                               DatabaseType databaseType,
                               DDLBuildContext ddlBuildContext,
                               TargetTableNamingRule targetTableNamingRule,
                               String fixedTargetTableName) {
        this.clientInfo = Objects.requireNonNull(clientInfo, "clientInfo");
        this.tableMappingConfigDTO = Objects.requireNonNull(tableMappingConfigDTO, "tableMappingConfigDTO");
        this.tableMetadataReaderWriterPair = Objects.requireNonNull(tableMetadataReaderWriterPair, "tableMetadataReaderWriterPair");
        this.databaseType = Objects.requireNonNull(databaseType, "databaseType");
        this.ddlBuildContext = Objects.requireNonNull(ddlBuildContext, "ddlBuildContext");
        this.targetTableNamingRule = Objects.requireNonNull(targetTableNamingRule, "targetTableNamingRule");
        this.fixedTargetTableName = fixedTargetTableName;
    }



    public static CreateTableContext forSourceTable(SimpleClientInfo clientInfo,
                                                    TableMappingConfigDTO tableMappingConfigDTO,
                                                    TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                                                    DatabaseType databaseType,
                                                    DDLBuildContext ddlBuildContext,
                                                    TargetTableNamingRule targetTableNamingRule) {
        return new CreateTableContext(
                clientInfo,
                tableMappingConfigDTO,
                tableMetadataReaderWriterPair,
                databaseType,
                ddlBuildContext,
                targetTableNamingRule,
                null
        );
    }

    public static CreateTableContext forFixedTargetTable(SimpleClientInfo clientInfo,
                                                         TableMappingConfigDTO tableMappingConfigDTO,
                                                         TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                                                         DatabaseType databaseType,
                                                         DDLBuildContext ddlBuildContext,
                                                         TargetTableNamingRule targetTableNamingRule,
                                                         String fixedTargetTableName) {
        if (fixedTargetTableName == null || fixedTargetTableName.trim().isEmpty()) {
            throw new IllegalArgumentException("fixedTargetTableName 不能为空");
        }
        return new CreateTableContext(
                clientInfo,
                tableMappingConfigDTO,
                tableMetadataReaderWriterPair,
                databaseType,
                ddlBuildContext,
                targetTableNamingRule,
                fixedTargetTableName.trim()
        );
    }

    public boolean needsConstraints() {
        return databaseType != DatabaseType.DORIS;
    }

    public boolean usesFixedTargetTableName() {
        return fixedTargetTableName != null && !fixedTargetTableName.isEmpty();
    }

    public String getFixedTargetTableName() {
        return fixedTargetTableName;
    }

    public SimpleClientInfo getClientInfo() {
        return clientInfo;
    }

    public String getClientId() {
        return clientInfo.getClientId();
    }

    public TargetTableNamingRule getTargetTableNamingRule() {
        return targetTableNamingRule;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return targetTableNamingRule.getTableNamingStrategy();
    }

    public String getTableNamePrefix() {
        return targetTableNamingRule.getTableNamePrefix();
    }

    public String getTableNameSuffix() {
        return targetTableNamingRule.getTableNameSuffix();
    }

    public TableMappingConfigDTO getTableMappingConfigDTO() {
        return tableMappingConfigDTO;
    }

    public TableMetadataReaderWriterPair getTableMetadataReaderWriterPair() {
        return tableMetadataReaderWriterPair;
    }

    public DatabaseType getDatabaseType() {
        return databaseType;
    }

    public DDLBuildContext getDdlBuildContext() {
        return ddlBuildContext;
    }
}