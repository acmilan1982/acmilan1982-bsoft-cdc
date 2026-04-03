package com.bsoft.server.api;

import com.bsoft.common.bean.DDLBuildContext;
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

    // 为 null 表示按源表 + 命名策略推导目标表名
    // 非 null 表示目标表名已经确定，直接使用
    private final String fixedTargetTableName;

    private CreateTableContext(SimpleClientInfo clientInfo,
                               TableMappingConfigDTO tableMappingConfigDTO,
                               TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                               DatabaseType databaseType,
                               DDLBuildContext ddlBuildContext,
                               String fixedTargetTableName) {
        this.clientInfo = Objects.requireNonNull(clientInfo, "clientInfo");
        this.tableMappingConfigDTO = Objects.requireNonNull(tableMappingConfigDTO, "tableMappingConfigDTO");
        this.tableMetadataReaderWriterPair = Objects.requireNonNull(tableMetadataReaderWriterPair, "tableMetadataReaderWriterPair");
        this.databaseType = Objects.requireNonNull(databaseType, "databaseType");
        this.ddlBuildContext = Objects.requireNonNull(ddlBuildContext, "ddlBuildContext");
        this.fixedTargetTableName = fixedTargetTableName;
    }

    public static CreateTableContext forSourceTable(SimpleClientInfo clientInfo,
                                                    TableMappingConfigDTO tableMappingConfigDTO,
                                                    TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                                                    DatabaseType databaseType,
                                                    DDLBuildContext ddlBuildContext) {
        return new CreateTableContext(
                clientInfo,
                tableMappingConfigDTO,
                tableMetadataReaderWriterPair,
                databaseType,
                ddlBuildContext,
                null
        );
    }

    public static CreateTableContext forFixedTargetTable(SimpleClientInfo clientInfo,
                                                         TableMappingConfigDTO tableMappingConfigDTO,
                                                         TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
                                                         DatabaseType databaseType,
                                                         DDLBuildContext ddlBuildContext,
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
                fixedTargetTableName.trim()  // 直接指定 目标表名，无需再生成
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

    public TableNamingStrategy getTableNamingStrategy() {
        return clientInfo.getTableNamingStrategy();
    }

    public String getTableNamePrefix() {
        return clientInfo.getTableNamePrefix();
    }

    public String getTableNameSuffix() {
        return clientInfo.getTableNameSuffix();
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