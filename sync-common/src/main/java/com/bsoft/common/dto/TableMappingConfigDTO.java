package com.bsoft.common.dto;

/**
 * ClassName: TableMappingConfigDTO
 * Package:   com.bsoft.dto
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/5 - 14:29
 */

import com.bsoft.common.bean.TableKeys;
import com.bsoft.common.bean.TableMappingConfig;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ClassName: TableMappingConfigDTO
 * Package:   com.bsoft.bean
 * Description: 用于序列化传输的 DTO，避免循环引用
 *
 * @Author: snoopy
 * @Create: 2025/1/17
 */
public class TableMappingConfigDTO implements Serializable {

    // 包含数据源配置信息（不包含循环引用的字段）
    private BusinessDataSourceConfigDTO businessDataSourceConfig;

    // 源库schema
    private String sourceSchema;
    // 源表名
    private String sourceTableName;
    // DDL
    private String ddl;

    // 表的键信息，包括主键、唯一约束、唯一索引、普通索引等
    private TableKeys tableKeys;

    public TableMappingConfigDTO() {
    }

    // 从 TableMappingConfig 转换为 DTO 的静态方法
    public static TableMappingConfigDTO from(TableMappingConfig config) {
        if (config == null) {
            return null;
        }

        TableMappingConfigDTO dto = new TableMappingConfigDTO();

        // 转换数据源配置（避免循环引用）
        dto.setBusinessDataSourceConfig(
                BusinessDataSourceConfigDTO.from(config.getBusinessDataSourceConfig())
        );

        dto.setSourceSchema(config.getSchemaName());
        dto.setSourceTableName(config.getTableName());
        dto.setDdl(config.getDdl());
        dto.setTableKeys(config.getTableKeys());

        return dto;
    }

    // 批量转换方法
    public static List<TableMappingConfigDTO> fromList(List<TableMappingConfig> configList) {
        if (configList == null) {
            return null;
        }
        return configList.stream()
                .map(TableMappingConfigDTO::from)
                .collect(Collectors.toList());
    }


    /**
     * 将 DTO 转换为原始的 TableMappingConfig 对象
     * 只设置 DTO 中存在的属性
     */
    public TableMappingConfig toTableMappingConfig() {
        TableMappingConfig config = new TableMappingConfig();

        // 设置数据源配置
        if (this.businessDataSourceConfig != null) {
            config.setBusinessDataSourceConfig(this.businessDataSourceConfig.toBusinessDataSourceConfig());
        }

        config.setSchemaName(this.sourceSchema);
        config.setTableName(this.sourceTableName);
        config.setDdl(this.ddl);

        return config;
    }

    public BusinessDataSourceConfigDTO getBusinessDataSourceConfig() {
        return businessDataSourceConfig;
    }

    public void setBusinessDataSourceConfig(BusinessDataSourceConfigDTO businessDataSourceConfig) {
        this.businessDataSourceConfig = businessDataSourceConfig;
    }

    public String getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public TableKeys getTableKeys() {
        return tableKeys;
    }

    public void setTableKeys(TableKeys tableKeys) {
        this.tableKeys = tableKeys;
    }
}