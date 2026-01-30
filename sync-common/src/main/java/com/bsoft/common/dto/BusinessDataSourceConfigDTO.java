package com.bsoft.common.dto;

/**
 * ClassName: BusinessDataSourceConfigDTO
 * Package:   com.bsoft.dto
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/5 - 14:30
 */

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.enums.DatabaseType;

import java.io.Serializable;

/**
 * ClassName: BusinessDataSourceConfigDTO
 * Package:   com.bsoft.bean
 * Description: 用于序列化传输的数据源配置 DTO，避免循环引用
 *
 * @Author: snoopy
 * @Create: 2025/1/17
 */
public class BusinessDataSourceConfigDTO implements Serializable {

    private String dataSourceId;             // 业务库主键
    private DatabaseType dataSourceType;           // oracle/mysql/doris
    private String clientId;                 // 客户端id

    public BusinessDataSourceConfigDTO() {
    }

    // 从 BusinessDataSourceConfig 转换为 DTO 的静态方法
    public static BusinessDataSourceConfigDTO from(BusinessDataSourceConfig config) {
        if (config == null) {
            return null;
        }

        BusinessDataSourceConfigDTO dto = new BusinessDataSourceConfigDTO();
        dto.setDataSourceId(config.getDataSourceId());
        dto.setDataSourceType(config.getDataSourceType());
        dto.setClientId(config.getClientId());
        return dto;
    }


    /**
     * 将 DTO 转换为原始的 BusinessDataSourceConfig 对象
     * 只设置 DTO 中存在的属性
     */
    public BusinessDataSourceConfig toBusinessDataSourceConfig() {
        BusinessDataSourceConfig config = new BusinessDataSourceConfig();
        config.setDataSourceId(this.dataSourceId);
        config.setDataSourceType(this.dataSourceType);
        config.setClientId(this.clientId);
        return config;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public DatabaseType getDataSourceType() {
        return dataSourceType;
    }

    public void setDataSourceType(DatabaseType dataSourceType) {
        this.dataSourceType = dataSourceType;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
