// BusinessDataSourceConfig.java
package com.bsoft.common.bean;

import java.io.Serializable;
import java.util.*;

/**
 * @author snoopy
 */
public class BusinessDataSourceConfigRegistry implements Serializable {

    // 以业务库的dataSourceId为key,
    private Map<String, BusinessDataSourceConfig> mappingMapByDataSourceId = new HashMap<>();; // 以ID为key的快速查找

    private List<BusinessDataSourceConfig> businessDataSourceConfigList = new ArrayList<>();


    public BusinessDataSourceConfigRegistry() {
    }

    public void afterPropertiesSet() {
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigList) {
            mappingMapByDataSourceId.computeIfAbsent(businessDataSourceConfig.getDataSourceId(), k -> businessDataSourceConfig);
        }
    }

    public void register(BusinessDataSourceConfig businessDataSourceConfig) {
        businessDataSourceConfigList.add(businessDataSourceConfig);
        mappingMapByDataSourceId.computeIfAbsent(businessDataSourceConfig.getDataSourceId(), k -> businessDataSourceConfig);
    }


    public List<BusinessDataSourceConfig> getBusinessDataSourceConfigList() {
        return businessDataSourceConfigList;
    }

    public void setBusinessDataSourceConfigList(List<BusinessDataSourceConfig> businessDataSourceConfigList) {
        this.businessDataSourceConfigList = businessDataSourceConfigList;
    }

    public BusinessDataSourceConfig findBusinessDataSourceConfigByDataSourceId(String dataSourceId) {
        return mappingMapByDataSourceId.get(dataSourceId);
    }

    public boolean isEmpty() {
        return mappingMapByDataSourceId == null
                || mappingMapByDataSourceId.isEmpty()
                || businessDataSourceConfigList == null
                || businessDataSourceConfigList.isEmpty();
    }

}