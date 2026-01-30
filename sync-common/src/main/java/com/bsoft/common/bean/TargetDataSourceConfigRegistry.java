// BusinessDataSourceConfig.java
package com.bsoft.common.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author snoopy
 */
// 目标库配置
public class TargetDataSourceConfigRegistry implements Serializable {

    // 以ToDataSourceId为key,
    private Map<String, TargetDataSourceConfig> mappingMapByDataSourceId = new HashMap<>();; // 以ID为key的快速查找

    private List<TargetDataSourceConfig> targetDataSourceConfigList = new ArrayList<>();


    public TargetDataSourceConfigRegistry() {
    }


    public void register(TargetDataSourceConfig targetDataSourceConfig) {
        targetDataSourceConfigList.add(targetDataSourceConfig);
        mappingMapByDataSourceId.put(targetDataSourceConfig.getDataSourceId(), targetDataSourceConfig);

    }


    //
    public TargetDataSourceConfig findTargetDataSourceConfigByDataSourceId(String dataSourceId) {
        return mappingMapByDataSourceId.get(dataSourceId);
    }

    public List<TargetDataSourceConfig> getTargetDataSourceConfigList() {
        return targetDataSourceConfigList;
    }

    public void setTargetDataSourceConfigList(List<TargetDataSourceConfig> targetDataSourceConfigList) {
        this.targetDataSourceConfigList = targetDataSourceConfigList;
    }
}