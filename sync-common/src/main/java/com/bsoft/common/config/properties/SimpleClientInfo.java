package com.bsoft.common.config.properties;

import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


// 基本上表示CDC_CLIENT中的单条记录
public class SimpleClientInfo extends BaseInfo implements Serializable {


    // 客户端ID，每个客户端都有唯一的id
    private String clientId;

    /**
     * 当前客户端负责采集的数据源 ID 列表
     *
     * 约定：
     * 1. 使用逗号分隔
     * 2. 例如：ds001,ds002,ds003
     */
    private String dataSourceIds;


//    // 客户端在目标库的表命名策略
//    private TableNamingStrategy tableNamingStrategy;

    // 客户端在目标库的自定义表名前缀
    private String tableNamePrefix;

    // 客户端在目标库的自定义表名后缀
    private String tableNameSuffix;



    public SimpleClientInfo()  {

    }


    /**
     * 将逗号分隔的 dataSourceIds 解析为有序去重的列表。
     */
    public List<String> getDataSourceIdList() {
        // 原始值为空时返回空列表
        if (dataSourceIds == null || dataSourceIds.trim().isEmpty()) {
            return Collections.emptyList();
        }

        // 按逗号拆分并规整为列表
        return Arrays.stream(dataSourceIds.split(","))
                .map(String::trim) // 去掉首尾空格
                .filter(id -> !id.isEmpty()) // 过滤空字符串
                .distinct() // 保持顺序去重
                .collect(Collectors.toList()); // 收集为列表
    }

    /**
     * 判断当前客户端是否包含指定数据源 ID。
     */
    public boolean containsDataSourceId(String dataSourceId) {
        // 入参为空时直接返回 false
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            return false;
        }

        // 使用解析后的列表进行包含判断
        return getDataSourceIdList().contains(dataSourceId.trim());
    }








    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


//    public TableNamingStrategy getTableNamingStrategy() {
//        return tableNamingStrategy;
//    }
//
//    public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
//        this.tableNamingStrategy = tableNamingStrategy;
//    }

    public String getTableNamePrefix() {
        return tableNamePrefix;
    }

    public void setTableNamePrefix(String tableNamePrefix) {
        this.tableNamePrefix = tableNamePrefix;
    }

    public String getTableNameSuffix() {
        return tableNameSuffix;
    }

    public void setTableNameSuffix(String tableNameSuffix) {
        this.tableNameSuffix = tableNameSuffix;
    }


    public String getDataSourceIds() {
        return dataSourceIds;
    }

    public void setDataSourceIds(String dataSourceIds) {
        this.dataSourceIds = dataSourceIds;
    }
}
