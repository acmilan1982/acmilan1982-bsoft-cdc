package com.bsoft.common.bean;

import com.bsoft.common.enums.DorisColumnLengthModel;
import com.bsoft.common.enums.DorisColumnModel;
import com.bsoft.common.enums.DorisTableModel;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * ClassName: DorisBizAttr
 * Package:   com.bsoft.common.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/18 - 9:53
 */
public class DorisBizAttr implements DdlBizAttr {
    /** UNIQUE / DUPLICATE */
    @JsonProperty("table_model")
    private DorisTableModel tableModel;

    /** SAME_AS_SOURCE / ALL_VARCHAR */
    @JsonProperty("column_model")
    private DorisColumnModel columnModel;


    /** KEY 字段，允许多个 */
    @JsonProperty("key_columns")
    private List<String> keyColumns;

    /** DISTRIBUTED BY HASH 字段，允许多个 */
    @JsonProperty("distributed_hash_columns")
    private List<String> distributedHashColumns;

    /** 自定义列映射：目标列 -> 源列 */
    @JsonProperty("custom_column_mapping")
    private Map<String, String> customColumnMapping;


    @JsonProperty("buckets")
    private int buckets;

    @JsonProperty("replication_num")
    private int replicationNum;

    public DorisTableModel getTableModel() {
        return tableModel;
    }

    public void setTableModel(DorisTableModel tableModel) {
        this.tableModel = tableModel;
    }

    public DorisColumnModel getColumnModel() {
        return columnModel;
    }

    public void setColumnModel(DorisColumnModel columnModel) {
        this.columnModel = columnModel;
    }



    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
        this.keyColumns = keyColumns;
    }

    public List<String> getDistributedHashColumns() {
        return distributedHashColumns;
    }

    public void setDistributedHashColumns(List<String> distributedHashColumns) {
        this.distributedHashColumns = distributedHashColumns;
    }

    public Map<String, String> getCustomColumnMapping() {
        return customColumnMapping;
    }

    public void setCustomColumnMapping(Map<String, String> customColumnMapping) {
        this.customColumnMapping = customColumnMapping;
    }


    public int getBuckets() {
        return buckets;
    }

    public void setBuckets(int buckets) {
        this.buckets = buckets;
    }

    public int getReplicationNum() {
        return replicationNum;
    }

    public void setReplicationNum(int replicationNum) {
        this.replicationNum = replicationNum;
    }
}