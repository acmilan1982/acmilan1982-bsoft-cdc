package com.bsoft.common.bean;

import com.bsoft.common.enums.TableNamingStrategy;

import java.io.Serializable;

public class TargetTableNamingRule implements Serializable {

    private String dataSourceId;
    private TableNamingStrategy tableNamingStrategy;
    private String tableNamePrefix;
    private String tableNameSuffix;
    private String targetDataSourceId;

    public TargetTableNamingRule() {
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TargetTableNamingRule instance = new TargetTableNamingRule();

        public Builder dataSourceId(String val) {
            instance.setDataSourceId(val);
            return this;
        }

        public Builder tableNamingStrategy(TableNamingStrategy val) {
            instance.setTableNamingStrategy(val);
            return this;
        }

        public Builder tableNamePrefix(String val) {
            instance.setTableNamePrefix(val);
            return this;
        }

        public Builder tableNameSuffix(String val) {
            instance.setTableNameSuffix(val);
            return this;
        }
        public Builder targetDataSourceId(String val) {
            instance.setTargetDataSourceId(val);
            return this;
        }

        public TargetTableNamingRule build() {
            return instance;
        }
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public TableNamingStrategy getTableNamingStrategy() {
        return tableNamingStrategy;
    }

    public void setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
        this.tableNamingStrategy = tableNamingStrategy;
    }

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

    public String getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public void setTargetDataSourceId(String targetDataSourceId) {
        this.targetDataSourceId = targetDataSourceId;
    }

    @Override
    public String toString() {
        return "TargetTableNamingRule{" +
                "dataSourceId='" + dataSourceId + '\'' +
                ", targetDataSourceId=" + targetDataSourceId +
                ", tableNamingStrategy=" + tableNamingStrategy +
                ", tableNamePrefix='" + tableNamePrefix + '\'' +
                ", tableNameSuffix='" + tableNameSuffix + '\'' +
                '}';
    }
}