package com.bsoft.pipeline.cache;

import java.util.Objects;

/**
 * ClassName: SchemaCacheKey
 * Package:   com.bsoft.pipeline.cache
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 9:59
 */
public class SchemaCacheKey {

    private String dataSourceId;
    private String schema;
    private String tableName;


    public SchemaCacheKey() {
    }

    public SchemaCacheKey(String dataSourceId, String schema, String tableName) {
        this.dataSourceId = dataSourceId;
        this.schema = schema;
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SchemaCacheKey that = (SchemaCacheKey) o;
        return Objects.equals(dataSourceId, that.dataSourceId) && Objects.equals(schema, that.schema) && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSourceId, schema, tableName);
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
