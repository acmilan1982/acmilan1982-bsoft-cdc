package com.bsoft.common.bean;

import java.io.Serializable;
import java.util.Objects;

/**
 * ClassName: Table
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/11 - 16:27
 */
public class Table implements Serializable {

    // 所属数据源id,一般是业务库
    private String dataSourceId;


    private String schemaName;
    private String tableName;

    private String fullyQualifiedTableName;

    public Table() {
    }

    // 如果是oracle，schema和tableName的入参应该大写
    public Table(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        generateFullyQualifiedTableName();
    }

    // 如果是oracle，schema和tableName的入参应该大写
    public Table(String dataSourceId, String schemaName, String tableName) {
        this.dataSourceId = dataSourceId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        generateFullyQualifiedTableName();
    }

    // 生成单表的的全限定表名
    public String generateFullyQualifiedTableName() {
        return this.fullyQualifiedTableName = schemaName.toUpperCase() + "." + tableName.toUpperCase();
    }


    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFullyQualifiedTableName() {

        if(this.fullyQualifiedTableName == null){
            generateFullyQualifiedTableName();
        }
        return fullyQualifiedTableName;
    }

    public void setFullyQualifiedTableName(String fullyQualifiedTableName) {

        this.fullyQualifiedTableName = fullyQualifiedTableName;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public void setDataSourceId(String dataSourceId) {
        this.dataSourceId = dataSourceId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Table table = (Table) object;
        return Objects.equals(schemaName, table.schemaName) && Objects.equals(tableName, table.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }
}
