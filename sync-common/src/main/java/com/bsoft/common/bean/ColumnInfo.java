package com.bsoft.common.bean;

/**
 * Doris表字段信息
 */
public class ColumnInfo {

    /** 字段名 */
    protected String columnName;

    /** 完整类型字符串(如: VARCHAR(100), DECIMAL(10,2)) */
    protected String columnType;


    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }
}
