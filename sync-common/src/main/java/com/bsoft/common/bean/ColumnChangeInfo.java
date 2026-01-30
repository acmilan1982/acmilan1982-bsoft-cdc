package com.bsoft.common.bean;

/**
 * ClassName: ColumnChangeInfo
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/29 - 10:53
 */
/**
 * 字段变更信息POJO
 */
public  class ColumnChangeInfo {
    private String tableName;
    private String columnName;
    private String columnType;
    private String operationType; // ADD, MODIFY, RENAME, DROP
    private String oldColumnName; // 用于RENAME操作
    private int length; // 新增:字段长度

    public ColumnChangeInfo(String tableName,String columnName, String columnType, String operationType) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.operationType = operationType;
    }

    public ColumnChangeInfo(String tableName,String columnName, String columnType, String operationType, String oldColumnName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.operationType = operationType;
        this.oldColumnName = oldColumnName;
    }
    // 新增:带长度的构造函数
    public ColumnChangeInfo(String tableName, String columnName, String columnType, int length) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.length = length;
    }


    // Getters and Setters
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getColumnType() { return columnType; }
    public void setColumnType(String columnType) { this.columnType = columnType; }

    public String getOperationType() { return operationType; }
    public void setOperationType(String operationType) { this.operationType = operationType; }

    public String getOldColumnName() { return oldColumnName; }
    public void setOldColumnName(String oldColumnName) { this.oldColumnName = oldColumnName; }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return "ColumnChangeInfo{" +
                "tableName='" + tableName + '\'' +
                ", columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", operationType='" + operationType + '\'' +
                ", oldColumnName='" + oldColumnName + '\'' +
                ", length=" + length +
                '}';
    }
}