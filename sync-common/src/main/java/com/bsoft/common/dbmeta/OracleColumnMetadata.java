package com.bsoft.common.dbmeta;



import java.io.Serializable;

/** 列元数据对象 */
public class OracleColumnMetadata implements Serializable {
    private String owner;
    private String tableName;
    private String columnName;
    private String dataType;

    private Integer columnId;        // COLUMN_ID
    private Integer dataLength;      // DATA_LENGTH
    private Integer dataPrecision;   // DATA_PRECISION
    private Integer dataScale;       // DATA_SCALE
    private String nullable;         // NULLABLE: Y/N
    private Integer charLength;      // CHAR_LENGTH

    public String getOwner() { return owner; }
    public void setOwner(String owner) { this.owner = owner; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getDataType() { return dataType; }
    public void setDataType(String dataType) { this.dataType = dataType; }

    public Integer getColumnId() { return columnId; }
    public void setColumnId(Integer columnId) { this.columnId = columnId; }

    public Integer getDataLength() { return dataLength; }
    public void setDataLength(Integer dataLength) { this.dataLength = dataLength; }

    public Integer getDataPrecision() { return dataPrecision; }
    public void setDataPrecision(Integer dataPrecision) { this.dataPrecision = dataPrecision; }

    public Integer getDataScale() { return dataScale; }
    public void setDataScale(Integer dataScale) { this.dataScale = dataScale; }

    public String getNullable() { return nullable; }
    public void setNullable(String nullable) { this.nullable = nullable; }

    public Integer getCharLength() { return charLength; }
    public void setCharLength(Integer charLength) { this.charLength = charLength; }
}