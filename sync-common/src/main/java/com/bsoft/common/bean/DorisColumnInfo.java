package com.bsoft.common.bean;

/**
 * Doris表字段信息
 */
public class DorisColumnInfo extends ColumnInfo{





    /** 基础类型(如: VARCHAR, DECIMAL, INT) */
    private String baseType;

    /** 字段长度(字符型) */
    private Integer length;

    /** 数值精度(DECIMAL类型) */
    private Integer precision;

    /** 数值小数位(DECIMAL类型) */
    private Integer scale;

    /** 是否可为空 */
    private boolean nullable;

    /** 键类型(PRI/UNI/MUL/空) */
    private String key;

    /** 默认值 */
    private String defaultValue;

    /** 额外信息(如: auto_increment) */
    private String extra;

    /** 字段注释 */
    private String comment;

    /** 字段顺序 */
    private int ordinalPosition;

    // ==================== 构造方法 ====================

    private DorisColumnInfo() {}

    private DorisColumnInfo(Builder builder) {
        this.columnName = builder.columnName;
        this.columnType = builder.columnType;
        this.baseType = builder.baseType;
        this.length = builder.length;
        this.precision = builder.precision;
        this.scale = builder.scale;
        this.nullable = builder.nullable;
        this.key = builder.key;
        this.defaultValue = builder.defaultValue;
        this.extra = builder.extra;
        this.comment = builder.comment;
        this.ordinalPosition = builder.ordinalPosition;
    }

    // ==================== Builder ====================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String columnName;
        private String columnType;
        private String baseType;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private boolean nullable;
        private String key;
        private String defaultValue;
        private String extra;
        private String comment;
        private int ordinalPosition;

        private Builder() {}

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder columnType(String columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder baseType(String baseType) {
            this.baseType = baseType;
            return this;
        }

        public Builder length(Integer length) {
            this.length = length;
            return this;
        }

        public Builder precision(Integer precision) {
            this.precision = precision;
            return this;
        }

        public Builder scale(Integer scale) {
            this.scale = scale;
            return this;
        }

        public Builder nullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder extra(String extra) {
            this.extra = extra;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder ordinalPosition(int ordinalPosition) {
            this.ordinalPosition = ordinalPosition;
            return this;
        }

        public DorisColumnInfo build() {
            return new DorisColumnInfo(this);
        }
    }

    // ==================== Getter/Setter ====================


    public String getBaseType() {
        return baseType;
    }

    public void setBaseType(String baseType) {
        this.baseType = baseType;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    // ==================== 业务方法 ====================

    /**
     * 判断是否为主键
     */
    public boolean isPrimaryKey() {
        return "PRI".equalsIgnoreCase(key);
    }

    /**
     * 判断是否为字符型字段
     */
    public boolean isCharacterType() {
        if (baseType == null) return false;
        return baseType.startsWith("VARCHAR") ||
                baseType.startsWith("CHAR") ||
                baseType.equals("TEXT") ||
                baseType.equals("STRING");
    }

    /**
     * 判断是否为数值型字段
     */
    public boolean isNumericType() {
        if (baseType == null) return false;
        return baseType.equals("DECIMAL") ||
                baseType.equals("INT") ||
                baseType.equals("BIGINT") ||
                baseType.equals("DOUBLE") ||
                baseType.equals("FLOAT");
    }

    @Override
    public String toString() {
        return "DorisColumnInfo{" +
                "columnName='" + columnName + '\'' +
                ", columnType='" + columnType + '\'' +
                ", baseType='" + baseType + '\'' +
                ", nullable=" + nullable +
                ", key='" + key + '\'' +
                ", ordinalPosition=" + ordinalPosition +
                '}';
    }
}
