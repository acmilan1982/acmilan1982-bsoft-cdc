package com.bsoft.common.enums;

/**
 * ClassName: TableNamingStrategy
 * Package:   com.bsoft.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:47
 */
public enum TableRowDeleteStrategy {
    DELETE("DELETE"),
    DELETE_FLAG("DELETE_FLAG");



    private final String code;

    TableRowDeleteStrategy(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static TableRowDeleteStrategy fromCode(String code) {
        for (TableRowDeleteStrategy type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported database type: " + code);
    }
}
