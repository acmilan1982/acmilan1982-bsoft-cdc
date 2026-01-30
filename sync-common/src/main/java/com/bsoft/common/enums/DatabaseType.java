package com.bsoft.common.enums;

/**
 * ClassName: DatabaseType
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 20:33
 */

public enum DatabaseType {
    ORACLE("ORACLE"),
    MYSQL("MYSQL"),
    DORIS("DORIS");

    private final String code;

    DatabaseType(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static DatabaseType fromCode(String code) {
        for (DatabaseType type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported database type: " + code);
    }
}
