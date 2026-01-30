package com.bsoft.common.enums;

/**
 * ClassName: TableNamingStrategy
 * Package:   com.bsoft.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:47
 */
public enum TableNamingStrategy {
    // 使用源表名，同名表合并到一个目标表
    TABLE_MERGE("TABLE_MERGE"),
    // 使用源表名，同名冲突时加前缀区分
    TABLE_PREFIX("TABLE_PREFIX"),
    // 探针ID_用户_表名，全局唯一表名，绝对不会冲突
    CLIENTID_SCHEMA_TABLE("CLIENTID_SCHEMA_TABLE");


    private final String code;

    TableNamingStrategy(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static TableNamingStrategy fromCode(String code) {
        for (TableNamingStrategy type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("不支持当前的合并策略: " + code);
    }
}
