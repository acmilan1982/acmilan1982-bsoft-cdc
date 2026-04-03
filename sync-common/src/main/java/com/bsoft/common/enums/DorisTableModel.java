package com.bsoft.common.enums;

/**
 * ClassName: DorisTableModel
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/18 - 9:49
 */

public enum DorisTableModel {
    // 唯一模型
    UNIQUE("UNIQUE"),
    // 明细模型
    DUPLICATE("DUPLICATE");

    private final String code;

    DorisTableModel(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static DorisTableModel fromCode(String code) {
        if (code == null) {
            return UNIQUE;
        }
        for (DorisTableModel v : values()) {
            if (v.code.equalsIgnoreCase(code)) {
                return v;
            }
        }
        return UNIQUE;
    }
}
