package com.bsoft.common.enums;

/**
 * ClassName: DorisColumnModel
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/18 - 9:52
 */
public enum DorisColumnModel {
    SAME_AS_SOURCE("SAME_AS_SOURCE"),
    ALL_VARCHAR("ALL_VARCHAR");

    private final String code;

    DorisColumnModel(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static DorisColumnModel fromCode(String code) {
        if (code == null) {
            return SAME_AS_SOURCE;
        }
        for (DorisColumnModel v : values()) {
            if (v.code.equalsIgnoreCase(code)) {
                return v;
            }
        }
        return SAME_AS_SOURCE;
    }
}