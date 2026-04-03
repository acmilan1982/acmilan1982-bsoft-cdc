package com.bsoft.common.enums;

/**
 * ClassName: DorisColumnLengthModel
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/18 - 9:52
 */
public enum DorisColumnLengthModel {
    SAME_AS_SOURCE("SAME_AS_SOURCE"),
    MAX_LENGTH("MAX_LENGTH");

    private final String code;

    DorisColumnLengthModel(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static DorisColumnLengthModel fromCode(String code) {
        if (code == null) {
            return SAME_AS_SOURCE;
        }
        for (DorisColumnLengthModel v : values()) {
            if (v.code.equalsIgnoreCase(code)) {
                return v;
            }
        }
        return SAME_AS_SOURCE;
    }
}
