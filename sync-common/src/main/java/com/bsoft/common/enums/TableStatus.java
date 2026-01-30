package com.bsoft.common.enums;

/**
 * ClassName: DatabaseType
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 20:33
 */

public enum TableStatus {


    // 普通模式
    NORMAL("NORMAL"),
    // 快照模式
    SNAPSHOT("SNAPSHOT"),
    // 接收模式,服务端专属TableStatus,服务端有且仅有这一个状态：RECEIVE
    // 客户端不会有这个状态，
    RECEIVE("RECEIVE");



    private final String code;

    TableStatus(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static TableStatus fromCode(String code) {
        for (TableStatus type : values()) {
            if (type.code.equalsIgnoreCase(code)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported database type: " + code);
    }
}
