package com.bsoft.common.enums;

/**
 * ClassName: KeyType
 * Package:   com.bsoft.enums
 * Description: oracle中的主键，唯一约束，唯一索引，普通索引
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 14:24
 */
public enum KeyType {
    PRIMARY_KEY,       // 约束类型 P
    UNIQUE_CONSTRAINT, // 约束类型 U
    UNIQUE_INDEX,      // （可选）非约束的唯一索引,唯一索引不一定有对应的唯一约束
    NORMAL_INDEX       // 普通索引（非唯一索引）
}
