package com.bsoft.common.bean;

/**
 * ClassName: RawMessageStorageStrategy
 * Package:   com.bsoft.common.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/17 - 10:27
 */
public enum RawMessageStorageStrategy {
    NONE,       // 不插入 RAW_MESSAGE 字段
    PLAIN,      // 插入原始文本，不压缩
    COMPRESS    // 使用 GZIP 压缩后插入
}
