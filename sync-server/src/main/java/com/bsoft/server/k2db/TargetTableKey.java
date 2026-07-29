package com.bsoft.server.k2db;

import java.io.Serializable;
import java.util.Objects;

// 目标表分组键，用于把多个 topic 合并到同一个目标表写入链路
public class TargetTableKey implements Serializable {

    // 目标数据源 ID
    private final String targetDataSourceId;

    // 目标表名
    private final String targetTableName;

    // 创建目标表分组键
    public TargetTableKey(String targetDataSourceId, String targetTableName) {
        // 保存目标数据源 ID
        this.targetDataSourceId = normalize(targetDataSourceId);

        // 保存目标表名
        this.targetTableName = normalize(targetTableName);
    }

    // 标准化 key 字段，避免首尾空格导致分组错误
    private String normalize(String value) {
        // 空值统一为空字符串，避免 equals/hashCode 空指针
        return value == null ? "" : value.trim();
    }

    // 获取目标数据源 ID
    public String getTargetDataSourceId() {
        // 返回目标数据源 ID
        return targetDataSourceId;
    }

    // 获取目标表名
    public String getTargetTableName() {
        // 返回目标表名
        return targetTableName;
    }

    // 判断两个分组键是否相同
    @Override
    public boolean equals(Object o) {
        // 同一对象直接相等
        if (this == o) return true;

        // 类型不同直接不相等
        if (!(o instanceof TargetTableKey)) return false;

        // 转换为当前类型
        TargetTableKey that = (TargetTableKey) o;

        // 按目标数据源和目标表判断相等
        return Objects.equals(targetDataSourceId, that.targetDataSourceId)
                && Objects.equals(targetTableName, that.targetTableName);
    }

    // 生成分组键哈希值
    @Override
    public int hashCode() {
        // 按目标数据源和目标表生成哈希
        return Objects.hash(targetDataSourceId, targetTableName);
    }

    // 输出日志友好的分组键
    @Override
    public String toString() {
        // 返回可读字符串
        return targetDataSourceId + "." + targetTableName;
    }
}