package com.bsoft.common.bean;

import com.bsoft.common.enums.KeyType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.io.Serializable;
import java.util.*;

/**
 * ClassName: TableKeys
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 14:28
 */
public class TableKeys implements Serializable {
    // schema
    public String owner;
    // 表名
    public String table;
    // 当前表的所有约束/索引
    public List<KeyDef> keys;

    @JsonIgnore  // 忽略序列化和反序列化
    private Map<KeyType, List<KeyDef>> grouped; // 分组视图（不可变）

    public TableKeys() {
    }

    public TableKeys(String owner, String table, List<KeyDef> keys) {
        this.owner = owner;
        this.table = table;
        this.keys = Collections.unmodifiableList(new ArrayList<>(keys));
        initializeGrouped();
    }

    // 提取初始化逻辑为单独方法
    private void initializeGrouped() {
        Map<KeyType, List<KeyDef>> tmp = new EnumMap<>(KeyType.class);

        // 为每个 KeyType 预先创建空列表
        for (KeyType keyType : KeyType.values()) {
            tmp.put(keyType, new ArrayList<>());
        }

        // 如果有 keys，则分组添加
        if (this.keys != null) {
            for (KeyDef kd : this.keys) {
                tmp.get(kd.getType()).add(kd);
            }

            // 同类型内按名字排序
            for (List<KeyDef> list : tmp.values()) {
                list.sort(Comparator.comparing(k -> k.getName()));
            }
        }

        this.grouped = Collections.unmodifiableMap(tmp);
    }

    // 添加 setter 方法，用于反序列化后重新初始化 grouped
    @JsonSetter("keys")
    public void setKeys(List<KeyDef> keys) {
        this.keys = keys;
        initializeGrouped();
    }

    // 确保 grouped 被正确初始化
    public Map<KeyType, List<KeyDef>> getGrouped() {
        if (grouped == null) {
            initializeGrouped();
        }
        return grouped;
    }

    public List<KeyDef> getPrimaryKeys() {
        return getGrouped().getOrDefault(KeyType.PRIMARY_KEY, Collections.emptyList());
    }

    public List<KeyDef> getUniqueConstraints() {
        return getGrouped().getOrDefault(KeyType.UNIQUE_CONSTRAINT, Collections.emptyList());
    }

    public List<KeyDef> getUniqueIndexes() {
        return getGrouped().getOrDefault(KeyType.UNIQUE_INDEX, Collections.emptyList());
    }

    // 新增：获取普通索引
    public List<KeyDef> getNormalIndexes() {
        return getGrouped().getOrDefault(KeyType.NORMAL_INDEX, Collections.emptyList());
    }

    @Override
    public String toString() {
        return owner + "." + table + " -> " + keys;
    }

    public List<KeyDef> getKeys() {
        return keys;
    }

    public KeyDef getFirstKeys() {
        if (keys == null || keys.isEmpty()) {
            return null;
        }
        return keys.get(0);
    }

    /**
     * 获取表的主键或唯一键
     * 按优先级依次检查：PRIMARY_KEY -> UNIQUE_CONSTRAINT -> UNIQUE_INDEX
     *
     * @return 找到的第一个键定义，如果都不存在则返回null
     */
    public KeyDef getPrimaryOrUniqueKey() {
        Map<KeyType, List<KeyDef>> groupedMap = getGrouped();

        // 按优先级检查主键
        List<KeyDef> primaryKeys = groupedMap.get(KeyType.PRIMARY_KEY);
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            return primaryKeys.get(0);
        }

        // 检查唯一约束
        List<KeyDef> uniqueConstraints = groupedMap.get(KeyType.UNIQUE_CONSTRAINT);
        if (uniqueConstraints != null && !uniqueConstraints.isEmpty()) {
            return uniqueConstraints.get(0);
        }

        // 检查唯一索引
        List<KeyDef> uniqueIndexes = groupedMap.get(KeyType.UNIQUE_INDEX);
        if (uniqueIndexes != null && !uniqueIndexes.isEmpty()) {
            return uniqueIndexes.get(0);
        }

        // 都没有找到，返回null
        return null;
    }



    // 添加 setter 方法以支持反序列化
    public void setOwner(String owner) {
        this.owner = owner;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getOwner() {
        return owner;
    }

    public String getTable() {
        return table;
    }
}
