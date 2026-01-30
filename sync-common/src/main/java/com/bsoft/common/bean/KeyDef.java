package com.bsoft.common.bean;

import com.bsoft.common.enums.KeyType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClassName: KeyDef
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 14:26
 */

//表示oracle中的一个约束或索引
public  class KeyDef   implements Serializable {
    private  String name;           // 约束名/索引名
    private  KeyType type;          // 类型
    private  List<String> columns;  // 列顺序与数据字典一致

    public KeyDef() {
    }

    public KeyDef(String name, KeyType type, List<String> columns) {
        this.name = name;
        this.type = type;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    }

    @Override public String toString() {
        return type + " " + name + " " + columns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public KeyType getType() {
        return type;
    }

    public void setType(KeyType type) {
        this.type = type;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
