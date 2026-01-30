package com.bsoft.common.bean;

import com.bsoft.common.enums.KeyType;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: KeyDefBuilder
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 14:40
 */
/* ===============  Builder：聚合同一约束/索引的多列  =============== */

public  class KeyDefBuilder {
    // 约束名/索引名
    private  String name;
    // 约束/索引 类型
    private  KeyType type;
    private  String owner;
    private  String table;
    // 约束/索引 包含的列，按顺序
    private  List<String> cols = new ArrayList<>();

    public KeyDefBuilder(String name, KeyType type, String owner, String table) {
        this.name = name;
        this.type = type;
        this.owner= owner;
        this.table= table;
    }

    public KeyDefBuilder addColumn(String col) {
        cols.add(col);
        return this;
    }

    public  KeyDef build() {
        return new KeyDef(name, type, cols);
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

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getCols() {
        return cols;
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }
}