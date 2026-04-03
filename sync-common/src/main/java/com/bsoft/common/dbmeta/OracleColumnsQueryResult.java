package com.bsoft.common.dbmeta;

/**
 * ClassName: OracleColumnsQueryResult
 * Package:   com.bsoft.common.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/4 - 11:13
 */

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 返回对象 */
public  class OracleColumnsQueryResult {
    private String owner;
    private List<OracleColumnMetadata> columns = new ArrayList<>();
    private Map<String, List<OracleColumnMetadata>> tableColumns = new LinkedHashMap<>();

    public void add(OracleColumnMetadata metadata) {
        this.columns.add(metadata);
        this.tableColumns.computeIfAbsent(metadata.getTableName(), k -> new ArrayList<>()).add(metadata);
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public List<OracleColumnMetadata> getColumns() {
        return columns;
    }

    public void setColumns(List<OracleColumnMetadata> columns) {
        this.columns = columns;
    }

    public Map<String, List<OracleColumnMetadata>> getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(Map<String, List<OracleColumnMetadata>> tableColumns) {
        this.tableColumns = tableColumns;
    }
}