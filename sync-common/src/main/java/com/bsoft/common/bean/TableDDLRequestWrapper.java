package com.bsoft.common.bean;

import com.bsoft.common.dto.TableMappingConfigDTO;
import java.util.List;

/**
 * 用于封装 TableDDLRequest 和 TableMappingConfigDTO 列表的包装类
 */
public class TableDDLRequestWrapper {
    private TableDDLRequest tableDDLRequest;
    private List<TableMappingConfigDTO> tableMappingConfigDTOList;

    public TableDDLRequestWrapper() {
    }

    public TableDDLRequestWrapper(TableDDLRequest tableDDLRequest, List<TableMappingConfigDTO> tableMappingConfigDTOList) {
        this.tableDDLRequest = tableDDLRequest;
        this.tableMappingConfigDTOList = tableMappingConfigDTOList;
    }

    public TableDDLRequest getTableDDLRequest() {
        return tableDDLRequest;
    }

    public void setTableDDLRequest(TableDDLRequest tableDDLRequest) {
        this.tableDDLRequest = tableDDLRequest;
    }

    public List<TableMappingConfigDTO> getTableMappingConfigDTOList() {
        return tableMappingConfigDTOList;
    }

    public void setTableMappingConfigDTOList(List<TableMappingConfigDTO> tableMappingConfigDTOList) {
        this.tableMappingConfigDTOList = tableMappingConfigDTOList;
    }
}

