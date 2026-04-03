package com.bsoft.pipeline.k2k;

import com.bsoft.common.dbmeta.OracleColumnMetadata;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.table.api.Schema;
/**
 * ClassName: RuntimeTableSchema
 * Package:   com.bsoft.pipeline.k2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/5 - 16:12
 */

public class RuntimeTableSchema implements Serializable {
    private final List<OracleColumnMetadata> columns;
    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private final RowTypeInfo rowTypeInfo;
    private final Schema schema;

    RuntimeTableSchema(List<OracleColumnMetadata> columns,
                       String[] fieldNames,
                       TypeInformation<?>[] fieldTypes,
                       RowTypeInfo rowTypeInfo,
                       Schema schema) {
        this.columns = columns;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.rowTypeInfo = rowTypeInfo;
        this.schema = schema;
    }

    public List<OracleColumnMetadata> getColumns() {
        return columns;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    public RowTypeInfo getRowTypeInfo() {
        return rowTypeInfo;
    }

    public Schema getSchema() {
        return schema;
    }
}

