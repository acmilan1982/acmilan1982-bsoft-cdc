package com.bsoft.pipeline.k2k;

import com.bsoft.common.dbmeta.OracleColumnMetadata;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;

/**
 * ClassName: RuntimeTableSchemaBuilder
 * Package:   com.bsoft.pipeline.k2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/3 - 14:54
 */
public final class RuntimeTableSchemaBuilder {

    private RuntimeTableSchemaBuilder() {
    }

    public static RuntimeTableSchema build(List<OracleColumnMetadata> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns is empty");
        }

        String[] fieldNames = new String[columns.size()];
        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[columns.size()];
        Schema.Builder schemaBuilder = Schema.newBuilder();

        for (int i = 0; i < columns.size(); i++) {
            OracleColumnMetadata c = columns.get(i);

            String fieldName = c.getColumnName().toLowerCase(Locale.ROOT);
            fieldNames[i] = fieldName;
            fieldTypes[i] = oracleToTypeInfo(c);
            schemaBuilder.column(fieldName, oracleToDataType(c));
        }

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        Schema schema = schemaBuilder.build();

        return new RuntimeTableSchema(columns, fieldNames, fieldTypes, rowTypeInfo, schema);
    }

    private static TypeInformation<?> oracleToTypeInfo(OracleColumnMetadata c) {
        String t = normalizeType(c.getDataType());

        switch (t) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                Integer p = c.getDataPrecision();
                Integer s = c.getDataScale();
                if (s != null && s > 0) {
                    return Types.BIG_DEC;
                }
                if (p == null) {
                    return Types.BIG_DEC;
                }
                if (p <= 9) {
                    return Types.INT;
                }
                if (p <= 18) {
                    return Types.LONG;
                }
                return Types.BIG_DEC;

            case "FLOAT":
            case "BINARY_FLOAT":
                return Types.FLOAT;

            case "BINARY_DOUBLE":
                return Types.DOUBLE;

            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return Types.SQL_TIMESTAMP;

            case "CHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "CLOB":
            case "NCLOB":
            default:
                return Types.STRING;
        }
    }

    private static DataType oracleToDataType(OracleColumnMetadata c) {
        String t = normalizeType(c.getDataType());

        switch (t) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                Integer p = c.getDataPrecision();
                Integer s = c.getDataScale();
                int precision = (p == null || p <= 0) ? 38 : Math.min(p, 38);
                int scale = (s == null || s < 0) ? 0 : Math.min(s, 18);
                return DataTypes.DECIMAL(precision, scale);

            case "FLOAT":
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();

            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();

            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return DataTypes.TIMESTAMP(3).bridgedTo(Timestamp.class);

            case "CHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "CLOB":
            case "NCLOB":
            default:
                return DataTypes.STRING();
        }
    }

    private static String normalizeType(String t) {
        return t == null ? "" : t.trim().toUpperCase(Locale.ROOT);
    }
}