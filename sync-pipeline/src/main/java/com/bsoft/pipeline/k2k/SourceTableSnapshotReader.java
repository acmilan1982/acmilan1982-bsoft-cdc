package com.bsoft.pipeline.k2k;


import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * 调试优先版：
 * 直接把源表快照全量读入内存，再交给 tableEnv 注册 snapshot view。
 */
public class SourceTableSnapshotReader {

    private static final int DEFAULT_FETCH_SIZE = 1000;

    public List<Row> readSnapshotRows(BusinessDataSourceConfig sourceConfig,
                                      ServerTopicInfo topicInfo,
                                      RuntimeTableSchema tableSchema) throws Exception {
        if (sourceConfig == null) {
            throw new IllegalArgumentException("sourceConfig is null");
        }
        if (topicInfo == null) {
            throw new IllegalArgumentException("topicInfo is null");
        }
        if (tableSchema == null) {
            throw new IllegalArgumentException("tableSchema is null");
        }

        DataSource dataSource = sourceConfig.getDataSource();
        String sql = buildSelectSql(topicInfo, tableSchema.getColumns());

        List<Row> rows = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     sql,
                     ResultSet.TYPE_FORWARD_ONLY,
                     ResultSet.CONCUR_READ_ONLY)) {

            conn.setReadOnly(true);
            ps.setFetchSize(DEFAULT_FETCH_SIZE);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Row row = new Row(RowKind.INSERT, tableSchema.getColumns().size());

                    for (int i = 0; i < tableSchema.getColumns().size(); i++) {
                        OracleColumnMetadata column = tableSchema.getColumns().get(i);
                        Object jdbcValue = rs.getObject(i + 1);
                        row.setField(i, normalizeJdbcValue(jdbcValue, column));
                    }

                    rows.add(row);
                }
            }
        }

        return rows;
    }

    private String buildSelectSql(ServerTopicInfo topicInfo, List<OracleColumnMetadata> columns) {
        String owner = requireNonBlank(topicInfo.getSchemaName(), "schemaName");
        String table = requireNonBlank(topicInfo.getTableName(), "tableName");

        String selectColumns = columns.stream()
                .map(c -> quoteIdentifier(c.getColumnName()))
                .collect(Collectors.joining(", "));

        return "SELECT " + selectColumns
                + " FROM " + quoteIdentifier(owner)
                + "." + quoteIdentifier(table);
    }

    private Object normalizeJdbcValue(Object jdbcValue, OracleColumnMetadata column) {
        if (jdbcValue == null) {
            return null;
        }

        String type = column.getDataType() == null
                ? ""
                : column.getDataType().trim().toUpperCase(Locale.ROOT);

        switch (type) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                return normalizeNumber(jdbcValue, column);

            case "FLOAT":
            case "BINARY_FLOAT":
                if (jdbcValue instanceof Number) {
                    return ((Number) jdbcValue).floatValue();
                }
                return jdbcValue;

            case "BINARY_DOUBLE":
                if (jdbcValue instanceof Number) {
                    return ((Number) jdbcValue).doubleValue();
                }
                return jdbcValue;

            case "DATE":
                if (jdbcValue instanceof java.sql.Date) {
                    return new Timestamp(((java.sql.Date) jdbcValue).getTime());
                }
                if (jdbcValue instanceof Timestamp) {
                    return jdbcValue;
                }
                return jdbcValue;

            case "TIMESTAMP":
            case "TIMESTAMP WITH TIME ZONE":
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                if (jdbcValue instanceof Timestamp) {
                    return jdbcValue;
                }
                if (jdbcValue instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) jdbcValue).getTime());
                }
                return jdbcValue;

            default:
                return jdbcValue;
        }
    }

    private Object normalizeNumber(Object jdbcValue, OracleColumnMetadata column) {
        if (!(jdbcValue instanceof Number)) {
            return jdbcValue;
        }

        Integer precision = column.getDataPrecision();
        Integer scale = column.getDataScale();
        Number n = (Number) jdbcValue;

        if (scale != null && scale > 0) {
            return (jdbcValue instanceof BigDecimal)
                    ? jdbcValue
                    : new BigDecimal(String.valueOf(jdbcValue));
        }

        if (precision == null) {
            return (jdbcValue instanceof BigDecimal)
                    ? jdbcValue
                    : new BigDecimal(String.valueOf(jdbcValue));
        }

        if (precision <= 9) {
            return n.intValue();
        }

        if (precision <= 18) {
            return n.longValue();
        }

        return (jdbcValue instanceof BigDecimal)
                ? jdbcValue
                : new BigDecimal(String.valueOf(jdbcValue));
    }

    private String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "") + "\"";
    }

    private String requireNonBlank(String value, String fieldName) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " is blank");
        }
        return value.trim();
    }
}