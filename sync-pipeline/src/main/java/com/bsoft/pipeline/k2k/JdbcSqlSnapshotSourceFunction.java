package com.bsoft.pipeline.k2k;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
/**
 * ClassName: JdbcSqlSnapshotSourceFunction
 * Package:   com.bsoft.pipeline.k2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/3 - 14:54
 */

public class JdbcSqlSnapshotSourceFunction extends RichSourceFunction<Row> {

    private static final int DEFAULT_FETCH_SIZE = 1000;

    private final BusinessDataSourceConfig sourceConfig;
    private final String schema;
    private final String sql;
    private final List<OracleColumnMetadata> columns;
    private final RowTypeInfo rowTypeInfo;

    private volatile boolean running = true;

    public JdbcSqlSnapshotSourceFunction(BusinessDataSourceConfig sourceConfig,
                                         String schema,
                                         String sql,
                                         List<OracleColumnMetadata> columns,
                                         RowTypeInfo rowTypeInfo) {
        this.sourceConfig = sourceConfig;
        this.schema = schema;
        this.sql = sql;
        this.columns = columns == null ? null : new ArrayList<>(columns);
        this.rowTypeInfo = rowTypeInfo;
    }

    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        if (sourceConfig == null) {
            throw new IllegalArgumentException("sourceConfig is null");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns is empty");
        }
        if (rowTypeInfo == null) {
            throw new IllegalArgumentException("rowTypeInfo is null");
        }
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("sql is blank");
        }

        DataSource dataSource = sourceConfig.getDataSource();

        try (Connection conn = dataSource.getConnection()) {
            conn.setReadOnly(true);

            if (schema != null && !schema.trim().isEmpty()) {
                try (PreparedStatement alterSchema = conn.prepareStatement(
                        "ALTER SESSION SET CURRENT_SCHEMA = " + quoteIdentifier(schema))) {
                    alterSchema.execute();
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY)) {

                ps.setFetchSize(DEFAULT_FETCH_SIZE);

                try (ResultSet rs = ps.executeQuery()) {
                    while (running && rs.next()) {
                        Row row = new Row(RowKind.INSERT, columns.size());

                        for (int i = 0; i < columns.size(); i++) {
                            OracleColumnMetadata column = columns.get(i);
                            Object jdbcValue = rs.getObject(i + 1);
                            row.setField(i, normalizeJdbcValue(jdbcValue, column));
                        }

                        synchronized (ctx.getCheckpointLock()) {
                            ctx.collect(row);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        this.running = false;
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
}