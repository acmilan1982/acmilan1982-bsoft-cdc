package com.bsoft.pipeline.k2k;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
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

    private static final Logger logger = LogManager.getLogger(JdbcSqlSnapshotSourceFunction.class);

    private static final int DEFAULT_FETCH_SIZE = 1000;

    private final BusinessDataSourceConfig sourceConfig;
    private final String schema;
    private final String sql;
    private final List<OracleColumnMetadata> columns;
    private final RowTypeInfo rowTypeInfo;
    private int count = 0;

    private volatile boolean running = true;

    // 快照读取基线 SCN
    private final Long snapshotScn;

    public JdbcSqlSnapshotSourceFunction(BusinessDataSourceConfig sourceConfig,
                                         String schema,
                                         String sql,
                                         Long snapshotScn,
                                         List<OracleColumnMetadata> columns,
                                         RowTypeInfo rowTypeInfo) {
        // 保存源库配置
        this.sourceConfig = sourceConfig;

        // 保存 schema
        this.schema = schema;

        // 保存原始 SQL
        this.sql = sql;

        // 保存 snapshot SCN
        this.snapshotScn = snapshotScn;

        // 保存字段元数据副本
        this.columns = columns == null ? null : new ArrayList<>(columns);

        // 保存 Flink RowTypeInfo
        this.rowTypeInfo = rowTypeInfo;
    }

    public RowTypeInfo getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        // 校验源库配置
        if (sourceConfig == null) {
            throw new IllegalArgumentException("sourceConfig is null");
        }

        // 校验字段列表
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns is empty");
        }

        // 校验 RowTypeInfo
        if (rowTypeInfo == null) {
            throw new IllegalArgumentException("rowTypeInfo is null");
        }

        // 校验 SQL
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("sql is blank");
        }

        // 校验 snapshotScn
        if (snapshotScn == null || snapshotScn <= 0) {
            throw new IllegalArgumentException("snapshotScn is null or invalid");
        }

        // 获取数据源
        DataSource dataSource = sourceConfig.getDataSource();

        // 获取 JDBC 连接
        try (Connection conn = dataSource.getConnection()) {
            // 设置只读连接
            conn.setReadOnly(true);

            // 设置当前 schema
            if (schema != null && !schema.trim().isEmpty()) {
                executeSessionSql(conn, "ALTER SESSION SET CURRENT_SCHEMA = " + quoteIdentifier(schema));
            }

            // 使用 DBMS_FLASHBACK 将当前 session 固定到 snapshotScn
            enableFlashbackScn(conn, snapshotScn);

            try {
                // 执行原始 snapshot SQL
                executeSnapshotQuery(conn, ctx);
            } finally {
                // 连接归还连接池前关闭 flashback，避免污染后续复用
                disableFlashbackQuietly(conn);
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

    private void executeSnapshotQuery(Connection conn, SourceContext<Row> ctx) throws Exception {
        // 创建只读前向游标 PreparedStatement
        try (PreparedStatement ps = conn.prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            // 设置 fetch size，避免一次性拉取过多数据
            ps.setFetchSize(DEFAULT_FETCH_SIZE);

            // 执行原始 SQL
            try (ResultSet rs = ps.executeQuery()) {
                // 持续读取结果集
                while (running && rs.next()) {
                    logger.info("sql执行，已输出 {} ", count++);

                    // 构造 INSERT Row
                    Row row = new Row(RowKind.INSERT, columns.size());

                    // 按列读取 JDBC 值并转换
                    for (int i = 0; i < columns.size(); i++) {
                        OracleColumnMetadata column = columns.get(i);
                        Object jdbcValue = rs.getObject(i + 1);
                        row.setField(i, normalizeJdbcValue(jdbcValue, column));
                    }

                    // 使用 Flink checkpoint lock 发出数据
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(row);
                    }
                }
            }
        }
    }

    private void executeSessionSql(Connection conn, String sql) throws Exception {
        // 执行 session 级 SQL
        try (Statement statement = conn.createStatement()) {
            statement.execute(sql);
        }
    }

    private void enableFlashbackScn(Connection conn, long snapshotScn) throws Exception {
        // 使用 SYS.DBMS_FLASHBACK，避免 public synonym 不存在导致 DBMS_FLASHBACK 不可见
        try (CallableStatement statement = conn.prepareCall(
                "BEGIN SYS.DBMS_FLASHBACK.ENABLE_AT_SYSTEM_CHANGE_NUMBER(?); END;")) {
            statement.setLong(1, snapshotScn);
            statement.execute();
        }

        logger.info("oracle flashback enabled, snapshotScn={}", snapshotScn);
    }

    private void disableFlashbackQuietly(Connection conn) {
        try (CallableStatement statement = conn.prepareCall(
                "BEGIN SYS.DBMS_FLASHBACK.DISABLE; END;")) {
            statement.execute();
            logger.info("oracle flashback disabled");
        } catch (Exception e) {
            logger.warn("failed to disable oracle flashback session", e);
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