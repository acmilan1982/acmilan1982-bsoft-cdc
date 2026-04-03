package com.bsoft.common.dbmeta;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.bean.TableKeys;
import com.bsoft.common.bean.TableMappingConfig;
import com.bsoft.common.util.OracleKeyInspector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ClassName: OracleTableMetadataReader
 * Package:   com.bsoft.client.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:30
 */
public class OracleTableMetadataReader {

    private static final Logger logger = LogManager.getLogger(OracleTableMetadataReader.class);
    // 业务库的数据源
    private DataSource dataSource;

    private BusinessDataSourceConfig businessDataSourceConfig;

    public OracleTableMetadataReader(BusinessDataSourceConfig businessDataSourceConfig) {
        this.businessDataSourceConfig = businessDataSourceConfig;
        this.dataSource = businessDataSourceConfig.getDataSource();
    }



    // 抓取并设置所有表的建表语句和键信息
    public void getAllTableMetadata()  {
        try {

            for (TableMappingConfig tableMappingConfig : this.businessDataSourceConfig.getTableMappingConfigList()) {
                getSingleTableMetadata(tableMappingConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void getSingleTableMetadata(TableMappingConfig tableMappingConfig)  {
        try {

                // 获取并设置建表语句
                tableMappingConfig.setDdl(getTableDDL(tableMappingConfig));
                // 获取并设置表的键信息
                tableMappingConfig.setTableKeys(getTableKeys(tableMappingConfig));
                logger.info("已抓取建表语句： {}.{}: \n{}",
                        tableMappingConfig.getSchemaName(),
                        tableMappingConfig.getTableName(),
                        tableMappingConfig.getDdl());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    // 获取每个表的建表语句,纯纯的建表语句，没有任何多余信息
    private String getTableDDL(TableMappingConfig tableMappingConfig){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // 1. 设置DBMS_METADATA转换参数，去掉不需要的部分
            String setupParams = "            BEGIN\n" +
                    "                -- 去掉存储参数（STORAGE子句）\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);\n" +
                    "                -- 去掉表空间信息\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);\n" +
                    "                -- 去掉段属性（PCTFREE, PCTUSED, INITRANS等）\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);\n" +
                    "                -- 去掉外键约束\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);\n" +
                    "                -- 去掉检查约束\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);\n" +
                    "                -- 去掉索引\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', FALSE);\n" +
                    "                -- 添加分号结尾\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);\n" +
                    "                -- 美化格式\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);\n" +
                    "            END;";

            conn.createStatement().execute(setupParams);

            // 2. 获取清理后的DDL
            String ddlQuery = "SELECT DBMS_METADATA.GET_DDL('TABLE', ?, ?) FROM DUAL";
            try (PreparedStatement stmt = conn.prepareStatement(ddlQuery)) {
                stmt.setString(1, tableMappingConfig.getTableName());
                stmt.setString(2, tableMappingConfig.getSchemaName());

                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    String ddl = rs.getString(1);

                    // 3. 进一步清理DDL（可选，作为双重保险）
                    return furtherCleanDdl(ddl);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Oracle表DDL失败: " + tableMappingConfig.getSchemaName() + "." + tableMappingConfig.getTableName(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


    /**
     * 进一步清理DDL，去掉可能残留的不需要部分
     */
    private String furtherCleanDdl(String originalDdl) {
        if (originalDdl == null || originalDdl.trim().isEmpty()) {
            return originalDdl;
        }

        String cleaned = originalDdl
                // 去掉可能残留的表空间声明
                .replaceAll("(?i)\\s+TABLESPACE\\s+\\w+", "")
                // 去掉存储参数
                .replaceAll("(?i)\\s+STORAGE\\s*\\([^)]+\\)", "")
                // 去掉物理属性
                .replaceAll("(?i)\\s+PCTFREE\\s+\\d+", "")
                .replaceAll("(?i)\\s+PCTUSED\\s+\\d+", "")
                .replaceAll("(?i)\\s+INITRANS\\s+\\d+", "")
                .replaceAll("(?i)\\s+MAXTRANS\\s+\\d+", "")
                // 去掉日志属性
                .replaceAll("(?i)\\s+(NO)?LOGGING", "")
                // 去掉压缩属性
                .replaceAll("(?i)\\s+(NO)?COMPRESS", "")
                // 去掉并行属性
                .replaceAll("(?i)\\s+(NO)?PARALLEL(\\s+\\d+)?", "")
                // 清理多余的空白和换行
                .replaceAll("\\s+", " ")
                .trim();

        // 确保以分号结尾
        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }

        return cleaned;
    }


    // 获取指定表的索引信息
    public TableKeys getTableKeys(TableMappingConfig tableMappingConfig) {
        try (Connection conn = dataSource.getConnection()) {
            return OracleKeyInspector.fetchKeys(
                    conn,
                    tableMappingConfig.getSchemaName(),
                    tableMappingConfig.getTableName(),
                    true,
                    true
            );

        } catch (Exception e) {
            throw new RuntimeException("获取Oracle表索引信息失败: "
                    + tableMappingConfig.getSchemaName() + "." + tableMappingConfig.getTableName(), e);
        }
    }



    // 获取指定表的列信息
    public OracleColumnsQueryResult queryColumnsByOwnerAndTables(OracleColumnsQueryRequest oracleColumnsQueryRequest) {
        String owner = oracleColumnsQueryRequest.getOwner();
        List<String> tableNames = oracleColumnsQueryRequest.getTableNames();
        if (owner == null || owner.trim().isEmpty()) {
            throw new IllegalArgumentException("owner 不能为空");
        }

        OracleColumnsQueryResult result = new OracleColumnsQueryResult();
        result.setOwner(owner.trim().toUpperCase());

        if (tableNames == null || tableNames.isEmpty()) {
            return result;
        }

        List<String> normalizedTables = tableNames.stream()
                .filter(t -> t != null && !t.trim().isEmpty())
                .map(t -> t.trim().toUpperCase())
                .distinct()
                .collect(java.util.stream.Collectors.toList());

        if (normalizedTables.isEmpty()) {
            return result;
        }

        final int batchSize = 1000;
        try (java.sql.Connection conn = dataSource.getConnection()) {
            for (int from = 0; from < normalizedTables.size(); from += batchSize) {
                int to = Math.min(from + batchSize, normalizedTables.size());
                List<String> batch = normalizedTables.subList(from, to);

                String inClause = String.join(",", java.util.Collections.nCopies(batch.size(), "?"));
                String sql =
                        "SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_ID, " +
                                "       DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_LENGTH " +
                                "FROM ALL_TAB_COLUMNS " +
                                "WHERE OWNER = ? AND TABLE_NAME IN (" + inClause + ") " +
                                "ORDER BY TABLE_NAME, COLUMN_ID";

                try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                    int idx = 1;
                    ps.setString(idx++, result.getOwner());
                    for (String tableName : batch) {
                        ps.setString(idx++, tableName);
                    }

                    try (java.sql.ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            OracleColumnMetadata meta = new OracleColumnMetadata();
                            meta.setOwner(rs.getString("OWNER"));
                            meta.setTableName(rs.getString("TABLE_NAME"));
                            meta.setColumnName(rs.getString("COLUMN_NAME"));
                            meta.setDataType(rs.getString("DATA_TYPE"));

                            meta.setColumnId(toInteger(rs.getObject("COLUMN_ID")));
                            meta.setDataLength(toInteger(rs.getObject("DATA_LENGTH")));
                            meta.setDataPrecision(toInteger(rs.getObject("DATA_PRECISION")));
                            meta.setDataScale(toInteger(rs.getObject("DATA_SCALE")));
                            meta.setCharLength(toInteger(rs.getObject("CHAR_LENGTH")));
                            meta.setNullable(rs.getString("NULLABLE"));

                            result.add(meta);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("查询 ALL_TAB_COLUMNS 失败, owner=" + owner + ", tables=" + tableNames, e);
        }

        return result;
    }
//
//    /**
//     * Execute user SQL in metadata-only mode and return result column metadata.
//     * The SQL must be a single SELECT/CTE statement.
//     */
//    public OracleColumnsQueryResult queryColumnsBySql(OracleSqlColumnsQueryRequest request) {
//        if (request == null) {
//            throw new IllegalArgumentException("request cannot be null");
//        }
//
//        // 待执行的sql
//        String sql = request.getSql();
//        validateQuerySql(sql);
//
//        // sql中的表，所属的schema
//        String targetSchema = normalizeSchema(request.getSchema()); // 可为空
//        OracleColumnsQueryResult result = new OracleColumnsQueryResult();
//
////        String owner = businessDataSourceConfig != null ? businessDataSourceConfig.getDataSourceUserName() : null;
////        if (owner != null && !owner.trim().isEmpty()) {
////            result.setOwner(owner.trim().toUpperCase());
////        }
//
//        try (Connection conn = dataSource.getConnection()) {
//            // 保存原始 schema
//            String originalSchema = getCurrentSchema(conn);
//
//            try {
//                if (targetSchema != null) {
//                    // Connection设置成当前请求的 schema
//                    setCurrentSchema(conn, targetSchema);
//                    result.setOwner(targetSchema); // 返回结果owner按目标schema展示
//                }
//
//                try (PreparedStatement ps = conn.prepareStatement(sql.trim())) {
//                    ResultSetMetaData metaData = ps.getMetaData();
//                    if (metaData == null) {
//                        ps.setMaxRows(1);
//                        try (ResultSet rs = ps.executeQuery()) {
//                            metaData = rs.getMetaData();
//                        }
//                    }
//
//                    if (metaData != null) {
//                        int columnCount = metaData.getColumnCount();
//                        for (int i = 1; i <= columnCount; i++) {
//                            OracleColumnMetadata meta = new OracleColumnMetadata();
//                            meta.setOwner(result.getOwner());
//                            meta.setTableName("SQL_RESULT");
//                            meta.setColumnName(metaData.getColumnLabel(i));
//                            meta.setDataType(metaData.getColumnTypeName(i));
//                            meta.setColumnId(i);
//                            meta.setDataLength(toNullableInteger(metaData.getColumnDisplaySize(i)));
//                            meta.setDataPrecision(toNullableInteger(metaData.getPrecision(i)));
//                            meta.setDataScale(toNullableInteger(metaData.getScale(i)));
//                            meta.setNullable(toNullableFlag(metaData.isNullable(i)));
//                            result.add(meta);
//                        }
//                    }
//                }
//            } finally {
//                if (originalSchema != null && !originalSchema.trim().isEmpty()) {
//                    // 还原原始 schema
//                    setCurrentSchema(conn, originalSchema);
//                }
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException("Execute SQL metadata query failed," + e.getMessage(), e);
//        }
//
//        return result;
//    }


    public OracleColumnsQueryResult queryColumnsBySql(OracleSqlColumnsQueryRequest request) {
        if (request == null) throw new IllegalArgumentException("request cannot be null");

        String sql = request.getSql();
        validateQuerySql(sql);

        String targetSchema = normalizeSchema(request.getSchema());
        OracleColumnsQueryResult result = new OracleColumnsQueryResult();

        try (Connection conn = dataSource.getConnection()) {
            String originalSchema = getCurrentSchema(conn);
            boolean switched = false;

            try {
                if (targetSchema != null && !targetSchema.equalsIgnoreCase(originalSchema)) {
                    setCurrentSchema(conn, targetSchema); // 这里可能 ORA-01031
                    switched = true;
                    result.setOwner(targetSchema);
                } else {
                    result.setOwner(originalSchema);
                }

                try  {
                    PreparedStatement ps = conn.prepareStatement(sql.trim());
                    ResultSetMetaData metaData = ps.getMetaData();
                    if (metaData == null) {
                        ps.setMaxRows(1);
                        try (ResultSet rs = ps.executeQuery()) {
                            metaData = rs.getMetaData();
                        }
                    }
                    if (metaData != null) {
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            OracleColumnMetadata meta = new OracleColumnMetadata();
                            meta.setOwner(result.getOwner());
                            meta.setTableName("SQL_RESULT");
                            meta.setColumnName(metaData.getColumnLabel(i));
                            meta.setDataType(metaData.getColumnTypeName(i));
                            meta.setColumnId(i);
                            meta.setDataLength(toNullableInteger(metaData.getColumnDisplaySize(i)));
                            meta.setDataPrecision(toNullableInteger(metaData.getPrecision(i)));
                            meta.setDataScale(toNullableInteger(metaData.getScale(i)));
                            meta.setNullable(toNullableFlag(metaData.isNullable(i)));
                            result.add(meta);
                        }
                    }


                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            } catch (SQLException e) {
                e.printStackTrace();
                // 保留 Oracle 错误码，主调可直接看到具体原因
                throw new RuntimeException(
                        "Execute SQL metadata query failed, oracleCode=" + e.getErrorCode() + ", message=" + e.getMessage(), e);
            } finally {
                // 恢复失败只记日志，不能再抛异常覆盖主异常
                if (switched) {
                    try {
                        setCurrentSchema(conn, originalSchema);
                    } catch (Exception restoreEx) {
                        logger.warn("restore schema failed, originalSchema={}, err={}",
                                originalSchema, restoreEx.getMessage(), restoreEx);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Execute SQL metadata query failed, " + e.getMessage(), e);
        }

        return result;
    }

    private void validateQuerySql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("sql cannot be empty");
        }
        String trimmed = sql.trim();
        if (trimmed.contains(";")) {
            throw new IllegalArgumentException("sql must be a single statement and cannot contain ';'");
        }
        String upper = trimmed.toUpperCase();
        if (!(upper.startsWith("SELECT ") || upper.startsWith("WITH "))) {
            throw new IllegalArgumentException("only SELECT/CTE sql is supported");
        }
    }

    private Integer toNullableInteger(int value) {
        return value <= 0 ? null : value;
    }

    private String toNullableFlag(int nullableCode) {
        if (nullableCode == ResultSetMetaData.columnNoNulls) {
            return "N";
        }
        if (nullableCode == ResultSetMetaData.columnNullable) {
            return "Y";
        }
        return null;
    }

    private Integer toInteger(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Integer) {
            return (Integer) v;
        }
        if (v instanceof java.math.BigDecimal) {
            return ((java.math.BigDecimal) v).intValue();
        }
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        return Integer.valueOf(String.valueOf(v));
    }



    private String normalizeSchema(String schema) {
        if (schema == null || schema.trim().isEmpty()) {
            return null;
        }
        String s = schema.trim().toUpperCase();
        if (!s.matches("[A-Z][A-Z0-9_\\$#]*")) {
            throw new IllegalArgumentException("invalid schema name");
        }
        return s;
    }

    private String getCurrentSchema(Connection conn) throws Exception {
        // Oracle下最稳妥
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private void setCurrentSchema(Connection conn, String schema) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "ALTER SESSION SET CURRENT_SCHEMA = " + schema)) {
            ps.execute();
        } catch (SQLException e) {
            if (e.getErrorCode() == 1031) {
                throw new SQLException("ORA-01031: insufficient privileges for ALTER SESSION/CURRENT_SCHEMA, schema=" + schema, e);
            }
            throw e;
        }
    }



















    /**
     * 获取Oracle表的建表DDL
     *
     * @param jdbcUrl  JDBC连接字符串，例如：jdbc:oracle:thin:@//host:port/service
     * @param dbUser   数据库登录用户名
     * @param dbPass   数据库登录密码
     * @param tableName  目标表名（大写）
     * @param owner    目标表所属用户（大写）
     * @return 建表DDL字符串
     */
    public static String getTableDDL(String jdbcUrl, String dbUser, String dbPass, String tableName, String owner) throws Exception {
        String ddl = null;
        String sql = "SELECT dbms_metadata.get_ddl('TABLE', ?, ?) FROM dual";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, owner);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    ddl = rs.getString(1);
                }
            }
        }
        return ddl;
    }

    // 示例主方法
    public static void main(String[] args) throws Exception {
        // 1. 配置Oracle数据源信息
        String jdbcUrl = "jdbc:oracle:thin:@//127.0.0.1:1521/orcl";
        String dbUser = "system";   // 用于连接的账号
        String dbPass = "oracle";   // 密码
        String tableName = "OPT_FEE"; // 表名（大写）
        String owner = "DEV_EHRVIEW"; // 拥有者（大写）

        String ddl = getTableDDL(jdbcUrl, dbUser, dbPass, tableName, owner);
        System.out.println(ddl);
    }
}
