package com.bsoft.server.sql.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import com.bsoft.common.bean.*;
import com.bsoft.common.ds.SharedHikariPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * ClassName: OracleDdlStrategy
 * Package:   com.bsoft.server.sql.ddl
 * Description: Oracle数据库DDL操作策略实现
 *
 * @Author: snoopy
 * @Create: 2025/8/17 - 12:50
 */
public class DorisDDLStrategy extends AbstractDDLStrategy {

    private static final Logger logger = LoggerFactory.getLogger(DorisDDLStrategy.class);

    public DorisDDLStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }


    /**
     * 将 Oracle 建表语句转换为 Doris 建表语句（带 TableKeys）
     * @param newTableName  目标 Doris 表名
     * @param oracleCreateTableSql Oracle CREATE TABLE DDL
     * @param tableKeys Oracle 表的约束信息（可为null）
     * @return Doris CREATE TABLE DDL
     */
    @Override
    public String convertOracleCreateTableToCurrentDB(String newTableName, String oracleCreateTableSql, TableKeys tableKeys) {
        try {
            // 1) 解析 Oracle CREATE TABLE 语句
            SQLCreateTableStatement oracleTable = parseOracleCreateTable(oracleCreateTableSql);

            // 2) 收集源表列
            List<ColMeta> srcCols = collectColumns(oracleTable);

            // 3) 始终添加 cdc_rowid 列
            ensureCdcRowIdExists(srcCols);

            // 4) 统一使用CDC_ROWID作为主键
            List<String> primaryKeyColumns = selectPrimaryKeyColumns(tableKeys, srcCols);

            // 5) 生成 Doris DDL（PRIMARY KEY 模型）
            return buildDorisPrimaryKeyCreate(newTableName, srcCols, primaryKeyColumns, 8, 1);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("转换 Oracle 建表语句到 Doris 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 确保 cdc_rowid 列存在
     */
    private void ensureCdcRowIdExists(List<ColMeta> srcCols) {
        boolean cdcRowIdExists = srcCols.stream()
                .anyMatch(col -> "cdc_rowid".equals(col.name));

        if (!cdcRowIdExists) {
            // 始终添加到开头，确保后续逻辑正确
            srcCols.add(0, buildCdcRowIdCol());
            System.out.println("添加 cdc_rowid 列");
        }
    }


    /**
     * 选择 PRIMARY KEY 列（按优先级）
     */
    private List<String> selectPrimaryKeyColumns(TableKeys tableKeys, List<ColMeta> srcCols) {
//        if(getServerInfo().getTableNamingStrategy() == TableNamingStrategy.TABLE_MERGE){
//            logger.info("合并模式：PrimaryKeys 使用 cdc_rowid ");
//            return Arrays.asList("cdc_rowid");
//        }
//
//        if (tableKeys == null) {
//            return Arrays.asList("cdc_rowid");
//        }
//
//
//        // 1. 优先使用主键
//        if (!tableKeys.getPrimaryKeys().isEmpty()) {
//            List<String> pkColumns = tableKeys.getPrimaryKeys().get(0).getColumns();
//            System.out.println("使用主键作为 PRIMARY KEY: " + pkColumns);
//            return normalizeColumnNames(pkColumns);
//        }
//
//        // 2. 使用唯一约束
//        if (!tableKeys.getUniqueConstraints().isEmpty()) {
//            List<String> ukColumns = tableKeys.getUniqueConstraints().get(0).getColumns();
//            System.out.println("使用唯一约束作为 PRIMARY KEY: " + ukColumns);
//            return normalizeColumnNames(ukColumns);
//        }
//
//        // 3. 使用唯一索引
//        if (!tableKeys.getUniqueIndexes().isEmpty()) {
//            List<String> uiColumns = tableKeys.getUniqueIndexes().get(0).getColumns();
//            System.out.println("使用唯一索引作为 PRIMARY KEY: " + uiColumns);
//            return normalizeColumnNames(uiColumns);
//        }
//
//        // 4. 兜底使用 cdc_rowid
//        System.out.println("未找到唯一键，使用 cdc_rowid 作为 PRIMARY KEY");
        return Arrays.asList("cdc_rowid");
    }

    /**
     * 生成 Doris PRIMARY KEY DDL
     */
    private String buildDorisPrimaryKeyCreate(String tableName,
                                              List<DorisDDLStrategy.ColMeta> cols,
                                              List<String> primaryKeyColumns,
                                              int buckets,
                                              int replicationNum) {
        // 重新排序列：UNIQUE KEY 列必须在最前面
        List<DorisDDLStrategy.ColMeta> reorderedCols = reorderColumnsForUniqueKey(cols, primaryKeyColumns);


        // 建表语句
        String colsDDL = buildColsDDL(reorderedCols);
        String primaryKeyDDL = buildUniqueKeyDDL(primaryKeyColumns);
        String firstPrimaryKeyCol = quote(primaryKeyColumns.get(0));

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(tableName.toLowerCase())).append(" (\n")
                .append(colsDDL).append("\n)\n")
                .append("UNIQUE KEY(").append(primaryKeyDDL).append(")\n")
                .append("DISTRIBUTED BY HASH(").append(firstPrimaryKeyCol).append(") BUCKETS ").append(buckets).append("\n")
                .append("PROPERTIES (\n")
                .append("  \"replication_num\"=\"").append(replicationNum).append("\"\n")
                .append(");");

        return sb.toString();
    }

    /**
     * 重新排序列，确保 UNIQUE KEY 列在最前面且按顺序排列
     */
    private List<DorisDDLStrategy.ColMeta> reorderColumnsForUniqueKey(List<DorisDDLStrategy.ColMeta> cols, List<String> uniqueKeyColumns) {
        List<DorisDDLStrategy.ColMeta> result = new ArrayList<>();
        Set<String> processedCols = new HashSet<>();

        // 1. 先添加 UNIQUE KEY 列（按顺序）
        for (String keyCol : uniqueKeyColumns) {
            for (DorisDDLStrategy.ColMeta col : cols) {
                if (keyCol.equalsIgnoreCase(col.name)) { // 使用 equalsIgnoreCase 更安全
                    result.add(col);
                    processedCols.add(col.name);
                    break;
                }
            }
        }

        // 2. 再添加其他列
        for (DorisDDLStrategy.ColMeta col : cols) {
            if (!processedCols.contains(col.name)) {
                result.add(col);
            }
        }

        System.out.println("重排序后的列顺序: " + result.stream().map(c -> c.name).collect(java.util.stream.Collectors.toList()));

        return result;
    }


    /**
     * 将列名标准化为小写
     */
    private List<String> normalizeColumnNames(List<String> columns) {
        List<String> normalized = new ArrayList<>();
        for (String col : columns) {
            normalized.add(normalize(col));
        }
        return normalized;
    }


    /**
     * 解析 Oracle CREATE TABLE 语句
     */
    private SQLCreateTableStatement parseOracleCreateTable(String oracleCreateTableSql) {
        try {
            // 尝试解析多语句
            List<SQLStatement> stmts = SQLUtils.parseStatements(oracleCreateTableSql, JdbcConstants.ORACLE);
            if (stmts != null && !stmts.isEmpty()) {
                for (SQLStatement s : stmts) {
                    if (s instanceof SQLCreateTableStatement) {
                        return (SQLCreateTableStatement) s;
                    }
                }
            }
        } catch (Exception ignored) {}

        // 回退到单语句解析
        try {
            String tableOnly = extractFirstCreateTableBlock(oracleCreateTableSql);
            if (tableOnly != null && !tableOnly.isEmpty()) {
                OracleStatementParser parser = new OracleStatementParser(tableOnly);
                SQLStatement stmt = parser.parseStatement();
                if (stmt instanceof SQLCreateTableStatement) {
                    return (SQLCreateTableStatement) stmt;
                }
            }
        } catch (Exception ignored) {}

        throw new IllegalArgumentException("未解析到有效的 CREATE TABLE 语句");
    }

    // ========== 元数据结构 ==========

    static class ColMeta {
        String name;       // 小写、去引号
        String oracleType; // 大写类型名
        Integer len;       // 对 varchar/char
        Integer prec;      // 对 number
        Integer scale;     // 对 number
        boolean notNull;   // 是否 NOT NULL
        boolean isArtificial; // 是否人工插入
    }

    private DorisDDLStrategy.ColMeta buildCdcRowIdCol() {
        ColMeta m = new ColMeta();
        m.name = "cdc_rowid";
        m.oracleType = "ROWID_VARCHAR";
        m.len = 64;
        m.notNull = true;
        m.isArtificial = true;
        return m;
    }

    // ========== 解析列 ==========
    private List<ColMeta> collectColumns(SQLCreateTableStatement oracleTable) {
        List<DorisDDLStrategy.ColMeta> cols = new ArrayList<>();
        for (SQLTableElement element : oracleTable.getTableElementList()) {
            if (element instanceof SQLColumnDefinition) {
                SQLColumnDefinition c = (SQLColumnDefinition) element;
                DorisDDLStrategy.ColMeta m = new DorisDDLStrategy.ColMeta();
                m.name = normalize(c.getName().getSimpleName());
                String typeName = c.getDataType().getName();
                m.oracleType = typeName == null ? "" : typeName.toUpperCase(Locale.ROOT);

                List<SQLExpr> args = c.getDataType().getArguments();
                fillTypeArgs(m, args);

                m.notNull = hasNotNull(c);
                m.isArtificial = false;

                cols.add(m);
            }
        }
        return cols;
    }

    private String normalize(String raw) {
        return raw == null ? "" : raw.replace("\"", "").toLowerCase(Locale.ROOT);
    }

    private boolean hasNotNull(SQLColumnDefinition col) {
        if (col.getConstraints() == null) return false;
        for (SQLColumnConstraint cc : col.getConstraints()) {
            if (cc instanceof SQLNotNullConstraint) return true;
        }
        return false;
    }

    private void fillTypeArgs(DorisDDLStrategy.ColMeta m, List<SQLExpr> args) {
        if (args == null || args.isEmpty()) return;
        try {
            if ("NUMBER".equals(m.oracleType)) {
                if (args.size() >= 1) m.prec = toInt(args.get(0).toString()).orElse(null);
                if (args.size() >= 2) m.scale = toInt(args.get(1).toString()).orElse(null);
            } else if ("VARCHAR2".equals(m.oracleType) || "NVARCHAR2".equals(m.oracleType)
                    || "VARCHAR".equals(m.oracleType) || "CHAR".equals(m.oracleType)) {
                m.len = toInt(args.get(0).toString()).orElse(null);
            }
        } catch (Exception ignored) {}
    }

    private Optional<Integer> toInt(String s) {
        try {
            return Optional.of(new BigDecimal(s.trim()).intValue());
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    // ========== 生成 Doris DDL ==========

    private String buildDorisCreate(String tableName,
                                    List<DorisDDLStrategy.ColMeta> cols,
                                    List<String> uniqueKeyColumns,
                                    int buckets,
                                    int replicationNum,
                                    boolean enableMoWForUniqueKey) {
        String colsDDL = buildColsDDL(cols);
        String uniqueKeyDDL = buildUniqueKeyDDL(uniqueKeyColumns);
        String firstUniqueKeyCol = quote(uniqueKeyColumns.get(0));

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(tableName.toLowerCase())).append(" (\n")
                .append(colsDDL).append("\n)\n")
                .append("UNIQUE KEY(").append(uniqueKeyDDL).append(")\n")
                .append("DISTRIBUTED BY HASH(").append(firstUniqueKeyCol).append(") BUCKETS ").append(buckets).append("\n")
                .append("PROPERTIES (\n")
                .append("  \"replication_num\"=\"").append(replicationNum).append("\"");

        if (enableMoWForUniqueKey) {
            sb.append(",\n  \"enable_unique_key_merge_on_write\"=\"true\"");
        }

        sb.append("\n);");
        return sb.toString();
    }

    private String buildColsDDL(List<DorisDDLStrategy.ColMeta> cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.size(); i++) {
            DorisDDLStrategy.ColMeta c = cols.get(i);
            if (i > 0) sb.append(",\n");
            sb.append("  ").append(quote(c.name)).append(" ").append(mapOracleToDoris(c));
            if (c.notNull) sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    private String buildUniqueKeyDDL(List<String> uniqueKeyColumns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uniqueKeyColumns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quote(uniqueKeyColumns.get(i)));
        }
        return sb.toString();
    }

    private String quote(String ident) {
        return "`" + ident.replace("`", "") + "`";
    }


    // oracle ->doris 字段类型映射
    private String mapOracleToDoris(ColMeta c) {
        // 人工 cdc_rowid 列
        if (c.isArtificial && "ROWID_VARCHAR".equals(c.oracleType)) {
            int len = (c.len != null ? c.len : 40);
            return "VARCHAR(" + len + ")";
        }

        String t = c.oracleType;
        if (t == null) return "STRING";
        switch (t) {
            case "NUMBER":
                if (c.scale != null) {
                    int p = c.prec != null ? c.prec : 38;
                    int s = c.scale;
                    p = clamp(p, 1, 38);
                    s = clamp(s, 0, Math.min(18, p));
                    return "DECIMAL(" + p + "," + s + ")";
                } else if (c.prec != null) {
                    int p = c.prec;
                    if (p <= 9)  return "INT";
                    if (p <= 18) return "BIGINT";
                    return "DECIMAL(" + clamp(p, 1, 38) + ",0)";
                } else {
                    return "BIGINT";
                }
            case "DATE":
                return this.dataSourceHolder.getTargetDataSourceConfig().shouldEnableDateTimeV2() ? "DATETIMEV2(0)" : "DATETIME";
            case "TIMESTAMP":
                return this.dataSourceHolder.getTargetDataSourceConfig().shouldEnableDateTimeV2() ? "DATETIMEV2(6)" : "DATETIME";

            case "VARCHAR2":
            case "VARCHAR":
            case "CHAR":
                return "VARCHAR(" + calcDorisVarcharLenByLen(c.len, false) + ")";
            case "NVARCHAR2":
                return "VARCHAR(" + calcDorisVarcharLenByLen(c.len, true) + ")";
            case "CLOB":
            case "BLOB":
                return "STRING";
            default:
                return "STRING";
        }
    }

    // 只用 len 计算 Doris VARCHAR 字节上限（不追求严格语义）
    private int calcDorisVarcharLenByLen(Integer len, boolean isNational) {
        final int DORIS_VARCHAR_MAX = 65533;

        int n = (len != null ? len : 255);

        // NVARCHAR2 更保守按 4B/char；其他按 3B/char
        double perChar = isNational ? 4.0 : 3.0;

        // 20% buffer（你也可以改成 1.0）
        double buffer = 1.2;

        int dorisLen = (int) Math.ceil(n * perChar * buffer);

        if (dorisLen < 1) dorisLen = 1;
        if (dorisLen > DORIS_VARCHAR_MAX) dorisLen = DORIS_VARCHAR_MAX;
        return dorisLen;
    }

    private int clamp(int v, int min, int max) {
        return Math.max(min, Math.min(max, v));
    }

    // ========== 工具方法 ==========

    private String extractFirstCreateTableBlock(String sql) {
        if (sql == null) return null;
        String s = sql.replace("\r\n", "\n");
        int start = indexOfIgnoreCase(s, "CREATE TABLE");
        if (start < 0) return null;
        String tail = s.substring(start);

        int pSemicolon = tail.indexOf(';');
        int pCreate = indexOfRegex(tail, "(?im)^\\s*CREATE\\b", 1);
        int pAlter  = indexOfRegex(tail, "(?im)^\\s*ALTER\\b", 0);
        int pSlash  = indexOfRegex(tail, "(?im)^\\s*/\\s*$", 0);

        int end = Integer.MAX_VALUE;
        if (pSemicolon >= 0) end = Math.min(end, pSemicolon);
        if (pCreate  >= 0) end = Math.min(end, pCreate);
        if (pAlter   >= 0) end = Math.min(end, pAlter);
        if (pSlash   >= 0) end = Math.min(end, pSlash);

        String one = (end == Integer.MAX_VALUE) ? tail : tail.substring(0, end);
        return one.trim();
    }

    private int indexOfIgnoreCase(String s, String needle) {
        return s.toLowerCase(Locale.ROOT).indexOf(needle.toLowerCase(Locale.ROOT));
    }

    private int indexOfRegex(String str, String pattern, int skipCount) {
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher m = p.matcher(str);
        int cnt = 0;
        while (m.find()) {
            if (cnt >= skipCount) return m.start();
            cnt++;
        }
        return -1;
    }

    // ========== 继承方法实现 ==========

    @Override
    protected String[] buildAddExtraColumnsSql(String tableName) {
        String lowerTableName = tableName.toLowerCase();
        String dateTimeFormat = this.dataSourceHolder.getTargetDataSourceConfig().shouldEnableDateTimeV2()
                ? "DATETIMEV2(0)"
                : "DATETIME";

        return new String[]{
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_op` varchar(32) NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_dml_time` " + dateTimeFormat + " NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_kafka_enqueue_time` " + dateTimeFormat + " NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_insert_time` " + dateTimeFormat + " NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_update_time` " + dateTimeFormat + " NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_time` " + dateTimeFormat + " NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_flag` varchar(1) NULL"
        };
    }

    @Override
    protected boolean isDuplicateColumnException(SQLException e) {
        return e.getErrorCode() == 1060; // Doris 与 MySQL 兼容
    }


    @Override
    public void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys) {
        // Doris 不需要单独创建索引和约束，UNIQUE KEY 已在建表时指定
        System.out.println("Doris 表 " + targetTableName + " 的 UNIQUE KEY 已在建表时创建，无需额外操作");
    }

    @Override
    public void ensureTableHasUniqueKey(String targetTableName) {
        // Doris UNIQUE KEY 表已经保证了唯一性，无需额外操作
        System.out.println("Doris 表 " + targetTableName + " 已有 UNIQUE KEY，无需额外操作");
    }

    @Override
    public void handleAddColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
//        validateColumnChangeInfo(columnChangeInfo);
//
//        String sql = String.format("ALTER TABLE %s ADD %s %s",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getColumnName(),
//                columnChangeInfo.getColumnType());
//
//        executeQuery(sql);
//        logger.info("✅ 成功新增字段: 表={}, 字段={}, 类型={}",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getColumnName(),
//                columnChangeInfo.getColumnType());
    }

    @Override
    public void handleModifyColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
//        validateColumnChangeInfo(columnChangeInfo);
//
//        String sql = String.format("ALTER TABLE %s MODIFY %s %s",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getColumnName(),
//                columnChangeInfo.getColumnType());
//
//        executeQuery(sql);
//        logger.info("✅ 成功修改字段: 表={}, 字段={}, 新类型={}",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getColumnName(),
//                columnChangeInfo.getColumnType());
    }

    @Override
    public void handleDropColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
//        validateColumnChangeInfo(columnChangeInfo);
//
//
//        String tableName = columnChangeInfo.getTableName();
//        String columnName = columnChangeInfo.getColumnName();
//        String newColumnName = "cdc_d_" + columnName;
//
//        try {
//            // 1. 先去掉NOT NULL约束（如果存在）
//            String modifyNullableSql = String.format("ALTER TABLE %s MODIFY (%s NULL)",
//                    tableName, columnName);
//            executeQuery(modifyNullableSql);
//            logger.debug("🔧 去掉字段必填约束: 表={}, 字段={}", tableName, columnName);
//
//            // 2. 重命名字段，添加删除前缀
//            String renameSql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
//                    tableName, columnName, newColumnName);
//            executeQuery(renameSql);
//
//            logger.info("✅ 成功标记删除字段: 表={}, 原字段名={}, 新字段名={}",
//                    tableName, columnName, newColumnName);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.error("❌ 标记删除字段失败: 表={}, 字段={}", tableName, columnName, e);
//            throw new Exception("标记删除字段失败: " + tableName + "." + columnName, e);
//        }
    }

    @Override
    public void handleRenameColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
//        validateColumnChangeInfo(columnChangeInfo);
//
//        if (columnChangeInfo.getOldColumnName() == null || columnChangeInfo.getOldColumnName().trim().isEmpty()) {
//            throw new IllegalArgumentException("重命名操作需要提供旧字段名");
//        }
//
//        String sql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getOldColumnName(),
//                columnChangeInfo.getColumnName());
//
//        executeQuery(sql);
//        logger.info("✅ 成功重命名字段: 表={}, 旧字段名={}, 新字段名={}",
//                columnChangeInfo.getTableName(),
//                columnChangeInfo.getOldColumnName(),
//                columnChangeInfo.getColumnName());
    }

    /**
     * 验证ColumnChangeInfo对象
     * @param columnChangeInfo 字段变更信息
     * @throws IllegalArgumentException 验证失败异常
     */
    private void validateColumnChangeInfo(ColumnChangeInfo columnChangeInfo) throws IllegalArgumentException {
//        if (columnChangeInfo == null) {
//            throw new IllegalArgumentException("字段变更信息不能为空");
//        }
//
//        if (columnChangeInfo.getTableName() == null || columnChangeInfo.getTableName().trim().isEmpty()) {
//            throw new IllegalArgumentException("表名不能为空");
//        }
//
//        if (columnChangeInfo.getColumnName() == null || columnChangeInfo.getColumnName().trim().isEmpty()) {
//            throw new IllegalArgumentException("字段名不能为空");
//        }
    }





    /**
     * 获取Doris表的完整字段信息
     * @param tableName 表名
     * @return 字段信息列表
     */
    public <T extends ColumnInfo> List<T> getTableColumns(String tableName) throws Exception {
        List<T> columns = new ArrayList<>();

        String sql = "SHOW FULL COLUMNS FROM " + tableName;

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                DorisColumnInfo column = DorisColumnInfo.builder()
                        .columnName(rs.getString("Field"))           // 字段名
                        .columnType(rs.getString("Type"))            // 完整类型(如: VARCHAR(100))
                        .nullable("YES".equalsIgnoreCase(rs.getString("Null"))) // 是否可为空
                        .key(rs.getString("Key"))                    // 键类型(PRI/UNI/MUL)
                        .defaultValue(rs.getString("Default"))       // 默认值
                        .extra(rs.getString("Extra"))                // 额外信息
                        .comment(rs.getString("Comment"))            // 字段注释
                        .ordinalPosition(columns.size() + 1)         // 字段顺序
                        .build();

                // 解析字段类型和长度
                parseColumnTypeInfo(column);

                columns.add((T) column);
            }

            logger.info("✅ 获取Doris表 {} 字段信息成功,共 {} 个字段", tableName, columns.size());

        } catch (Exception e) {
            logger.error("❌ 获取Doris表 {} 字段信息失败", tableName, e);
            throw e;
        }

        return columns;
    }

    /**
     * 解析字段类型信息(提取基础类型和长度)
     * @param column 字段信息对象
     */
    private void parseColumnTypeInfo(DorisColumnInfo column) {
        String fullType = column.getColumnType();
        if (fullType == null) {
            return;
        }

        // 提取基础类型和长度,如: VARCHAR(100) -> baseType=VARCHAR, length=100
        if (fullType.contains("(")) {
            int idx = fullType.indexOf("(");
            column.setBaseType(fullType.substring(0, idx).toUpperCase());

            String lengthPart = fullType.substring(idx + 1, fullType.indexOf(")"));
            // 处理 DECIMAL(10,2) 这种情况
            if (lengthPart.contains(",")) {
                String[] parts = lengthPart.split(",");
                column.setPrecision(Integer.parseInt(parts[0].trim()));
                column.setScale(Integer.parseInt(parts[1].trim()));
            } else {
                column.setLength(Integer.parseInt(lengthPart.trim()));
            }
        } else {
            column.setBaseType(fullType.toUpperCase());
        }
    }













}