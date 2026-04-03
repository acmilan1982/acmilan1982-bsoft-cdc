package com.bsoft.server.sql.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import com.bsoft.common.bean.*;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.ds.SharedHikariPool;
import com.bsoft.common.enums.DorisTableModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import com.bsoft.server.sql.doris.DorisCustomColumnMappingSupport;

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
     * @param tableKeys Oracle 表的约束信息（可为null）,
     *
     * @return Doris CREATE TABLE DDL
     */
    @Override
    public String convertOracleCreateTableToCurrentDB(
            String newTableName, String oracleCreateTableSql, TableKeys tableKeys, DDLBuildContext context) {

        try {


            // 解析 Oracle CREATE TABLE 语句
            SQLCreateTableStatement oracleTable = parseOracleCreateTable(oracleCreateTableSql);

            // 表级/系统级 建表属性：如果没有指定表级，则使用系统级
            DorisBizAttr bizAttr = resolveBizAttr(context);
            // 建表模型
            DorisTableModel model = resolveTableModel(bizAttr);


            // 获取最终的建表字段，包括 1.源表字段 2.内置字段 3.自定义字段
            List<ColMeta> finalCols = buildFinalColumns(oracleTable, bizAttr);

            // 判断排序列，分布列中，是否指定了cdc_rowid
            if (shouldEnsureCdcRowIdByBizAttr(bizAttr)) {
                ensureCdcRowIdExists(finalCols, model);
            }

            // 判断建表字段是否重复
            validateNoDuplicateColumns(finalCols);

            // 从建表属性中提取排序列
            List<String> keyColumns = resolveKeyColumns(bizAttr,finalCols);
            // 从建表属性中提取排序列
            List<String> hashColumns = resolveHashColumns(bizAttr,finalCols);

            int buckets = (bizAttr != null && bizAttr.getBuckets() != 0) ? bizAttr.getBuckets() : 8;
            int replicationNum = (bizAttr != null && bizAttr.getReplicationNum() != 0) ? bizAttr.getReplicationNum() : 1;

            // 创建doris建表语句
            return buildDorisCreateTableSql(newTableName, finalCols, keyColumns, hashColumns, model, buckets, replicationNum);


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("转换 Oracle 建表语句到 Doris 失败: " + e.getMessage(), e);
        }
    }

    // 表级/系统级 建表属性：如果没有指定表级，则使用系统级
    private DorisBizAttr resolveBizAttr(DDLBuildContext context) {
        if (context.isTableOverride()) {
            DdlBizAttr override = context.getBizAttrOverride();
            if (!(override instanceof DorisBizAttr)) {
                throw new IllegalArgumentException("TABLE_OVERRIDE requires DorisBizAttr for DorisDDLStrategy");
            }
            return (DorisBizAttr) override;
        }

        DdlBizAttr systemLevel = this.dataSourceHolder.getTargetDataSourceConfig().getSystemLevelDdlBizAttr();
        if (systemLevel == null) {
            return null;
        }
        if (!(systemLevel instanceof DorisBizAttr)) {
            throw new IllegalArgumentException("SYSTEM_DEFAULT requires DorisBizAttr for DorisDDLStrategy");
        }
        return (DorisBizAttr) systemLevel;
    }

    // 从建表属性中获取建表模型
    private DorisTableModel resolveTableModel(DorisBizAttr bizAttr) {
        if (bizAttr == null || bizAttr.getTableModel() == null) return DorisTableModel.UNIQUE;
        return bizAttr.getTableModel();
    }



    // 获取最终的建表字段，包括 1.源表字段 2.内置字段 3.自定义字段
    private List<ColMeta> buildFinalColumns(SQLCreateTableStatement oracleTable,
                                            DorisBizAttr bizAttr) {
        // cols：表示doris建表所需的所有字段，目前只包括源表所有字段 的 ColMeta对象
        List<ColMeta> cols = collectColumns(oracleTable);

        // 源表 字段名 集合
        Set<String> sourceCols = new HashSet<>();
        for (ColMeta c : cols) {
            sourceCols.add(c.name.toLowerCase(Locale.ROOT));
        }

        // cols中新增 内置 CDC列　
        appendBuiltinCdcColumns(cols);      // cdc_op/cdc_dml_time/... 固定 CDC 列

        // cols中新增 自定义列　
        applyCustomMappingColumns(cols, sourceCols, bizAttr);

        return cols;
    }

    // cols中新增 内置 CDC列　
    private void appendBuiltinCdcColumns(List<ColMeta> cols) { // 源表字段
        // 源表字段
        Set<String> exists = new HashSet<>();
        for (ColMeta c : cols) exists.add(c.name.toLowerCase(Locale.ROOT));

        for (ColMeta b : builtinCdcColumnMeta().values()) {
            if (!exists.contains(b.name.toLowerCase(Locale.ROOT))) {
                cols.add(b);
            }
        }
    }

    // cols中新增 自定义列
    private void applyCustomMappingColumns(List<ColMeta> cols,       // 源表字段 + 内置 CDC列(不包括cdc_rowid)
                                           Set<String> sourceCols,  // 源表列名集合
                                           DorisBizAttr bizAttr) {
        if (bizAttr == null || bizAttr.getCustomColumnMapping() == null || bizAttr.getCustomColumnMapping().isEmpty()) {
            return;
        }

        // 源表字段 + 已追加的内置 CDC 列
        Map<String, ColMeta> current = new LinkedHashMap<>();
        for (ColMeta c : cols) {
            current.put(c.name.toLowerCase(Locale.ROOT), c);
        }

        // 内置 CDC 列名
        Set<String> builtinCdcNames = builtinCdcColumnMeta().keySet();

        // 使用公共组件统一处理 custom_column_mapping
        Map<String, String> mapping = DorisCustomColumnMappingSupport
                .normalizeMapping(bizAttr.getCustomColumnMapping());
        DorisCustomColumnMappingSupport.validateNoCycle(mapping);

        for (Map.Entry<String, String> e : mapping.entrySet()) {
            String target = normalize(e.getKey());
            String source = normalize(e.getValue());

            if (builtinCdcNames.contains(target)) {
                throw new IllegalArgumentException("custom_column_mapping.key 不能是内置 CDC 列: " + target);
            }

            boolean sourceExists = sourceCols.contains(source);
            boolean builtinExists = builtinCdcNames.contains(source);
            if (!sourceExists && !builtinExists) {
                throw new IllegalArgumentException("custom_column_mapping.value 不存在于源列或内置 CDC 列: " + source);
            }

            // key 是源列且 key == value，忽略
            if (sourceCols.contains(target) && target.equalsIgnoreCase(source)) {
                continue;
            }

            // target 已存在(源列或已追加列)则不新增列；仅用于写入时映射
            if (current.containsKey(target)) {
                continue;
            }

            // 新增列：类型跟随映射源列
            ColMeta srcMeta = current.get(source);
            if (srcMeta == null) {
                throw new IllegalArgumentException("映射来源列元数据不存在: " + source);
            }

            ColMeta newMeta = cloneWithName(srcMeta, target);
            current.put(target, newMeta);
            cols.add(newMeta);
        }
    }

    // 判断排序列，分布列中，是否指定了cdc_rowid
    private boolean shouldEnsureCdcRowIdByBizAttr(DorisBizAttr bizAttr) {
        if (bizAttr == null) {
            return false;
        }

        List<String> keyColumns = normalizeColumnNames(bizAttr.getKeyColumns());
        if (keyColumns.contains(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER)) {
            return true;
        }

        List<String> hashColumns = normalizeColumnNames(bizAttr.getDistributedHashColumns());
        return hashColumns.contains(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
    }

    // 确保 cdc_rowid 列存在
    private void ensureCdcRowIdExists(List<ColMeta> srcCols, DorisTableModel tableModel) {
        // 判断 源表字段 中是否存在cdc_rowid，一般是不存在的
        boolean cdcRowIdExists = srcCols.stream()
                .anyMatch(col -> CdcColumnConstants.CDC_ROWID_COLUMN_LOWER.equals(col.name));

        if (!cdcRowIdExists) {
            // 始终添加到开头，确保后续逻辑正确
            srcCols.add(0, buildCdcRowIdCol(tableModel));
            System.out.println("添加 cdc_rowid 列");
        }
    }


    // 判断建表字段是否重复
    private void validateNoDuplicateColumns(List<ColMeta> finalCols) {
        if (finalCols == null || finalCols.isEmpty()) {
            return;
        }

        Map<String, Integer> nameCount = new LinkedHashMap<>();
        for (ColMeta col : finalCols) {
            if (col == null || col.name == null || col.name.trim().isEmpty()) {
                continue;
            }
            String normalized = col.name.toLowerCase(Locale.ROOT);
            nameCount.put(normalized, nameCount.getOrDefault(normalized, 0) + 1);
        }

        List<String> duplicates = new ArrayList<>();
        for (Map.Entry<String, Integer> e : nameCount.entrySet()) {
            if (e.getValue() > 1) {
                duplicates.add(e.getKey());
            }
        }

        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException("构建 Doris 建表字段失败，存在同名字段: " + duplicates);
        }
    }


    // 解析排序列
    private List<String> resolveKeyColumns( DorisBizAttr bizAttr,List<ColMeta> finalCols) {
        return resolveRequiredColumns(
                bizAttr == null ? null : bizAttr.getKeyColumns(),
                finalCols,
                "key_columns"
        );
    }

    //  解析分布列
    private List<String> resolveHashColumns(DorisBizAttr bizAttr,List<ColMeta> finalCols) {
        return resolveRequiredColumns(
                bizAttr == null ? null : bizAttr.getDistributedHashColumns(),
                finalCols,
                "distributed_hash_columns"
        );
    }


    private List<String> resolveRequiredColumns(List<String> configuredColumns,
                                                List<ColMeta> finalCols,
                                                String fieldName) {
        List<String> cols = normalizeColumnNames(configuredColumns);

        if (cols.isEmpty()) {
            throw new IllegalArgumentException("必须配置 " + fieldName);
        }

        // 去重但保序，避免配置重复列导致后续 DDL 异常
        cols = new ArrayList<>(new LinkedHashSet<>(cols));

        validateColumnsExist(cols, finalCols, fieldName);
        return cols;
    }

    private void validateColumnsExist(List<String> configCols, List<ColMeta> srcCols, String fieldName) {
        Set<String> allCols = new LinkedHashSet<>();
        for (ColMeta c : srcCols) {
            allCols.add(c.name.toLowerCase(Locale.ROOT));
        }

        List<String> missing = new ArrayList<>();
        for (String c : configCols) {
            String normalized = c == null ? "" : c.toLowerCase(Locale.ROOT);
            if (!allCols.contains(normalized)) {
                missing.add(c);
            }
        }

        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "DATA_SOURCE_BIZ_ATTR." + fieldName + " 包含不存在字段: " + missing + "，可用字段: " + allCols
            );
        }
    }



    // 创建doris建表语句
    private String buildDorisCreateTableSql(String tableName,
                                           List<ColMeta> cols,
                                           List<String> keyColumns,
                                           List<String> hashColumns,
                                           DorisTableModel tableModel,
                                           int buckets,
                                           int replicationNum) {
        // 建表字段进行排序，确保 key_columns列在最前面且按顺序排列
        List<ColMeta> reorderedCols = reorderColumnsForUniqueKey(cols, keyColumns); // key 列排最前
        String colsDDL = buildColsDDL(reorderedCols);

        String keyDDL = buildUniqueKeyDDL(keyColumns);
        String hashDDL = buildUniqueKeyDDL(hashColumns);

        String keyClause = (tableModel == DorisTableModel.DUPLICATE) ? "DUPLICATE KEY" : "UNIQUE KEY";

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(tableName.toLowerCase())).append(" (\n")
                .append(colsDDL).append("\n)\n")
                .append(keyClause).append("(").append(keyDDL).append(")\n")
                .append("DISTRIBUTED BY HASH(").append(hashDDL).append(") BUCKETS ").append(buckets).append("\n")
                .append("PROPERTIES (\n")
                .append("  \"replication_num\"=\"").append(replicationNum).append("\"");

        if (tableModel == DorisTableModel.UNIQUE) {
            sb.append(",\n  \"enable_unique_key_merge_on_write\"=\"true\"");
        }
        sb.append("\n);");
        return sb.toString();
    }


    // 重新排序列，确保 key_columns列在最前面且按顺序排列
    private List<DorisDDLStrategy.ColMeta> reorderColumnsForUniqueKey(List<DorisDDLStrategy.ColMeta> cols, List<String> uniqueKeyColumns) {
        // 需要对源表的列进行重新排序，这是排序结果
        List<DorisDDLStrategy.ColMeta> result = new ArrayList<>();

        Set<String> processedCols = new HashSet<>();

        // 1. 先添加 key_columns 中指定的列（按顺序）
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
        return Arrays.asList(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
    }





    /**
     * 将列名标准化为小写
     */
    private List<String> normalizeColumnNames(List<String> columns) {
        List<String> normalized = new ArrayList<>();
        if (columns == null) return normalized;
        for (String col : columns) {
            if (col == null || col.trim().isEmpty()) continue;
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
        enum ColOrigin {
            SOURCE,       // 源表字段
            BUILTIN_CDC,  // 内置 CDC 字段（含 cdc_rowid）
            CUSTOM        // custom_column_mapping 新增字段
        }
        String name;       // 小写、去引号
        String oracleType; // 大写类型名
        Integer len;       // 对 varchar/char
        Integer prec;      // 对 number
        Integer scale;     // 对 number
        boolean notNull;   // 是否 NOT NULL
        boolean isArtificial; // 是否人工插入
        ColOrigin origin;
    }

    // 按模型设置 notNull or null
    private DorisDDLStrategy.ColMeta buildCdcRowIdCol(DorisTableModel tableModel) {
        ColMeta m = new ColMeta();
        m.name = CdcColumnConstants.CDC_ROWID_COLUMN_LOWER;
        m.oracleType = "ROWID_VARCHAR";
        m.len = 64;

        // UNIQUE KEY: 不能为空; DUPLICATE KEY: 可为空
        m.notNull = (tableModel == DorisTableModel.UNIQUE);


        m.isArtificial = true;
        m.origin = ColMeta.ColOrigin.BUILTIN_CDC;
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
                m.origin = ColMeta.ColOrigin.SOURCE;

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




    // oracle ->doris 字段类型映射
    private String mapOracleToDoris(ColMeta c) {
        // 仅 SOURCE/CUSTOM 在 ALL_VARCHAR 下强制转 VARCHAR(4000)
        if (c.origin != ColMeta.ColOrigin.BUILTIN_CDC && isAllVarcharModel()) {
            return "VARCHAR(4000)";
        }

        // 以下保留你现有逻辑（特别是 BUILTIN_CDC 的 DATETIME/DATETIMEV2 分支）
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
            case "DATETIMEV2_0":
                return "DATETIMEV2(0)";
            case "DATETIME":
                return "DATETIME";
            default:
                return "STRING";
        }
    }


    private boolean isAllVarcharModel() {
        DdlBizAttr ddlBizAttr = this.dataSourceHolder.getTargetDataSourceConfig().getSystemLevelDdlBizAttr();
        DorisBizAttr bizAttr = (ddlBizAttr instanceof DorisBizAttr) ? (DorisBizAttr) ddlBizAttr : null;
        return bizAttr != null
                && bizAttr.getColumnModel() != null
                && "ALL_VARCHAR".equalsIgnoreCase(bizAttr.getColumnModel().name());
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
//        String lowerTableName = tableName.toLowerCase();
//        String dateTimeFormat = this.dataSourceHolder.getTargetDataSourceConfig().shouldEnableDateTimeV2()
//                ? "DATETIMEV2(0)"
//                : "DATETIME";
//
//        return new String[]{
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_op` varchar(32) NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_dml_time` " + dateTimeFormat + " NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_kafka_enqueue_time` " + dateTimeFormat + " NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_insert_time` " + dateTimeFormat + " NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_update_time` " + dateTimeFormat + " NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_time` " + dateTimeFormat + " NULL",
//                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_flag` varchar(1) NULL"
//        };
        return new String[]{};
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










    private ColMeta cloneWithName(ColMeta src, String targetName) {
        ColMeta c = new ColMeta();
        c.name = normalize(targetName);
        c.oracleType = src.oracleType;
        c.len = src.len;
        c.prec = src.prec;
        c.scale = src.scale;
        c.notNull = false;
        c.isArtificial = true;
        c.origin = ColMeta.ColOrigin.CUSTOM;
        return c;
    }

    // 固定 内置CDC 列元数据
    private Map<String, ColMeta> builtinCdcColumnMeta() {
        Map<String, ColMeta> m = new LinkedHashMap<>();

        String dateTimeType = this.dataSourceHolder.getTargetDataSourceConfig().shouldEnableDateTimeV2()
                ? "DATETIMEV2_0"
                : "DATETIME";

        m.put("cdc_op", col("cdc_op", "VARCHAR2", 32, false));
        m.put("cdc_dml_time", col("cdc_dml_time", dateTimeType, null, false));
        m.put("cdc_kafka_enqueue_time", col("cdc_kafka_enqueue_time", dateTimeType, null, false));
        m.put("cdc_insert_time", col("cdc_insert_time", dateTimeType, null, false));
        m.put("cdc_update_time", col("cdc_update_time", dateTimeType, null, false));
        m.put("cdc_delete_time", col("cdc_delete_time", dateTimeType, null, false));
        m.put("cdc_delete_flag", col("cdc_delete_flag", "VARCHAR2", 1, false));

        return m;
    }

    // 创建 内置CDC 列元数据 对应的ColMeta
    private ColMeta col(String name, String type, Integer len, boolean notNull) {
        ColMeta c = new ColMeta();
        c.name = normalize(name);
        c.oracleType = type;
        c.len = len;
        c.notNull = notNull;
        c.isArtificial = true;
        c.origin = ColMeta.ColOrigin.BUILTIN_CDC;
        return c;
    }




}