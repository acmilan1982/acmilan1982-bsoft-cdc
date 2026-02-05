package com.bsoft.server.sql.ddl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OraclePrimaryKey;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.bsoft.common.bean.*;
import com.bsoft.server.sql.converter.OracleToMySQLTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: MySQLDdlStrategy
 * Package:   com.bsoft.server.sql.ddl
 * Description: MySQL数据库DDL操作策略实现
 *
 * @Author: snoopy
 * @Create: 2025/8/29 - 14:35
 */
public class MySQLDDLStrategy extends AbstractDDLStrategy {

    private static final Logger logger = LoggerFactory.getLogger(MySQLDDLStrategy.class);

    public MySQLDDLStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }





    @Override
    public String convertOracleCreateTableToCurrentDB(String newTableName, String oracleCreateTableSql, TableKeys tableKey) {
        try {
            // 解析Oracle建表语句
            OracleStatementParser parser = new OracleStatementParser(oracleCreateTableSql);
            SQLStatement stmt = parser.parseStatement();
            if (!(stmt instanceof SQLCreateTableStatement)) {
                throw new IllegalArgumentException("不是有效的建表语句");
            }

            SQLCreateTableStatement oracleTable = (SQLCreateTableStatement) stmt;

            List<ColumnInfo> columnInfos = new ArrayList<>();
            List<String> primaryKeys = new ArrayList<>();
            List<String> indexStatements = new ArrayList<>();

            // 第一次遍历：收集字段信息
            for (SQLTableElement element : oracleTable.getTableElementList()) {
                if (element instanceof SQLColumnDefinition) {
                    SQLColumnDefinition col = (SQLColumnDefinition) element;
                    String columnName = col.getName().getSimpleName()
                            .toLowerCase()
                            .replace("\"", "");

                    String mysqlType = convertOracleTypeToMySQL(
                            col.getDataType().getName(),
                            col.getDataType().getArguments()
                    );

                    ColumnInfo columnInfo = new ColumnInfo();
                    columnInfo.name = columnName;
                    columnInfo.type = mysqlType;
                    columnInfo.originalColumn = col;

                    columnInfos.add(columnInfo);

                } else if (element instanceof OraclePrimaryKey) {
                    // 处理主键
                    OraclePrimaryKey pk = (OraclePrimaryKey) element;
                    for (SQLSelectOrderByItem col : pk.getColumns()) {
                        String pkColumn = col.getExpr().toString()
                                .toLowerCase()
                                .replace("\"", "");
                        primaryKeys.add("`" + pkColumn + "`");
                    }
                } else if (element instanceof SQLUnique) {
                    // 处理唯一约束
                    SQLUnique unique = (SQLUnique) element;
                    List<String> uniqueColumns = new ArrayList<>();
                    for (SQLSelectOrderByItem col : unique.getColumns()) {
                        String uniqueColumn = col.getExpr().toString()
                                .toLowerCase()
                                .replace("\"", "");
                        uniqueColumns.add("`" + uniqueColumn + "`");
                    }
                    String indexName = unique.getName() != null ?
                            unique.getName().getSimpleName().toLowerCase() :
                            randomIndexName("uk");
                    indexStatements.add("CREATE UNIQUE INDEX `" + indexName + "` ON `" +
                            newTableName.toLowerCase() + "` (" + String.join(", ", uniqueColumns) + ");");

                } else if (element instanceof SQLIndexDefinition) {
                    // 处理普通索引
                    SQLIndexDefinition index = (SQLIndexDefinition) element;
                    List<String> indexColumns = new ArrayList<>();
                    for (SQLSelectOrderByItem col : index.getColumns()) {
                        String indexColumn = col.getExpr().toString()
                                .toLowerCase()
                                .replace("\"", "");
                        indexColumns.add("`" + indexColumn + "`");
                    }
                    String indexName = index.getName() != null ?
                            index.getName().getSimpleName().toLowerCase() :
                            randomIndexName("idx");
                    indexStatements.add("CREATE INDEX `" + indexName + "` ON `" +
                            newTableName.toLowerCase() + "` (" + String.join(", ", indexColumns) + ");");
                }
            }

            // 估算字节数并优化类型
            optimizeColumnTypes(columnInfos);

            // 构建最终的字段定义
            List<String> columnLines = new ArrayList<>();
            for (ColumnInfo columnInfo : columnInfos) {
                StringBuilder colLine = new StringBuilder();
                colLine.append("  `").append(columnInfo.name).append("` ").append(columnInfo.type);

                // 处理NOT NULL约束
                if (columnInfo.originalColumn.getConstraints() != null) {
                    for (SQLColumnConstraint constraint : columnInfo.originalColumn.getConstraints()) {
                        if (constraint instanceof SQLNotNullConstraint) {
                            colLine.append(" NOT NULL");
                        }
                    }
                }

                // 处理默认值
                if (columnInfo.originalColumn.getDefaultExpr() != null) {
                    String defaultValue = columnInfo.originalColumn.getDefaultExpr().toString();
                    if (!defaultValue.toUpperCase().contains("SYSDATE")) {
                        if (columnInfo.type.contains("datetime") || columnInfo.type.contains("timestamp")) {
                            colLine.append(" DEFAULT CURRENT_TIMESTAMP");
                        } else {
                            colLine.append(" DEFAULT ").append(defaultValue);
                        }
                    }
                }

                columnLines.add(colLine.toString());
            }

            // 构建MySQL CREATE TABLE语句
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE `").append(newTableName.toLowerCase()).append("` (\n");
            sb.append(String.join(",\n", columnLines));

            // 建表时不设置主键，统一使用CDC_ROWID作为主键
            // 添加主键
//            if (!primaryKeys.isEmpty()) {
//                sb.append(",\n  PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
//            }

            sb.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;\n");

            // 添加索引语句
            for (String indexStmt : indexStatements) {
                sb.append("\n").append(indexStmt);
            }

            return sb.toString();

        } catch (Exception e) {
            throw new RuntimeException("转换Oracle建表语句到MySQL失败: " + e.getMessage(), e);
        }
    }

    /**
     * 字段信息类
     */
    private static class ColumnInfo {
        String name;
        String type;
        SQLColumnDefinition originalColumn;
    }

    /**
     * 优化字段类型，避免行长度超限
     */
    private void optimizeColumnTypes(List<ColumnInfo> columnInfos) {
        // 估算总字节数
        int totalBytes = calculateRowBytes(columnInfos);

        // 如果超过60000字节，进行优化
        if (totalBytes > 60000) {
            System.out.println("⚠️  表行长度估算: " + totalBytes + " 字节，开始优化字段类型...");

            for (ColumnInfo columnInfo : columnInfos) {
                String originalType = columnInfo.type;

                // 将长度>=1000的varchar转换为text
                if (isLongVarchar(columnInfo.type)) {
                    columnInfo.type = "text";
                    System.out.println("🔄 字段 " + columnInfo.name + " 类型优化: " + originalType + " → text");
                }
            }

            int optimizedBytes = calculateRowBytes(columnInfos);
            System.out.println("✅ 优化后行长度估算: " + optimizedBytes + " 字节");
        } else {
            System.out.println("✅ 表行长度估算: " + totalBytes + " 字节，无需优化");
        }
    }

    /**
     * 计算一行数据的估算字节数
     */
    private int calculateRowBytes(List<ColumnInfo> columnInfos) {
        int totalBytes = 0;

        for (ColumnInfo columnInfo : columnInfos) {
            totalBytes += getColumnBytes(columnInfo.type);
        }

        // 加上MySQL行头开销（大约8-10字节）
        totalBytes += 10;

        return totalBytes;
    }

    /**
     * 获取单个字段的字节数估算
     */
    private int getColumnBytes(String mysqlType) {
        mysqlType = mysqlType.toLowerCase();

        if (mysqlType.startsWith("varchar(")) {
            // 提取varchar长度
            Pattern pattern = Pattern.compile("varchar\\((\\d+)\\)");
            Matcher matcher = pattern.matcher(mysqlType);
            if (matcher.find()) {
                int length = Integer.parseInt(matcher.group(1));
                // utf8mb4编码，每字符最多4字节，加上长度字段开销
                return length * 4 + (length > 255 ? 2 : 1);
            }
            return 1024; // 默认估算

        } else if (mysqlType.startsWith("char(")) {
            // 提取char长度
            Pattern pattern = Pattern.compile("char\\((\\d+)\\)");
            Matcher matcher = pattern.matcher(mysqlType);
            if (matcher.find()) {
                int length = Integer.parseInt(matcher.group(1));
                return length * 4; // utf8mb4编码
            }
            return 4;

        } else if (mysqlType.startsWith("decimal(")) {
            // decimal类型根据精度计算
            return 9; // 平均估算

        } else if (mysqlType.equals("int")) {
            return 4;
        } else if (mysqlType.equals("bigint")) {
            return 8;
        } else if (mysqlType.equals("datetime") || mysqlType.equals("timestamp")) {
            return 8;
        } else if (mysqlType.equals("text")) {
            return 12; // text类型只存储指针，实际数据在页外
        } else if (mysqlType.equals("blob")) {
            return 12;
        } else {
            return 20; // 默认估算
        }
    }

    /**
     * 判断是否为长varchar类型（长度>=1000）
     */
    private boolean isLongVarchar(String mysqlType) {
        if (!mysqlType.toLowerCase().startsWith("varchar(")) {
            return false;
        }

        Pattern pattern = Pattern.compile("varchar\\((\\d+)\\)");
        Matcher matcher = pattern.matcher(mysqlType.toLowerCase());
        if (matcher.find()) {
            int length = Integer.parseInt(matcher.group(1));
            return length >= 1000;
        }

        return false;
    }


    private  String convertOracleTypeToMySQL(String oracleType, List<SQLExpr> args) {
        oracleType = oracleType.toUpperCase();
        switch (oracleType) {
            case "VARCHAR2":
            case "NVARCHAR2":
            case "VARCHAR":
                return "varchar(" + (args.isEmpty() ? "255" : args.get(0)) + ")";
            case "CHAR":
                return "char(" + (args.isEmpty() ? "1" : args.get(0)) + ")";
            case "NUMBER":
                if (args.size() == 2) {
                    return "decimal(" + args.get(0) + "," + args.get(1) + ")";
                } else if (args.size() == 1) {
                    return "decimal(" + args.get(0) + ")";
                } else {
                    return "int";
                }
            case "DATE":
            case "TIMESTAMP":
                return "datetime";
            case "CLOB":
                return "text";
            case "BLOB":
                return "blob";
            default:
                return oracleType.toLowerCase(); // fallback
        }
    }


    private  String randomIndexName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }


    // 向目标表中添加额外的列（如有需要）
    @Override
    protected String[] buildAddExtraColumnsSql(String tableName) {
        String lowerTableName = tableName.toLowerCase();
        return new String[]{
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_rowid` varchar(64) NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_op` varchar(32) NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_dml_time` datetime NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_kafka_enqueue_time` datetime NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_insert_time` datetime NULL DEFAULT CURRENT_TIMESTAMP",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_update_time` datetime NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_time` datetime NULL",
                "ALTER TABLE `" + lowerTableName + "` ADD COLUMN `cdc_delete_flag` varchar(1) NULL DEFAULT '0'"
        };
    }

    @Override
    protected boolean isDuplicateColumnException(SQLException e) {
        return e.getErrorCode() == 1060; // MySQL duplicate column error
    }




    @Override
    public void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {

            // 创建主键约束
            for (KeyDef primaryKey : tableKeys.getPrimaryKeys()) {
                String randomConstraintName = randomIndexName("pk");
                // 将列名转换为MySQL格式并加反引号
                List<String> mysqlColumns = new ArrayList<>();
                for (String column : primaryKey.getColumns()) {
                    mysqlColumns.add("`" + column.toLowerCase() + "`");
                }
                String sql = String.format("ALTER TABLE `%s` ADD CONSTRAINT `%s` PRIMARY KEY (%s)",
                        targetTableName.toLowerCase(),
                        randomConstraintName,
                        String.join(", ", mysqlColumns));
                stmt.execute(sql);
                System.out.println("已创建主键约束: " + randomConstraintName);
            }

            // 创建唯一约束（合并唯一约束和唯一索引）
            for (KeyDef uniqueConstraint : tableKeys.getUniqueConstraints()) {
                String randomIndexName = randomIndexName("uk");
                List<String> mysqlColumns = new ArrayList<>();
                for (String column : uniqueConstraint.getColumns()) {
                    mysqlColumns.add("`" + column.toLowerCase() + "`");
                }
                String sql = String.format("CREATE UNIQUE INDEX `%s` ON `%s` (%s)",
                        randomIndexName,
                        targetTableName.toLowerCase(),
                        String.join(", ", mysqlColumns));
                stmt.execute(sql);
                System.out.println("已创建唯一索引（唯一约束）: " + randomIndexName);
            }

            // 创建唯一索引
            for (KeyDef uniqueIndex : tableKeys.getUniqueIndexes()) {
                String randomIndexName = randomIndexName("uk");
                List<String> mysqlColumns = new ArrayList<>();
                for (String column : uniqueIndex.getColumns()) {
                    mysqlColumns.add("`" + column.toLowerCase() + "`");
                }
                String sql = String.format("CREATE UNIQUE INDEX `%s` ON `%s` (%s)",
                        randomIndexName,
                        targetTableName.toLowerCase(),
                        String.join(", ", mysqlColumns));
                stmt.execute(sql);
                System.out.println("已创建唯一索引: " + randomIndexName);
            }

            // 创建普通索引
            for (KeyDef normalIndex : tableKeys.getNormalIndexes()) {
                String randomIndexName = randomIndexName("idx");
                List<String> mysqlColumns = new ArrayList<>();
                for (String column : normalIndex.getColumns()) {
                    mysqlColumns.add("`" + column.toLowerCase() + "`");
                }
                String sql = String.format("CREATE INDEX `%s` ON `%s` (%s)",
                        randomIndexName,
                        targetTableName.toLowerCase(),
                        String.join(", ", mysqlColumns));
                stmt.execute(sql);
                System.out.println("已创建普通索引: " + randomIndexName);
            }

        } catch (Exception e) {
            System.err.println("创建表索引和约束失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("创建表索引和约束失败", e);
        }
    }

    @Override
    public void ensureTableHasUniqueKey(String targetTableName) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection()) {
            // 1. 检查MySQL表是否已有主键或唯一键
            boolean hasUniqueKey = checkMySqlTableHasUniqueKey(conn, targetTableName);

            // 2. 如果没有唯一键，使用CDC_ROWID作为主键
            if (!hasUniqueKey) {
                String constraintName = randomIndexName("pk");
                String sql = String.format("ALTER TABLE `%s` ADD CONSTRAINT `%s` PRIMARY KEY (`cdc_rowid`)",
                        targetTableName.toLowerCase(),
                        constraintName);

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                    System.out.println("已为表 " + targetTableName + " 创建CDC_ROWID主键约束: " + constraintName);
                }
            } else {
                System.out.println("表 " + targetTableName + " 已有唯一键，无需创建CDC_ROWID主键");
            }

        } catch (Exception e) {
            throw new RuntimeException("为表 " + targetTableName + " 设置唯一键失败: " + e.getMessage(), e);
        }
    }

    /**
     * 检查MySQL表是否已有主键或唯一键
     */
    private boolean checkMySqlTableHasUniqueKey(Connection conn, String tableName) throws SQLException {
        // 检查主键
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.table_constraints " +
                        "WHERE table_schema = DATABASE() AND table_name = ? AND constraint_type = 'PRIMARY KEY'")) {
            ps.setString(1, tableName.toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    return true;
                }
            }
        }

        // 检查唯一键
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM information_schema.table_constraints " +
                        "WHERE table_schema = DATABASE() AND table_name = ? AND constraint_type = 'UNIQUE'")) {
            ps.setString(1, tableName.toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public void handleAddColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        // 转换Oracle类型到MySQL类型
        String mysqlType = OracleToMySQLTypeConverter.convertType(columnChangeInfo.getColumnType());

        String sql = String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                mysqlType);

        executeQuery(sql);
        logger.info("✅ 成功新增字段: 表={}, 字段={}, Oracle类型={}, MySQL类型={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType(),
                mysqlType);
    }

    @Override
    public void handleModifyColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        // 转换Oracle类型到MySQL类型
        String mysqlType = OracleToMySQLTypeConverter.convertType(columnChangeInfo.getColumnType());

        String sql = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                mysqlType);

        executeQuery(sql);
        logger.info("✅ 成功修改字段: 表={}, 字段={}, Oracle类型={}, MySQL类型={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType(),
                mysqlType);
    }

    @Override
    public void handleDropColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        String tableName = columnChangeInfo.getTableName();
        String columnName = columnChangeInfo.getColumnName();
        String newColumnName = "cdc_d_" + columnName;

        try {
            // 1. 先查询原字段的完整类型定义
            String originalColumnType = getColumnType(tableName, columnName);
            if (originalColumnType == null) {
                throw new Exception("无法找到字段 " + columnName + " 的类型信息");
            }

            // 2. 去掉NOT NULL约束，保留其他属性
            String nullableColumnType = originalColumnType.replaceAll("(?i)\\s+NOT\\s+NULL", "");

            String modifyNullableSql = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s",
                    tableName, columnName, nullableColumnType);
            executeQuery(modifyNullableSql);
            logger.debug("🔧 去掉字段必填约束: 表={}, 字段={}", tableName, columnName);

            // 3. 重命名字段，添加删除前缀
            String renameSql = String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s",
                    tableName, columnName, newColumnName, nullableColumnType);
            executeQuery(renameSql);

            logger.info("✅ 成功标记删除字段: 表={}, 原字段名={}, 新字段名={}",
                    tableName, columnName, newColumnName);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("❌ 标记删除字段失败: 表={}, 字段={}", tableName, columnName, e);
            throw new Exception("标记删除字段失败: " + tableName + "." + columnName, e);
        }
    }

    @Override
    public void handleRenameColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        if (columnChangeInfo.getOldColumnName() == null || columnChangeInfo.getOldColumnName().trim().isEmpty()) {
            throw new IllegalArgumentException("重命名操作需要提供旧字段名");
        }

        // 查询原字段的数据类型
        String originalColumnType = getColumnType(columnChangeInfo.getTableName(), columnChangeInfo.getOldColumnName());

        if (originalColumnType == null) {
            throw new Exception("无法找到字段 " + columnChangeInfo.getOldColumnName() + " 的类型信息");
        }

        // MySQL 5.x CHANGE COLUMN语法，需要指定完整的字段定义
        String sql = String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getOldColumnName(),
                columnChangeInfo.getColumnName(),
                originalColumnType);

        executeQuery(sql);
        logger.info("✅ 成功重命名字段: 表={}, 旧字段名={}, 新字段名={}, 类型={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getOldColumnName(),
                columnChangeInfo.getColumnName(),
                originalColumnType);
    }

    /**
     * 查询字段的数据类型
     * @param tableName 表名
     * @param columnName 字段名
     * @return 字段的完整类型定义
     * @throws Exception 查询异常
     */
    private String getColumnType(String tableName, String columnName) throws Exception {
        String sql = "SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";

        try (Connection connection = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            statement.setString(1, tableName);
            statement.setString(2, columnName);

            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String columnType = resultSet.getString("COLUMN_TYPE");
                    logger.debug("🔍 查询到字段类型: 表={}, 字段={}, 类型={}", tableName, columnName, columnType);
                    return columnType;
                } else {
                    logger.warn("⚠️ 未找到字段信息: 表={}, 字段={}", tableName, columnName);
                    return null;
                }
            }

        } catch (SQLException e) {
            logger.error("❌ 查询字段类型失败: 表={}, 字段={}", tableName, columnName, e);
            throw new Exception("查询字段类型失败: " + tableName + "." + columnName, e);
        }
    }

    /**
     * 验证ColumnChangeInfo对象
     * @param columnChangeInfo 字段变更信息
     * @throws IllegalArgumentException 验证失败异常
     */
    private void validateColumnChangeInfo(ColumnChangeInfo columnChangeInfo) throws IllegalArgumentException {
        if (columnChangeInfo == null) {
            throw new IllegalArgumentException("字段变更信息不能为空");
        }

        if (columnChangeInfo.getTableName() == null || columnChangeInfo.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("表名不能为空");
        }

        if (columnChangeInfo.getColumnName() == null || columnChangeInfo.getColumnName().trim().isEmpty()) {
            throw new IllegalArgumentException("字段名不能为空");
        }
    }



}