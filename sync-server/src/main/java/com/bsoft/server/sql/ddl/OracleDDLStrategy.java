package com.bsoft.server.sql.ddl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OraclePrimaryKey;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.bsoft.common.bean.*;
import com.bsoft.common.util.OracleKeyInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * ClassName: OracleDdlStrategy
 * Package:   com.bsoft.server.sql.ddl
 * Description: Oracle数据库DDL操作策略实现
 *
 * @Author: snoopy
 * @Create: 2025/8/17 - 12:50
 */
public class OracleDDLStrategy extends AbstractDDLStrategy {

    private static final Logger logger = LoggerFactory.getLogger(OracleDDLStrategy.class);


    public OracleDDLStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }



    protected String preprocessCreateTableSql(String sql) {
        // 清理SQL语句中的换行符和多余空格
        String cleanSql = sql
                .replaceAll("\\r\\n|\\r|\\n", " ")
                .replaceAll("\\s+", " ")
                .trim();

        System.out.println("原始SQL: " + sql);
        System.out.println("清理后SQL: " + cleanSql);
        return cleanSql;
    }


    //  将Oracle的建表语句转换为当前数据库的建表语句，当前步骤不处理主键等约束和索引
    @Override
    public  String convertOracleCreateTableToCurrentDB(String newTableName,String oracleCreateTableSql,
                                                       TableKeys tableKeys, DDLBuildContext context) {
        // 解析原建表语句
        OracleStatementParser parser = new OracleStatementParser(oracleCreateTableSql);
        SQLStatement stmt = parser.parseStatement();
        if (!(stmt instanceof SQLCreateTableStatement)) {
            throw new IllegalArgumentException("不是建表语句");
        }

        SQLCreateTableStatement table = (SQLCreateTableStatement) stmt;
        // 普通字段
        List<String> normalColumns = new ArrayList<>();
        // 主键字段，建表的时候，只添加主键，不添加其他约束和索引
        List<String> primaryKeys = new ArrayList<>();

        // 只要字段和主键即可，toString结果就是标准Oracle格式
        for (SQLTableElement element : table.getTableElementList()) {
            // 普通字段
            if (element instanceof SQLColumnDefinition) {
                normalColumns.add("  " + element.toString());
            } else if (element instanceof OraclePrimaryKey) {
                OraclePrimaryKey oraclePrimaryKey = (OraclePrimaryKey) element;
                oraclePrimaryKey.getColumns().forEach(col -> primaryKeys.add(col.toString()));
            }
        }


//        String constraintName = randomIndexName("PK");

        // 拼接CREATE TABLE语句
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(newTableName).append(" (\n");
        sb.append(String.join(",\n", normalColumns));
        // 建表时不设置主键，统一使用CDC_ROWID作为主键
//        if (!primaryKeys.isEmpty()) {
//            sb.append(",\n");
//            sb.append(" CONSTRAINT \"").append(constraintName).append("\"  PRIMARY KEY ").append(buildOracleKeyList(primaryKeys));
//        }
        sb.append("\n)");
        return sb.toString();
    }



    /**
     * 生成随机索引名，格式如：IDX_20240804_AB12CD34
     * @param prefix   前缀（如IDX/PK/UK/FK等）
     * @return         索引名
     */
    public  String randomIndexName(String prefix) {
        // 取当前日期+UUID前缀片段，保证基本唯一且不太长
        String date = new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date());
        String uuid = UUID.randomUUID().toString().replace("-", "").toUpperCase();
        String rand = uuid.substring(0, 8); // 可按需调整长度
        String name = prefix.toUpperCase() + "_" + date + "_" + rand;
        // Oracle对象名最大30字符，必要时再截断
        return name.length() > 30 ? name.substring(0, 30) : name;
    }


    public  String buildOracleKeyList(List<String> primaryKeys) {
        return "(" + String.join(", ", primaryKeys) + ")";
    }


    // 向目标表中添加额外的列（如有需要）
    @Override
    protected String[] buildAddExtraColumnsSql(String tableName) {
        String upperTableName = tableName.toUpperCase();
        return new String[]{
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_ROWID\" VARCHAR2(64)",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_OP\" VARCHAR2(32)",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_DML_TIME\" DATE",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_KAFKA_ENQUEUE_TIME\" DATE",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_INSERT_TIME\" DATE DEFAULT SYSDATE",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_UPDATE_TIME\" DATE",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_DELETE_TIME\" DATE",
                "ALTER TABLE \"" + upperTableName + "\" ADD \"CDC_DELETE_FLAG\" VARCHAR2(1) DEFAULT '0'"
        };
    }

    @Override
    protected boolean isDuplicateColumnException(SQLException e) {
        return e.getErrorCode() == 957; // Oracle duplicate column error
    }


    // 应用源表约束和索引到目标表,TableKeys包含了源表所有的约束与索引信息
    // 考虑到多个源表的数据合并到一个目标表，目前，目标表只应用普通索引
    @Override
    public void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {

            // 创建主键约束，考虑到多个源表的数据合并到一个目标表，不应用该类型约束
//            for (KeyDef primaryKey : tableKeys.getPrimaryKeys()) {
//                String randomConstraintName = randomIndexName("PK");
//                String sql = String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" PRIMARY KEY (%s)",
//                        targetTableName.toUpperCase(),
//                        randomConstraintName,
//                        String.join(", ", primaryKey.getColumns()));
//                stmt.execute(sql);
//                System.out.println("已创建主键约束: " + randomConstraintName);
//            }

            // 创建唯一约束，考虑到多个源表的数据合并到一个目标表，不应用该类型约束
//            for (KeyDef uniqueConstraint : tableKeys.getUniqueConstraints()) {
//                String randomConstraintName = randomIndexName("UK");
//                String sql = String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" UNIQUE (%s)",
//                        targetTableName.toUpperCase(),
//                        randomConstraintName,
//                        String.join(", ", uniqueConstraint.getColumns()));
//                stmt.execute(sql);
//                System.out.println("已创建唯一约束: " + randomConstraintName);
//            }

            // 创建唯一索引，考虑到多个源表的数据合并到一个目标表，不应用该类型索引
//            for (KeyDef uniqueIndex : tableKeys.getUniqueIndexes()) {
//                String randomIndexName = randomIndexName("UDX");
//                String sql = String.format("CREATE UNIQUE INDEX \"%s\" ON \"%s\" (%s)",
//                        randomIndexName,
//                        targetTableName.toUpperCase(),
//                        String.join(", ", uniqueIndex.getColumns()));
//                stmt.execute(sql);
//                System.out.println("已创建唯一索引: " + randomIndexName);
//            }

            // 创建普通索引
            for (KeyDef normalIndex : tableKeys.getNormalIndexes()) {
                String randomIndexName = randomIndexName("IDX");
                String sql = String.format("CREATE INDEX \"%s\" ON \"%s\" (%s)",
                        randomIndexName,
                        targetTableName.toUpperCase(),
                        String.join(", ", normalIndex.getColumns()));
                stmt.execute(sql);
                System.out.println("已创建普通索引: " + randomIndexName);
            }

        } catch (Exception e) {
            System.err.println("创建表索引和约束失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void ensureTableHasUniqueKey(String targetTableName) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection()) {
            // 1. 读取目标表的键和约束
            String schemaName = this.dataSourceHolder.getTargetDataSourceConfig().getDataSourceUserName();
            TableKeys tableKeys = OracleKeyInspector.fetchKeys(
                    conn,
                    schemaName,
                    targetTableName,
                    true,
                    false
            );

            // 2. 检查是否已有主键或唯一约束
            boolean hasUniqueKey = false;
            if (tableKeys != null) {
                hasUniqueKey = !tableKeys.getPrimaryKeys().isEmpty() ||
                        !tableKeys.getUniqueConstraints().isEmpty() ||
                        !tableKeys.getUniqueIndexes().isEmpty();
            }

            // 3. 如果没有唯一键，使用CDC_ROWID作为主键
            if (!hasUniqueKey) {
                String constraintName = randomIndexName("PK");
                String sql = String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" PRIMARY KEY (\"CDC_ROWID\")",
                        targetTableName.toUpperCase(),
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



    @Override
    public void alterColumnLength(String tableName, String columnName, int newLength) {
        String sql = String.format("ALTER TABLE \"%s\" MODIFY \"%s\" VARCHAR2(%d)",
                tableName.toUpperCase(), columnName.toUpperCase(), newLength);
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            logger.info("成功更新字段长度: {}.{} -> {}", tableName, columnName, newLength);
        } catch (SQLException e) {
            logger.error("更新字段长度失败: {}", e.getMessage(), e);
            throw new RuntimeException("更新字段长度失败", e);
        }
    }




    @Override
    public void handleAddColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        String sql = String.format("ALTER TABLE %s ADD %s %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType());

        executeQuery(sql);
        logger.info("✅ 成功新增字段: 表={}, 字段={}, 类型={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType());
    }

    @Override
    public void handleModifyColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);

        String sql = String.format("ALTER TABLE %s MODIFY %s %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType());

        executeQuery(sql);
        logger.info("✅ 成功修改字段: 表={}, 字段={}, 新类型={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getColumnName(),
                columnChangeInfo.getColumnType());
    }

    @Override
    public void handleDropColumn(ColumnChangeInfo columnChangeInfo) throws Exception {
        validateColumnChangeInfo(columnChangeInfo);


        String tableName = columnChangeInfo.getTableName();
        String columnName = columnChangeInfo.getColumnName();
        String newColumnName = "cdc_d_" + columnName;

        try {
            // 1. 先去掉NOT NULL约束（如果存在）
            String modifyNullableSql = String.format("ALTER TABLE %s MODIFY (%s NULL)",
                    tableName, columnName);
            executeQuery(modifyNullableSql);
            logger.debug("🔧 去掉字段必填约束: 表={}, 字段={}", tableName, columnName);

            // 2. 重命名字段，添加删除前缀
            String renameSql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                    tableName, columnName, newColumnName);
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

        String sql = String.format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getOldColumnName(),
                columnChangeInfo.getColumnName());

        executeQuery(sql);
        logger.info("✅ 成功重命名字段: 表={}, 旧字段名={}, 新字段名={}",
                columnChangeInfo.getTableName(),
                columnChangeInfo.getOldColumnName(),
                columnChangeInfo.getColumnName());
    }

    /**
     * 验证ColumnChangeInfo对象
     *
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

