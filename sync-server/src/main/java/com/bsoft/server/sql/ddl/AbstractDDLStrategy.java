package com.bsoft.server.sql.ddl;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.DorisColumnInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClassName: DdlStrategy
 * Package:   com.bsoft.server.sql.ddl
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/17 - 12:49
 */
public abstract class AbstractDDLStrategy implements DDLStrategy{
    private static final Logger logger = LogManager.getLogger(AbstractDDLStrategy.class);

    protected DataSourceHolder dataSourceHolder;


    public AbstractDDLStrategy() {
    }

    protected AbstractDDLStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }


    // 读取目标表字段元数据，供 Oracle、MySQL 的字段过滤和补齐使用。
// Doris 已重写该方法，继续使用自己的字段读取实现。
    @Override
    @SuppressWarnings("unchecked")
    public <T extends ColumnInfo> List<T> getTableColumns(String tableName)
            throws Exception {
        // 校验目标表名。
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("读取字段元数据的目标表名不能为空");
        }

        // 检查数据源初始化状态。
        if (dataSourceHolder == null || dataSourceHolder.getDataSource() == null) {
            throw new IllegalStateException("读取目标字段元数据的数据源尚未初始化");
        }

        // 沿用现有策略的目标表名使用方式。
        // 恒假条件保证结果集不包含业务记录。
        String sql = "SELECT * FROM " + tableName + " WHERE 1 = 0";

        // 仅执行查询，不创建或修改表结构。
        try (Connection connection = dataSourceHolder.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            // 从空结果集读取 JDBC 字段描述。
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();
            if (columnCount <= 0) {
                throw new SQLException(
                        "未读取到目标表字段元数据，table=" + tableName);
            }

            // 按目标表字段顺序构造列表。
            List<T> columns = new ArrayList<>(columnCount);
            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                // 保留数据库返回的实际字段名称及大小写。
                String columnName = metadata.getColumnName(columnIndex);
                if (columnName == null || columnName.trim().isEmpty()) {
                    throw new SQLException(
                            "目标字段元数据包含空字段名，table="
                                    + tableName + "，columnIndex=" + columnIndex);
                }

                // 当前 JDBC 更新使用字段名进行过滤和补齐。
                // 类型保存驱动返回的类型名称，不用于重建完整 DDL。
                ColumnInfo column = new ColumnInfo();
                column.setColumnName(columnName);
                column.setColumnType(metadata.getColumnTypeName(columnIndex));

                // 沿用现有泛型接口，当前 writer 以 List<ColumnInfo> 接收。
                // 本基类返回普通 ColumnInfo，不提供数据库专属子类对象。
                columns.add((T) column);
            }

            // 读取失败时异常向上传播，不用空列表掩盖失败。
            return columns;
        }
    }


    /**
     * 执行DDL语句
     * @param sql DDL语句
     * @throws Exception 执行异常
     */
    protected void executeQuery(String sql) throws Exception {
        try (Connection connection = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {

            logger.debug("🔧 执行DDL语句: {}", sql);
            statement.executeUpdate();

        } catch (SQLException e) {
            logger.error("❌ DDL语句执行失败: {}", sql, e);
//            throw new Exception("执行DDL语句失败: " + sql, e);
        }
    }

    // 执行建表语句
    @Override
    public void executeCreateTable(String createTableSql) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            String cleanedSql = preprocessCreateTableSql(createTableSql);
            stmt.execute(cleanedSql);
        } catch (Exception e) {
            throw new RuntimeException("执行建表语句失败: " + createTableSql, e);
        }
    }

    /**
     * 预处理建表SQL(子类可覆盖)
     */
    protected String preprocessCreateTableSql(String sql) {
        return sql;
    }


    @Override
    public void alterColumnLength(String tableName, String columnName, int newLength) {
    }


    /**
     * 获取添加字段的 SQL 语句（各数据库语法不同）
     */
    protected abstract String[] buildAddExtraColumnsSql(String tableName);

    /**
     * 判断是否为"字段已存在"异常（各数据库错误码不同）
     */
    protected abstract boolean isDuplicateColumnException(SQLException e);

    /**
     * 向目标表添加额外的同步字段（模板方法）
     */
    @Override
    public void addExtraColumnsToTable(String tableName) {
        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             Statement stmt = conn.createStatement()) {

            String[] alterSqls = buildAddExtraColumnsSql(tableName);

            for (String alterSql : alterSqls) {
                try {
                    stmt.execute(alterSql);
                } catch (SQLException e) {
                    if (isDuplicateColumnException(e)) {
                        logger.debug("字段已存在，跳过: {}", alterSql);
                    } else {
                        throw e;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("向表 " + tableName + " 添加同步字段失败: " + e.getMessage(), e);
        }
    }


//    @Override
//    public  <T extends ColumnInfo> List<T> getTableColumns(String tableName) throws Exception{
//        return Collections.emptyList();
//    }



}