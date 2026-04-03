package com.bsoft.server.sql.ddl;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.DorisColumnInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
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


    @Override
    public  <T extends ColumnInfo> List<T> getTableColumns(String tableName) throws Exception{
        return Collections.emptyList();
    }



}