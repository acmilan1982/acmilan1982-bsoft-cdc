package com.bsoft.server.sql.select;

import com.bsoft.common.bean.ColumnChangeInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.util.CdcSqlConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Doris 单条插入策略实现
 * - 仅构建单条 INSERT INTO ... VALUES(...) 语句
 * - 不实现 "存在则不插入"；对 UNIQUE/PRIMARY KEY 表，Doris 将表现为 UPSERT（存在即覆盖）
 */
public class MySQLSelectStrategy extends AbstractSelectStrategy  {



    public MySQLSelectStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }

    @Override
    public String buildCheckTableExistsSql() {
        return CdcSqlConstants.MYSQL_CHECK_TABLE_EXISTS;
    }



    @Override
    public void setParameters(PreparedStatement ps, String tableName) throws SQLException {
        ps.setString(1, tableName.toLowerCase());
    }

    // 获取指定表的所有列信息
    @Override
    public Map<String, ColumnChangeInfo> getTableColumns(String tableName) {
        Map<String, ColumnChangeInfo> columns = new HashMap<>();

        //todo:待实现
        return columns;
    }
}
