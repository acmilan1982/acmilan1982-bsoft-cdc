package com.bsoft.server.sql.select;

import com.bsoft.common.bean.ColumnChangeInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcSqlConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
public class OracleSelectStrategy  extends AbstractSelectStrategy {

    private static final Logger logger = LogManager.getLogger(OracleSelectStrategy.class);


    public OracleSelectStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }

    @Override
    public String buildCheckTableExistsSql() {
        return CdcSqlConstants.ORACLE_CHECK_TABLE_EXISTS;
    }



    @Override
    public void setParameters(PreparedStatement ps , String tableName) throws SQLException {
        ps.setString(1, tableName.toUpperCase());
    }


    // 获取指定表的所有列信息
    @Override
    public Map<String, ColumnChangeInfo> getTableColumns(String tableName) {
        Map<String, ColumnChangeInfo> columns = new HashMap<>();
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, tableName.toUpperCase());
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int length = rs.getInt("DATA_LENGTH");

                    ColumnChangeInfo info = new ColumnChangeInfo(tableName, columnName, dataType, length);
                    columns.put(columnName, info);
                }
            }
        } catch (SQLException e) {
            logger.error("读取表列信息失败: {}", e.getMessage(), e);
        }
        return columns;
    }

}
