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
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;

/**
 * Doris 单条插入策略实现
 * - 仅构建单条 INSERT INTO ... VALUES(...) 语句
 * - 不实现 "存在则不插入"；对 UNIQUE/PRIMARY KEY 表，Doris 将表现为 UPSERT（存在即覆盖）
 */
public class DorisSelectStrategy extends AbstractSelectStrategy  {

    private static final Logger logger = LogManager.getLogger(DorisSelectStrategy.class);


    public DorisSelectStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
    }

    @Override
    public String buildCheckTableExistsSql() {
        return CdcSqlConstants.DORIS_CHECK_TABLE_EXISTS;
    }



    @Override
    public void setParameters(PreparedStatement ps, String tableName) throws SQLException {
        ps.setString(1, this.dataSourceHolder.getTargetDataSourceConfig().getDataSourceServiceName());
        ps.setString(2, tableName.toLowerCase());
    }

    // 获取指定表的所有列信息
    @Override
    public Map<String, ColumnChangeInfo> getTableColumns(String tableName) {
        Map<String, ColumnChangeInfo> columns = new HashMap<>();

        //todo:待实现
        return columns;
    }


    // 获取指定表的 唯一键
    @Override
    public LinkedHashSet<String> getTargetUniqueKeys(String tableName) throws Exception {
        String sql = "SHOW COLUMNS FROM " + sanitizeIdentifier(tableName);
        LinkedHashSet<String> uniqueKeys = new LinkedHashSet<>();

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String field = rs.getString("Field");
                String key = rs.getString("Key");

                if (field == null || field.trim().isEmpty()) {
                    continue;
                }

                if ("YES".equalsIgnoreCase(key)) {
                    uniqueKeys.add(field.trim().toLowerCase(Locale.ROOT));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to resolve Doris UNIQUE KEY via SHOW COLUMNS, table={}", tableName, e);
            throw e;
        }

        if (uniqueKeys.isEmpty()) {
            throw new IllegalStateException(
                    "No UNIQUE KEY columns found via SHOW COLUMNS, table=" + tableName
            );
        }

        logger.info("Resolved Doris UNIQUE KEY via SHOW COLUMNS, table={}, keys={}", tableName, uniqueKeys);
        return uniqueKeys;
    }

    private String sanitizeIdentifier(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException("tableName is blank");
        }
        return raw.replace("`", "").trim();
    }
}
