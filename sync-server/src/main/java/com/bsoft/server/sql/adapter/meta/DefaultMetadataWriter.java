package com.bsoft.server.sql.adapter.meta;

/**
 * ClassName: DefaultMetadataWriter
 * Package:   com.bsoft.server.sql.adapter
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/31 - 15:13
 */
import com.bsoft.common.bean.TableKeys;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.ddl.DDLStrategy;
import com.bsoft.server.sql.factory.DDLStrategyFactory;

public class DefaultMetadataWriter implements MetadataWriter {

    private final DDLStrategy ddlStrategy;

    public DefaultMetadataWriter(TargetDataSourceConfig targetDataSourceConfig) {
        this.ddlStrategy = DDLStrategyFactory.getStrategy(targetDataSourceConfig);
    }

    @Override
    public void executeCreateTable(String createTableSql) {
        this.ddlStrategy.executeCreateTable(createTableSql);
    }

    @Override
    public String convertOracleCreateTableToCurrentDB(String tableName, String oracleDDL, TableKeys tableKeys) {
        return this.ddlStrategy.convertOracleCreateTableToCurrentDB(tableName, oracleDDL, tableKeys);
    }

    @Override
    public void addExtraColumnsToTable(String tableName) {
        this.ddlStrategy.addExtraColumnsToTable(tableName);
    }

    @Override
    public void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys) {
        this.ddlStrategy.createTableIndexesAndConstraints(targetTableName, tableKeys);
    }

    @Override
    public void ensureTableHasUniqueKey(String tableName) {
        this.ddlStrategy.ensureTableHasUniqueKey(tableName);
    }

    @Override
    public void alterColumnLength(String tableName, String columnName, int newLength) {
        this.ddlStrategy.alterColumnLength(tableName,columnName, newLength);
    }
}

