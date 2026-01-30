package com.bsoft.server.sql.adapter.meta;

/**
 * ClassName: DefaultMetadataReader
 * Package:   com.bsoft.server.sql.adapter
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/31 - 15:14
 */

import com.bsoft.common.bean.ColumnChangeInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.factory.SelectStrategyFactory;
import com.bsoft.server.sql.select.SelectStrategy;

import java.util.Map;

public class DefaultMetadataReader implements MetadataReader {

    private final SelectStrategy selectStrategy;

    public DefaultMetadataReader(TargetDataSourceConfig targetDataSourceConfig) {
        this.selectStrategy = SelectStrategyFactory.getStrategy(targetDataSourceConfig);
    }

    @Override
    public boolean isTableExists(String targetTableName) throws Exception {
        return this.selectStrategy.checkTableExists(targetTableName);
    }

    @Override
    public Map<String, ColumnChangeInfo> getTableColumns(String tableName) {
        return this.selectStrategy.getTableColumns(tableName);
    }
}

