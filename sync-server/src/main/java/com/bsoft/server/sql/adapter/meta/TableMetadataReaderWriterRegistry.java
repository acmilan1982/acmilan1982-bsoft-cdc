// BusinessDataSourceConfig.java
package com.bsoft.server.sql.adapter.meta;

import com.bsoft.common.bean.TargetDataSourceConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author snoopy
 */
public class TableMetadataReaderWriterRegistry implements Serializable {

    private Map<String, TableMetadataReaderWriterPair> tableMetadataReaderWriterPairRegistry = new HashMap();



    public TableMetadataReaderWriterRegistry() {
    }

    public void register(TargetDataSourceConfig targetDataSourceConfig) {
        tableMetadataReaderWriterPairRegistry.put(targetDataSourceConfig.getDataSourceId(),new TableMetadataReaderWriterPair(targetDataSourceConfig));
    }

    public TableMetadataReaderWriterPair getTableMetadataReaderWriterPair(TargetDataSourceConfig targetDataSourceConfig) {
        return tableMetadataReaderWriterPairRegistry.get(targetDataSourceConfig.getDataSourceId());
    }


}