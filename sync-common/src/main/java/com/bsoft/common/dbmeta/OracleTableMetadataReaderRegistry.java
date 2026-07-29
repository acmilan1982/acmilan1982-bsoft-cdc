package com.bsoft.common.dbmeta;

import com.bsoft.common.bean.*;
import com.bsoft.common.util.OracleKeyInspector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName: OracleTableMetadataReader
 * Package:   com.bsoft.client.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:30
 */
public class OracleTableMetadataReaderRegistry {

    private static final Logger logger = LogManager.getLogger(OracleTableMetadataReaderRegistry.class);

    // 以DataSourceId为key,
    private Map<String, OracleTableMetadataReader> readerMapByDataSourceId = new HashMap<>();; // 以ID为key的快速查找

    private List<OracleTableMetadataReader> oracleTableMetadataReaderList = new ArrayList<>();


    public OracleTableMetadataReaderRegistry() {
    }

    public OracleTableMetadataReaderRegistry(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry) {
        for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {

            OracleTableMetadataReader oracleTableMetadataReader = new OracleTableMetadataReader(businessDataSourceConfig);
            oracleTableMetadataReaderList.add(oracleTableMetadataReader);
            readerMapByDataSourceId.put(businessDataSourceConfig.getDataSourceId(), oracleTableMetadataReader);

        }
    }

    public Map<String, OracleTableMetadataReader> getReaderMapByDataSourceId() {
        return readerMapByDataSourceId;
    }

    public void setReaderMapByDataSourceId(Map<String, OracleTableMetadataReader> readerMapByDataSourceId) {
        this.readerMapByDataSourceId = readerMapByDataSourceId;
    }

    public List<OracleTableMetadataReader> getOracleTableMetadataReaderList() {
        return oracleTableMetadataReaderList;
    }

    public void setOracleTableMetadataReaderList(List<OracleTableMetadataReader> oracleTableMetadataReaderList) {
        this.oracleTableMetadataReaderList = oracleTableMetadataReaderList;
    }

    public OracleTableMetadataReader getOracleTableMetadataReader(String dataSourceId) {
        return this.readerMapByDataSourceId.get(dataSourceId);
    }
}
