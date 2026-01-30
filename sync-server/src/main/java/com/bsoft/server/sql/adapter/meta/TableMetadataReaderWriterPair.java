package com.bsoft.server.sql.adapter.meta;



import com.bsoft.common.bean.TargetDataSourceConfig;
;

/**
 * ClassName: MySqlTableMetadataReaderWriterPari
 * Package:   com.bsoft.server.dbmeta.mysql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/4 - 9:59
 */
public class TableMetadataReaderWriterPair   {

    private MetadataReader metadataReader;
    private MetadataWriter metadataWriter;

    public TableMetadataReaderWriterPair(TargetDataSourceConfig targetDataSourceConfig) {
        this.metadataReader = new DefaultMetadataReader(targetDataSourceConfig);
        this.metadataWriter = new DefaultMetadataWriter(targetDataSourceConfig);;
    }


    public MetadataReader getMetadataReader() {
        return metadataReader;
    }

    public void setMetadataReader(MetadataReader metadataReader) {
        this.metadataReader = metadataReader;
    }

    public MetadataWriter getMetadataWriter() {
        return metadataWriter;
    }

    public void setMetadataWriter(MetadataWriter metadataWriter) {
        this.metadataWriter = metadataWriter;
    }
}
