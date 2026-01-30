package com.bsoft.server.sql.adapter;

import com.bsoft.common.bean.TargetDataSourceConfig;

import java.util.Set;

/**
 * ClassName: AbstractInsertStrategy
 * Package:   com.bsoft.server.sql.insert
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/29 - 18:22
 */
public class AbstractDatabaseStrategy     {

    protected  DataSourceHolder dataSourceHolder;


    public AbstractDatabaseStrategy() {
    }

    protected AbstractDatabaseStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }

    public void appendCdcTimeFields(Set<String> dateFieldSet) {
        dateFieldSet.add("cdc_dml_Time");
        dateFieldSet.add("cdc_kafka_enqueue_time");
        dateFieldSet.add("cdc_insert_time");
    }
}
