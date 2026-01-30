package com.bsoft.server.sql.adapter.meta;

import com.bsoft.common.bean.ColumnChangeInfo;

import java.util.Map;

/**
 * ClassName: MetadataReader
 * Package:   com.bsoft.server.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/31 - 10:13
 */
public interface MetadataReader {

    // 判断当前库中是否存在指定表
    boolean isTableExists(String targetTableName) throws Exception;

    // 获取指定表的所有列信息
    Map<String, ColumnChangeInfo> getTableColumns(String tableName);


}
