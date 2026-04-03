package com.bsoft.server.sql.adapter.meta;

import com.bsoft.common.bean.DDLBuildContext;
import com.bsoft.common.bean.DorisBizAttr;
import com.bsoft.common.bean.TableKeys;

/**
 * ClassName: MetadataReader
 * Package:   com.bsoft.server.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/31 - 10:13
 */
public interface MetadataWriter {

    // 将Oracle的建表语句转换为当前数据库的建表语句
    String convertOracleCreateTableToCurrentDB(
            String tableName, String oracleDDL, TableKeys tableKeys, DDLBuildContext context);

    // 执行建表语句
    void executeCreateTable(String createTableSql);

    // 向目标表中添加额外的列（如有需要）
    void addExtraColumnsToTable(String tableName);

    // 应用源表的主键等约束
    void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys);

    // 如果源表没有主键或唯一键，则为目标表添加一个主键字段:CDC_ROWID，否则无法跟新或删除
    void ensureTableHasUniqueKey(String tableName) ;

    // 修改字段长度
    void alterColumnLength(String tableName, String columnName, int newLength);




}
