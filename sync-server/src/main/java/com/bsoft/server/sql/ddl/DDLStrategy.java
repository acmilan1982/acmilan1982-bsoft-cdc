package com.bsoft.server.sql.ddl;

import com.bsoft.common.bean.ColumnChangeInfo;
import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.DorisColumnInfo;
import com.bsoft.common.bean.TableKeys;

import java.util.List;

/**
 * ClassName: DdlStrategy
 * Package:   com.bsoft.server.sql.ddl
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/17 - 12:49
 */
public interface DDLStrategy {
    // 将Oracle的建表语句转换为当前数据库的建表语句
    String convertOracleCreateTableToCurrentDB(String tableName, String oracleDDL, TableKeys tableKeys);

    // 向目标表中添加额外的列（如有需要）
    void addExtraColumnsToTable(String tableName);

    // 应用源表的主键等约束
    void createTableIndexesAndConstraints(String targetTableName, TableKeys tableKeys);

    // 如果源表没有主键或唯一键，则为目标表添加一个主键字段:CDC_ROWID，否则无法跟新或删除
    void ensureTableHasUniqueKey(String tableName) ;

    /**
     * 获取表字段信息(泛型方法)
     * @param tableName 表名
     * @param <T> 字段信息类型(必须继承自ColumnInfo)
     * @return 字段信息列表
     */
    <T extends ColumnInfo> List<T> getTableColumns(String tableName) throws Exception;


    // 修改字段长度
    void alterColumnLength(String tableName, String columnName, int newLength);

    // 新增字段
    void handleAddColumn(ColumnChangeInfo columnChangeInfo) throws Exception;
    // 修改字段
    void handleModifyColumn(ColumnChangeInfo columnChangeInfo) throws Exception;
    // 删除字段
    void handleDropColumn(ColumnChangeInfo columnChangeInfo) throws Exception;
    // 重命名字段
    void handleRenameColumn(ColumnChangeInfo columnChangeInfo) throws Exception;

    void executeCreateTable(String createTableSql);
}