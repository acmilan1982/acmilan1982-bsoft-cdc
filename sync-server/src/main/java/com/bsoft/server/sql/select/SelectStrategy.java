package com.bsoft.server.sql.select;

/**
 * ClassName: InsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:23
 */

import com.bsoft.common.bean.ColumnChangeInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据库插入策略接口
 * 用于处理不同数据库的插入操作差异
 */
public interface SelectStrategy {


    String buildCheckTableExistsSql();


    void setParameters(PreparedStatement ps, String tableName) throws SQLException;


    boolean checkTableExists(String tableName) throws Exception;


    Map<String, ColumnChangeInfo> getTableColumns(String tableName);
}
