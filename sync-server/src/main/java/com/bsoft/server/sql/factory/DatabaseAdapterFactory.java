package com.bsoft.server.sql.factory;

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.adapter.DorisAdapter;
import com.bsoft.server.sql.adapter.MySQLAdapter;
import com.bsoft.server.sql.adapter.OracleAdapter;

/**
 * ClassName: DatabaseAdapterFactory
 * Package:   com.bsoft.server.sql.factory
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/30 - 19:05
 */
public class DatabaseAdapterFactory {

    /**
     * 根据数据源配置创建适配器
     *
     * @param targetDataSourceConfig 目标数据源配置
     * @return 对应的数据库适配器
     */
    public static DatabaseAdapter createAdapter(TargetDataSourceConfig targetDataSourceConfig) {
        DatabaseType dataSourceType = targetDataSourceConfig.getDataSourceType();
        return createAdapter(dataSourceType);
    }

    /**
     * 根据数据库类型创建适配器
     *
     * @param type 数据库类型
     * @return 对应的数据库适配器
     */
    public static DatabaseAdapter createAdapter(DatabaseType type) {
        switch (type) {
            case MYSQL:
                return new MySQLAdapter();
            case ORACLE:
                return new OracleAdapter();
            case DORIS:
                return new DorisAdapter();
            default:
                throw new IllegalArgumentException("Unsupported database type: " + type);
        }
    }
}
