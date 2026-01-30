package com.bsoft.server.sql.factory;

/**
 * ClassName: UpdateStrategyFactory
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:10
 */


import com.bsoft.common.bean.DatabaseInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.server.sql.insert.DorisInsertStrategy;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.insert.MySQLInsertStrategy;
import com.bsoft.server.sql.insert.OracleInsertStrategy;
import com.bsoft.server.sql.update.DorisUpdateStrategy;
import com.bsoft.server.sql.update.MySQLUpdateStrategy;
import com.bsoft.server.sql.update.OracleUpdateStrategy;
import com.bsoft.server.sql.update.UpdateStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 更新策略工厂类
 */
public class UpdateStrategyFactory {

    private static final Map<String, UpdateStrategy> STRATEGY_CACHE = new ConcurrentHashMap<>();

    public static UpdateStrategy getStrategy(DatabaseType databaseType) {
        switch (databaseType) {
            case MYSQL:
                return new MySQLUpdateStrategy();
            case ORACLE:
                return new OracleUpdateStrategy();
            case DORIS:
                return new DorisUpdateStrategy();
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + databaseType);
        }
    }


    // 根据数据库信息获取对应的插入策略
    public static UpdateStrategy getStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        return STRATEGY_CACHE.computeIfAbsent(targetDataSourceConfig.getDataSourceId(), info -> {
            UpdateStrategy strategy = createStrategy(targetDataSourceConfig);
            return strategy;
        });
    }

    /**
     * 创建策略实例
     */
    private static UpdateStrategy createStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        switch (targetDataSourceConfig.getDatabaseInfo().getType()) {
            case MYSQL:
                return new MySQLUpdateStrategy(targetDataSourceConfig);
            case ORACLE:
                return new OracleUpdateStrategy(targetDataSourceConfig);
            case DORIS:
                return new DorisUpdateStrategy(targetDataSourceConfig);
            default:
                throw new IllegalArgumentException("不支持的数据库类型: " + targetDataSourceConfig.getDatabaseInfo().getType());
        }
    }
    /**
     * 清除缓存
     */
    public static void clearCache() {
        STRATEGY_CACHE.clear();
    }
}