package com.bsoft.server.sql.factory;

/**
 * ClassName: InsertStrategyFactory
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.DatabaseInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.insert.DorisInsertStrategy;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.insert.MySQLInsertStrategy;
import com.bsoft.server.sql.insert.OracleInsertStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 插入策略工厂类
 * 负责创建和缓存不同数据库的插入策略实例
 */
public class InsertStrategyFactory {

    private static final Map<String, InsertStrategy> STRATEGY_CACHE = new ConcurrentHashMap<>();

//    static {
//        // 预创建策略实例
//        STRATEGY_CACHE.put(DatabaseType.MYSQL, new MySQLInsertStrategy());
//        STRATEGY_CACHE.put(DatabaseType.ORACLE, new OracleInsertStrategy());
//        STRATEGY_CACHE.put(DatabaseType.DORIS, new DorisInsertStrategy());
//    }

    // 根据数据库信息获取对应的插入策略
    public static InsertStrategy getStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        return STRATEGY_CACHE.computeIfAbsent(targetDataSourceConfig.getDataSourceId(), info -> {
            InsertStrategy strategy = createStrategy(targetDataSourceConfig);
            return strategy;
        });
    }

    /**
     * 创建策略实例
     */
    private static InsertStrategy createStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        switch (targetDataSourceConfig.getDatabaseInfo().getType()) {
            case MYSQL:
                return new MySQLInsertStrategy(targetDataSourceConfig);
            case ORACLE:
                return new OracleInsertStrategy(targetDataSourceConfig);
            case DORIS:
                return new DorisInsertStrategy(targetDataSourceConfig);
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