package com.bsoft.server.sql.factory;

/**
 * ClassName: InsertStrategyFactory
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.select.DorisSelectStrategy;
import com.bsoft.server.sql.select.MySQLSelectStrategy;
import com.bsoft.server.sql.select.OracleSelectStrategy;
import com.bsoft.server.sql.select.SelectStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询策略工厂类
 * 负责创建和缓存不同数据库的查询策略实例
 */
public class SelectStrategyFactory {

    private static final Map<String, SelectStrategy> STRATEGY_CACHE = new ConcurrentHashMap<>();



    // 根据数据库信息获取对应的查询策略
    public static SelectStrategy getStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        return STRATEGY_CACHE.computeIfAbsent(targetDataSourceConfig.getDataSourceId(), info -> {
            SelectStrategy strategy = createStrategy(targetDataSourceConfig);
            return strategy;
        });
    }

    /**
     * 创建策略实例
     */
    private static SelectStrategy createStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        switch (targetDataSourceConfig.getDatabaseInfo().getType()) {
            case MYSQL:
                return new MySQLSelectStrategy(targetDataSourceConfig);
            case ORACLE:
                return new OracleSelectStrategy(targetDataSourceConfig);
            case DORIS:
                return new DorisSelectStrategy(targetDataSourceConfig);
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