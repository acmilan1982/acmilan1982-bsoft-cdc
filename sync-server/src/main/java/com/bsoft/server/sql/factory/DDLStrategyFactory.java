package com.bsoft.server.sql.factory;

/**
 * ClassName: DeleteStrategyFactory
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:23
 */


import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.sql.ddl.DDLStrategy;
import com.bsoft.server.sql.ddl.DorisDDLStrategy;
import com.bsoft.server.sql.ddl.MySQLDDLStrategy;
import com.bsoft.server.sql.ddl.OracleDDLStrategy;
import com.bsoft.server.sql.delete.DeleteStrategy;
import com.bsoft.server.sql.delete.DorisDeleteStrategy;
import com.bsoft.server.sql.delete.MySQLDeleteStrategy;
import com.bsoft.server.sql.delete.OracleDeleteStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 删除策略工厂类
 */
public class DDLStrategyFactory {

    private static final Map<String, DDLStrategy> STRATEGY_CACHE = new ConcurrentHashMap<>();


    // 根据数据库信息获取对应的插入策略
    public static DDLStrategy getStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        return STRATEGY_CACHE.computeIfAbsent(targetDataSourceConfig.getDataSourceId(), info -> {
            DDLStrategy strategy = createStrategy(targetDataSourceConfig);
            return strategy;
        });
    }

    /**
     * 创建策略实例
     */
    private static DDLStrategy createStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        switch (targetDataSourceConfig.getDatabaseInfo().getType()) {
            case MYSQL:
                return new MySQLDDLStrategy(targetDataSourceConfig);
            case ORACLE:
                return new OracleDDLStrategy(targetDataSourceConfig);
            case DORIS:
                return new DorisDDLStrategy(targetDataSourceConfig);
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