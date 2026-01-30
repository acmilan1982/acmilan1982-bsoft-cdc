package com.bsoft.server.sql.factory;

/**
 * ClassName: DeleteStrategyFactory
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 20:23
 */


import com.bsoft.common.bean.DatabaseInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.server.sql.delete.DeleteStrategy;
import com.bsoft.server.sql.delete.DorisDeleteStrategy;
import com.bsoft.server.sql.delete.MySQLDeleteStrategy;
import com.bsoft.server.sql.delete.OracleDeleteStrategy;
import com.bsoft.server.sql.insert.DorisInsertStrategy;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.insert.MySQLInsertStrategy;
import com.bsoft.server.sql.insert.OracleInsertStrategy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 删除策略工厂类
 */
public class DeleteStrategyFactory {

    private static final Map<String, DeleteStrategy> STRATEGY_CACHE = new ConcurrentHashMap<>();


    // 根据数据库信息获取对应的插入策略
    public static DeleteStrategy getStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        return STRATEGY_CACHE.computeIfAbsent(targetDataSourceConfig.getDataSourceId(), info -> {
            DeleteStrategy strategy = createStrategy(targetDataSourceConfig);
            return strategy;
        });
    }

    /**
     * 创建策略实例
     */
    private static DeleteStrategy createStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        switch (targetDataSourceConfig.getDatabaseInfo().getType()) {
            case MYSQL:
                return new MySQLDeleteStrategy(targetDataSourceConfig);
            case ORACLE:
                return new OracleDeleteStrategy(targetDataSourceConfig);
            case DORIS:
                return new DorisDeleteStrategy(targetDataSourceConfig);
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