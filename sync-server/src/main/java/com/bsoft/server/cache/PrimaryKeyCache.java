package com.bsoft.server.cache;

/**
 * ClassName: PrimaryKeyCache
 * Package:   com.bsoft.cache
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:25
 */


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * 主键信息缓存管理器
 * 缓存表的主键信息，避免重复查询数据库元数据
 */
public class PrimaryKeyCache {

    private static final Logger LOG = Logger.getLogger(PrimaryKeyCache.class.getName());

    private final ConcurrentMap<String, Set<String>> primaryKeyCache = new ConcurrentHashMap<>();

    /**
     * 获取表的主键字段集合
     * @param connection 数据库连接
     * @param tableName 表名
     * @return 主键字段集合
     */
    public Set<String> getPrimaryKeys(Connection connection, String tableName) {
        // 先从缓存中获取
        String cacheKey = generateCacheKey(connection, tableName);
        Set<String> cachedKeys = primaryKeyCache.get(cacheKey);
        if (cachedKeys != null) {
            return new HashSet<>(cachedKeys);
        }

        // 缓存中没有，查询数据库
        Set<String> primaryKeys = queryPrimaryKeysFromDatabase(connection, tableName);

        // 放入缓存
        primaryKeyCache.put(cacheKey, new HashSet<>(primaryKeys));

        return primaryKeys;
    }

    /**
     * 从数据库查询主键信息
     */
    private Set<String> queryPrimaryKeysFromDatabase(Connection connection, String tableName) {
        Set<String> primaryKeys = new HashSet<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            String catalog = connection.getCatalog();
            String schema = connection.getSchema();

            try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName.toUpperCase())) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    primaryKeys.add(columnName.toLowerCase());
                }
            }

            // 如果没有找到主键，尝试使用小写表名查询（针对某些数据库）
            if (primaryKeys.isEmpty()) {
                try (ResultSet rs = metaData.getPrimaryKeys(catalog, schema, tableName.toLowerCase())) {
                    while (rs.next()) {
                        String columnName = rs.getString("COLUMN_NAME");
                        primaryKeys.add(columnName.toLowerCase());
                    }
                }
            }

            LOG.info("查询到表 " + tableName + " 的主键字段: " + primaryKeys);

        } catch (SQLException e) {
            LOG.warning("查询表 " + tableName + " 的主键信息失败: " + e.getMessage());
        }

        return primaryKeys;
    }

    /**
     * 生成缓存键
     */
    private String generateCacheKey(Connection connection, String tableName) {
        try {
            String url = connection.getMetaData().getURL();
            return url + ":" + tableName.toLowerCase();
        } catch (SQLException e) {
            // 如果获取URL失败，使用表名作为键
            return tableName.toLowerCase();
        }
    }

    /**
     * 清理缓存
     */
    public void clearCache() {
        primaryKeyCache.clear();
    }

    /**
     * 移除特定表的缓存
     */
    public void removeCacheForTable(Connection connection, String tableName) {
        String cacheKey = generateCacheKey(connection, tableName);
        primaryKeyCache.remove(cacheKey);
    }
}