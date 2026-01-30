package com.bsoft.common.dataSource;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ClassName: DataSourceFactory
 * Package:   com.bsoft.common.factory
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/29 - 17:08
 */
public class DataSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    // 按 dataSourceId 缓存，同一个目标库共享连接池
    private static final Map<String, HikariDataSource> DATASOURCE_CACHE = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService monitorScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "hikari-pool-monitor");
                thread.setDaemon(true);
                return thread;
            });


    static {
        // 每30秒自动打印所有连接池的监控信息
        monitorScheduler.scheduleAtFixedRate(() -> {
            try {
                if (DATASOURCE_CACHE.isEmpty()) {
                    return;
                }

                logger.info("========== HikariCP 连接池监控 ==========");
                DATASOURCE_CACHE.forEach((key, dataSource) -> {
                    try {
                        HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
                        logger.info("连接池[{}]: 活动={} 空闲={} 总数={} 最大={} 等待线程={}",
                                key,
                                pool.getActiveConnections(),
                                pool.getIdleConnections(),
                                pool.getTotalConnections(),
                                dataSource.getMaximumPoolSize(),
                                pool.getThreadsAwaitingConnection()
                        );
                    } catch (Exception e) {
                        logger.warn("获取连接池[{}]监控信息失败: {}", key, e.getMessage());
                    }
                });
                logger.info("==========================================");
            } catch (Exception e) {
                logger.error("连接池监控任务执行失败", e);
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public static DataSource getDataSource(TargetDataSourceConfig config) {
        return DATASOURCE_CACHE.computeIfAbsent(
                config.getDataSourceId(),
                info -> createDataSource(config)
        );
    }

    public static DataSource getDataSource(ConfigDataSourceConfig configDataSourceConfig) {
        return DATASOURCE_CACHE.computeIfAbsent(
                configDataSourceConfig.getInstanceId(),
                info -> createDataSource(configDataSourceConfig)
        );
    }

    private static HikariDataSource createDataSource(TargetDataSourceConfig targetDataSourceConfig) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(targetDataSourceConfig.getUrl());
        hikariConfig.setUsername(targetDataSourceConfig.getDataSourceUserName());
        hikariConfig.setPassword(targetDataSourceConfig.getDataSourcePassword());
        hikariConfig.setMaximumPoolSize(20);      // 连接池最大连接数(包括空闲+活动)
        hikariConfig.setMinimumIdle(10);           // 池中保持的最小空闲连接数
        hikariConfig.setConnectionTimeout(10000);    // 客户端从连接池获取连接的最大等待时间(毫秒)
        hikariConfig.setIdleTimeout(60000);       // 空闲连接在池中的最大存活时间(毫秒)
        // ✅ 添加 UUID 以区分连接池实例
        String poolName = "target-db-pool-" + "-" + UUID.randomUUID().toString().substring(0, 8);
        hikariConfig.setPoolName(poolName);
        hikariConfig.setRegisterMbeans(true);  // ✅ 启用 MBean
        hikariConfig.setLeakDetectionThreshold(60000); // 连接被持有超过此时间,触发泄漏警告(毫秒)

        // ✅ 连接测试（确保连接有效）
        hikariConfig.setConnectionTestQuery("SELECT 1 FROM DUAL");  // Oracle

        // ✅ 性能优化
        hikariConfig.addDataSourceProperty("cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return  new HikariDataSource(hikariConfig);
    }


    private static HikariDataSource createDataSource(ConfigDataSourceConfig configDataSourceConfig) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName(configDataSourceConfig.getDriverClassName());
        hikariConfig.setJdbcUrl(configDataSourceConfig.getUrl());
        hikariConfig.setUsername(configDataSourceConfig.getUsername());
        hikariConfig.setPassword(configDataSourceConfig.getPassword());

        // ✅ 使用配置的参数
        hikariConfig.setMaximumPoolSize(configDataSourceConfig.getMaximumPoolSize());
        hikariConfig.setMinimumIdle(configDataSourceConfig.getMinimumIdle());
        hikariConfig.setConnectionTimeout(configDataSourceConfig.getConnectionTimeout());
        hikariConfig.setIdleTimeout(configDataSourceConfig.getIdleTimeout());
        hikariConfig.setPoolName(configDataSourceConfig.getUsername() + "-config-db-pool");

        return  new HikariDataSource(hikariConfig);
    }





}
