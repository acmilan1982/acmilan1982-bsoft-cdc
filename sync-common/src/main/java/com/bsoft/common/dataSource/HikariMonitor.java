package com.bsoft.common.dataSource;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class HikariMonitor {
    private static final Logger logger = LogManager.getLogger(HikariMonitor.class);

    /**
     * 通过 MXBean 获取连接池详细状态(需要配置 registerMbeans=true)
     */
    public static void logDetailedPoolStats(String poolName) {
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName poolObjectName = new ObjectName("com.zaxxer.hikari:type=Pool (" + poolName + ")");
            HikariPoolMXBean poolProxy = JMX.newMXBeanProxy(mBeanServer, poolObjectName, HikariPoolMXBean.class);

            logger.error("【MBean连接池状态】池名={}, 活动={}, 空闲={}, 总数={}, 等待线程={}",
                    poolName,
                    poolProxy.getActiveConnections(),
                    poolProxy.getIdleConnections(),
                    poolProxy.getTotalConnections(),
                    poolProxy.getThreadsAwaitingConnection());
        } catch (Exception e) {
            logger.error("获取MBean连接池统计失败", e);
        }
    }

    /**
     * 直接从 HikariDataSource 获取状态(不依赖 MBean)
     */
    public static void logPoolStatsDirectly(HikariDataSource dataSource, String tag) {
        try {
            HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
            if (pool != null) {
                logger.error("【直接连接池状态】{}, 活动={}, 空闲={}, 总数={}, 等待线程={}",
                        tag,
                        pool.getActiveConnections(),
                        pool.getIdleConnections(),
                        pool.getTotalConnections(),
                        pool.getThreadsAwaitingConnection());
            } else {
                logger.error("【配置信息】{}, 最大连接数={}, 最小空闲={}, 连接超时={}ms",
                        tag,
                        dataSource.getMaximumPoolSize(),
                        dataSource.getMinimumIdle(),
                        dataSource.getConnectionTimeout());
            }
        } catch (Exception e) {
            logger.error("获取连接池统计失败", e);
        }
    }
}
