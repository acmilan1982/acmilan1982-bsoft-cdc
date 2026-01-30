package com.bsoft.common.dataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class OracleConnectionTest {

    private static final Logger logger = LoggerFactory.getLogger(OracleConnectionTest.class);
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static final AtomicLong totalConnTime = new AtomicLong(0);

    public static void main(String[] args) {
        // 配置数据库连接信息
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:oracle:thin:@//172.18.232.239:1521/orcl");
        config.setUsername("SPT_HIS");
        config.setPassword("Bsoft_2025");
        config.setMaximumPoolSize(200);
        config.setMinimumIdle(10);
        config.setConnectionTimeout(10000);
        config.setIdleTimeout(60000);
        config.setMaxLifetime(1800000);
        config.setPoolName("test-pool");
        config.setConnectionTestQuery("SELECT 1 FROM DUAL");

        HikariDataSource dataSource = new HikariDataSource(config);

        // 启动监控线程
        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> {
            try {
                HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
                int total = successCount.get() + errorCount.get();
                long avgTime = total > 0 ? totalConnTime.get() / total : 0;
                logger.info("连接池状态 - 活动:{} 空闲:{} 总数:{} 等待:{} | 成功:{} 失败:{} 平均耗时:{}ms",
                        pool.getActiveConnections(),
                        pool.getIdleConnections(),
                        pool.getTotalConnections(),
                        pool.getThreadsAwaitingConnection(),
                        successCount.get(),
                        errorCount.get(),
                        avgTime);
            } catch (Exception e) {
                logger.error("监控失败", e);
            }
        }, 0, 5, TimeUnit.SECONDS);

        // 创建50个并发线程
        ExecutorService executor = Executors.newFixedThreadPool(200);

        try {
            logger.info("启动50个并发测试线程...");

            for (int i = 0; i < 150; i++) {
                final int threadId = i + 1;
                executor.submit(() -> {
                    try {
                        // 每个线程循环执行
                        while (!Thread.currentThread().isInterrupted()) {
                            long startTime = System.currentTimeMillis();
                            Connection conn = null;

                            try {
                                conn = dataSource.getConnection();
                                long connTime = System.currentTimeMillis() - startTime;
                                totalConnTime.addAndGet(connTime);
                                logger.info("线程#{} 获取连接耗时: {}ms", threadId, connTime);

                                try (Statement stmt = conn.createStatement();
                                     ResultSet rs = stmt.executeQuery("SELECT SYSDATE FROM DUAL")) {

                                    if (rs.next()) {
                                        successCount.incrementAndGet();
//                                        logger.debug("线程#{} 查询成功: {}", threadId, rs.getTimestamp(1));
                                    }
                                }

                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                                long connTime = System.currentTimeMillis() - startTime;
                                logger.error("线程#{} 获取连接或查询失败,耗时: {}ms", threadId, connTime, e);
                            } finally {
                                if (conn != null) {
                                    try {
                                        conn.close();
                                    } catch (Exception e) {
                                        logger.error("线程#{} 关闭连接失败", threadId, e);
                                    }
                                }
                            }

                            // 间隔1秒
                            Thread.sleep(1000);
                        }
                    } catch (InterruptedException e) {
                        logger.info("线程#{} 被中断", threadId);
                        Thread.currentThread().interrupt();
                    }
                });
            }

            // 运行10分钟
            logger.info("测试运行中,观察日志输出...");
            Thread.sleep(600000);

        } catch (Exception e) {
            logger.error("测试失败", e);
        } finally {
            executor.shutdownNow();
            dataSource.close();
            monitor.shutdown();

            int total = successCount.get() + errorCount.get();
            long avgTime = total > 0 ? totalConnTime.get() / total : 0;
            logger.info("测试结束 - 总成功:{} 总失败:{} 平均获取连接耗时:{}ms",
                    successCount.get(), errorCount.get(), avgTime);
        }
    }
}