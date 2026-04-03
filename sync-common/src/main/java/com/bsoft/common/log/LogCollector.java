package com.bsoft.common.log;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.bean.SyncLogEntry;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.common.util.DateTimeConstants;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

/**
 * 日志收集器 - 异步批量写入同步日志到配置库
 * 优化策略:
 * 1. 无界队列，保证所有日志不丢失
 * 2. 按成功/失败分队列存储，减少锁竞争
 * 3. 循环刷新机制，快速清空积压队列
 * 4. 完全由定时任务控制刷新时机
 * 5. 错误队列也支持时间阈值触发刷新
 */
public class LogCollector {

    private static final Logger logger = LogManager.getLogger(LogCollector.class);

    // ==================== 队列配置（无界队列） ====================
    private final BlockingQueue<SyncLogEntry> correctQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<SyncLogEntry> errorQueue = new LinkedBlockingQueue<>();

    // 成功队列刷新配置，至少5000条或1分钟刷新一次
    private static final int MAX_CORRECT_QUEUE_SIZE = 5000;
    private static final long CORRECT_FLUSH_INTERVAL_MS = 1 * 60 * 1000L;

    // 失败队列刷新配置，至少5000条或1分钟刷新一次
    private static final int MAX_ERROR_QUEUE_SIZE = 5000;
    private static final long ERROR_FLUSH_INTERVAL_MS = 1 * 60 * 1000;

    // ==================== 定时刷新配置 ====================
    private  ScheduledExecutorService flushScheduler;


    // ==================== 批量写入线程池 ====================
    private final ExecutorService batchExecutor = new ThreadPoolExecutor(
            8,                           // 核心线程数
            16,                                      // 最大线程数
            60L, TimeUnit.SECONDS,                 // 空闲线程存活时间
            new LinkedBlockingQueue<>(),            // 无界队列
            new ThreadFactory() {
                private int count = 0;
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "log-batch-writer-" + (++count));
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy()  // 拒绝策略
    );

    private static final int SUB_BATCH_SIZE = 10000;  // 每个子任务处理的日志数
    private static final int BATCH_COMMIT_SIZE = 1000; // 每个子任务每次批量提交的大小

    // ==================== 刷新控制标志 ====================
    private volatile long lastCorrectFlushTime = System.currentTimeMillis();
    private volatile long lastErrorFlushTime = System.currentTimeMillis();      // ✅ 新增错误队列时间戳
    private final AtomicBoolean running = new AtomicBoolean(true);

    // ==================== 数据源配置 ====================
    private DataSource dataSource;


    private static final int MAX_PENDING_TASK_COUNT = 16;
    private static final int MAX_ADD_RETRIES = 32; // 最多等待 1 分钟


    /**
     * 原始消息存储策略
     * 配置项: server-info.raw-message-storage-strategy
     * 可选值: NONE(不插入) | PLAIN(不压缩) | COMPRESS(压缩,默认)
     */
    private  RawMessageStorageStrategy rawMessageStorageStrategy = RawMessageStorageStrategy.COMPRESS;

    // ✅ 为每个 LogCollector 实例生成唯一标识
    private final String instanceId;

    public LogCollector() {
        this.instanceId = UUID.randomUUID().toString().substring(0, 8);
    }

    public LogCollector(ConfigDataSourceConfig configDataSourceConfig) {
        this();
        this.dataSource = configDataSourceConfig.initDataSource();
        initScheduler();
    }

    public LogCollector(ConfigDataSourceConfig configDataSourceConfig, RawMessageStorageStrategy rawMessageStorageStrategy) {
        this();
        this.dataSource = configDataSourceConfig.initDataSource();
        this.rawMessageStorageStrategy = rawMessageStorageStrategy;
        initScheduler();
    }
    private void initScheduler() {
        logger.info("实例化LogCollector: {}", instanceId);

        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "log-flush-scheduler");
            t.setDaemon(true);
            return t;
        });

        this.flushScheduler.scheduleAtFixedRate(
                this::checkAndFlushAllQueues,
                1, 8, TimeUnit.SECONDS
        );

        logger.info("✅ LogCollector 已启动: instanceId={}", instanceId);
    }

    // ==================== 添加日志====================
    public void add(SyncLogEntry entry) throws TimeoutException, InterruptedException {
        BlockingQueue<SyncLogEntry> queue = (entry.getResultCode() == 0) ? correctQueue : errorQueue;


        int retryCount = 0;


        while (getPendingTaskCount() >= MAX_PENDING_TASK_COUNT) {
            retryCount++;

            //  1. 记录告警日志
            logger.warn("⚠写入线程池繁忙(待处理任务={}/{}),等待第 {}/{} 次重试...",
                    getPendingTaskCount(), MAX_PENDING_TASK_COUNT, retryCount, MAX_ADD_RETRIES);


            //  2. 检查死锁
            if (batchExecutor.isShutdown()) {
                throw new IllegalStateException("写入线程池已关闭");
            }

            //  3. 防止永久阻塞
            if (retryCount >= MAX_ADD_RETRIES) {
                throw new TimeoutException("队列长时间堵塞,疑似死锁");
            }


            Thread.sleep(16000);
        }

        queue.offer(entry);
    }

    // ✅获取待处理任务数
    private int getPendingTaskCount() {
        if (batchExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) batchExecutor;
            return executor.getQueue().size();
        }
        return 0;
    }

    public void add(List<SyncLogEntry> syncLogEntries ) {
        if (syncLogEntries == null || syncLogEntries.isEmpty()) {
            return;
        }
        // ✅ 按结果码分组，直接批量添加到对应队列
        for (SyncLogEntry entry : syncLogEntries) {
            if (entry.getResultCode() == 0) {
                correctQueue.offer(entry);
            } else {
                errorQueue.offer(entry);
            }
        }
    }

    // ==================== 定时检查所有队列 ====================
    private void checkAndFlushAllQueues() {
        logger.info("✅ checkAndFlushAllQueues：启动", LocalDateTime.now().format(DateTimeConstants.FMT_DATETIME));
        if (!running.get()) {
            return;
        }
        checkAndFlushCorrectQueue();
        checkAndFlushErrorQueue();
    }

    // ==================== 检查并刷新正确队列（循环刷新） ====================
    private void checkAndFlushCorrectQueue() {


        while (shouldFlushCorrectQueue()) {
            flush(correctQueue,CdcSqlConstants.INSERT_CDC_LOG_CORRECT, "CDC_LOG_CORRECT");
            lastCorrectFlushTime = System.currentTimeMillis();
        }

    }


    // ==================== 检查并刷新错误队列（循环刷新） ====================
    private void checkAndFlushErrorQueue() {

        while (shouldFlushErrorQueue()) {
            flush(errorQueue,CdcSqlConstants.INSERT_CDC_LOG_ERROR, "CDC_LOG_ERROR");
            lastErrorFlushTime = System.currentTimeMillis(); // ✅ 更新错误队列时间戳
        }

    }

    // ==================== 正确数据队列刷新条件判断 ====================
    private boolean shouldFlushCorrectQueue() {
        long currentTime = System.currentTimeMillis();
        int queueSize = correctQueue.size();

        boolean sizeThreshold = queueSize >= MAX_CORRECT_QUEUE_SIZE;
        boolean timeThreshold = (currentTime - lastCorrectFlushTime >= CORRECT_FLUSH_INTERVAL_MS)
                && queueSize > 0;

        return sizeThreshold || timeThreshold;
    }

    // ==================== 错误数据队列刷新条件判断 ====================
    private boolean shouldFlushErrorQueue() {
        long currentTime = System.currentTimeMillis();
        int queueSize = errorQueue.size();

        // ✅ 新增：超过时间间隔且队列非空时触发刷新
        boolean sizeThreshold = queueSize >= MAX_ERROR_QUEUE_SIZE;
        boolean timeThreshold = (currentTime - lastErrorFlushTime >= ERROR_FLUSH_INTERVAL_MS)
                && queueSize > 0;

        return sizeThreshold || timeThreshold;
    }

    // ==================== 批量写入数据库 ====================
    private void flush(BlockingQueue<SyncLogEntry> queue,String sql, String tableName) {
        List<SyncLogEntry> batch = new ArrayList<>(100000);

        logger.info("correctQueue待写入日志----------------------->{} 个SyncLogEntry对象", this.correctQueue.size());

        // ✅ 提交前记录状态
        if (batchExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) batchExecutor;

            int size = executor.getQueue().size();            // 待处理日志条数
            int activeThreads = executor.getActiveCount();    // 正在执行任务
            int poolSize = executor.getPoolSize();            // 总线程数
            int idleThreads = poolSize - activeThreads;       // 空闲线程

            logger.warn("CORRECT日志队列待处理个数={},每个任务至多{}条记录,活动线程={}, 空闲线程={}, 总线程={}",
                    size, SUB_BATCH_SIZE, activeThreads, idleThreads, poolSize);
        }

        queue.drainTo(batch, 100000);

        if (batch.isEmpty()) {
            return;
        }

        // 拆分为子任务
        List<List<SyncLogEntry>> subBatches = Lists.partition(batch, SUB_BATCH_SIZE);

        // 直接提交所有子任务（不等待完成）
        for (List<SyncLogEntry> subBatch : subBatches) {
            batchExecutor.submit(() -> executeBatch(subBatch, sql, tableName));
        }

    }



    // 优化 executeBatch 方法 - 分批提交
    private void executeBatch(List<SyncLogEntry> batch, String sql, String tableName) {
        long startTime = System.currentTimeMillis();
        long successCount = 0;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);


            for (int i = 0; i < batch.size(); i++) {
                SyncLogEntry entry = batch.get(i);

                try {
                    setParameters(ps, entry, tableName);
                    ps.addBatch();

                    // 每500条执行一次批量提交
                    if ((i + 1) % BATCH_COMMIT_SIZE == 0) {
                        int[] results = ps.executeBatch();
                        conn.commit();
                        successCount += Arrays.stream(results)
                                .filter(r -> r != PreparedStatement.EXECUTE_FAILED)
                                .count();
                        ps.clearBatch();
                    }
                } catch (Exception e) {
                    logger.warn("参数绑定失败: {}", e.getMessage());
                }
            }

            // 提交剩余的数据
            if (batch.size() % BATCH_COMMIT_SIZE != 0) {
                int[] results = ps.executeBatch();
                conn.commit();
                successCount += Arrays.stream(results)
                        .filter(r -> r != PreparedStatement.EXECUTE_FAILED)
                        .count();
            }

            logger.info("LogCollector子任务完成: 表={}, 成功插入日志={}/{}条, 耗时={}ms",
                    tableName, successCount, batch.size(),
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            logger.error("LogCollector子任务失败: 表={}, 错误={}", tableName, e.getMessage());
        }
    }

    // ==================== 参数绑定 ====================
    private void setParameters(PreparedStatement ps, SyncLogEntry entry,String tableName) throws Exception {
        ps.setString(1, entry.getCdcLogId());
        ps.setString(2, entry.getInstructionType());
        ps.setString(3, entry.getSourceTime());
        ps.setString(4, entry.getTargetTime());
//        ps.setObject(5, entry.getInsertTime());
        ps.setString(5, entry.getLogDetail());
        ps.setString(6, entry.getSourceDataSourceId());
        ps.setString(7, entry.getTargetDataSourceId());
        ps.setString(8, entry.getSourceTableName());
        ps.setString(9, entry.getTargetTableName());
        ps.setString(10, entry.getResultDetail());

        if (entry.getResultCode() != null) {
            ps.setInt(11, entry.getResultCode());
        } else {
            ps.setNull(11, Types.INTEGER);
        }

        if (entry.getOffset() != null) {
            ps.setLong(12, entry.getOffset());
        } else {
            ps.setNull(12, Types.BIGINT);
        }

        ps.setString(13, entry.getKafkaEnqueueTime());
        ps.setString(14, entry.getSourceSchemaName());
        if (tableName.equals("CDC_LOG_ERROR")){
            ps.setString(15,entry.getRawMessage());
        }
        else{
            ps.setBytes(15, processRawMessage(entry.getRawMessage()));
        }
    }

    private byte[] processRawMessage(String rawMessage) {

        if (rawMessageStorageStrategy == RawMessageStorageStrategy.NONE) {            // 不插入该字段
            return null;
        } else if (rawMessageStorageStrategy == RawMessageStorageStrategy.PLAIN) {    // 直接以字符串形式存储
            return rawMessage != null ? rawMessage.getBytes() : null;
        } else if (rawMessageStorageStrategy == RawMessageStorageStrategy.COMPRESS) { // 使用 GZIP 压缩后存储
            return compressString(rawMessage);
        }
        return null;
    }

    // 保留原有的工具方法
    public static String getSafeStackTraceHead(Throwable e, int maxLines, int maxLength) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        String[] lines = sw.toString().split("\\r?\\n");
        StringBuilder limited = new StringBuilder();

        for (int i = 0; i < Math.min(maxLines, lines.length); i++) {
            limited.append(lines[i]).append(System.lineSeparator());
        }

        if (lines.length > maxLines) {
            limited.append("... (truncated, total ").append(lines.length).append(" lines)");
        }

        String result = limited.toString();

        if (result.length() > maxLength) {
            result = result.substring(0, maxLength - 20) + "... (truncated)";
        }

        return result;
    }

    // 在 LogCollector 类中添加压缩方法
    private byte[] compressString(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(str.getBytes("UTF-8"));
            gzip.finish();
            return bos.toByteArray();
        } catch (IOException e) {
            logger.info("压缩消息失败: {}", e.getMessage());
            return str.getBytes(); // 失败则返回原始字节
        }
    }



    // ==================== 关闭资源 ====================
    public void shutdown() {
        running.set(false);

        // 1.先停止定时任务,等待当前任务完成
        flushScheduler.shutdown();
        // 2.等待写入线程完成
        batchExecutor.shutdown();


        logger.info("✅ LogCollector 已关闭");
    }
}