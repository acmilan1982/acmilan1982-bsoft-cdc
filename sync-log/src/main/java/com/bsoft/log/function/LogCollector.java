package com.bsoft.log.function;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.bean.RawMessageStorageStrategy;
import com.bsoft.common.bean.SyncLogEntry;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.common.util.DateTimeConstants;
import com.bsoft.log.k2db.LogKafkaMessage;
import com.bsoft.log.offset.LogTopicOffsetTracker;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    private final BlockingQueue<LogKafkaMessage> correctQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<LogKafkaMessage> errorQueue = new LinkedBlockingQueue<>();

    // 成功队列刷新配置，至少5000条或1分钟刷新一次
    private static final int MAX_CORRECT_QUEUE_SIZE = 5000;
    private static final long CORRECT_FLUSH_INTERVAL_MS = 1 * 10 * 1000L;

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



    // 最大 未处理任务个数，达到阈值，暂停发布任务
    private static final int MAX_PENDING_TASK_COUNT = 16;
    // 未处理任务个数检查间隔
    private static final long PENDING_TASK_WAIT_INTERVAL_MS = 16_000L;
    // 环境故障暂停 add 的检查间隔
    private static final long ENVIRONMENT_RETRY_PAUSE_CHECK_INTERVAL_MS = 16_000L;



    // 日志 topic offset 水位线追踪器
    private LogTopicOffsetTracker offsetTracker;

    /**
     * 原始消息存储策略
     * 配置项: server-info.raw-message-storage-strategy
     * 可选值: NONE(不插入) | PLAIN(不压缩) | COMPRESS(压缩,默认)
     */
    private  RawMessageStorageStrategy rawMessageStorageStrategy = RawMessageStorageStrategy.COMPRESS;

    // 为每个 LogCollector 实例生成唯一标识
    private final String instanceId;



    /**
     * offset 持久化调度器。
     */
    private ScheduledExecutorService offsetFlushScheduler;

    /**
     * offset 持久化周期。
     */
    private static final long OFFSET_FLUSH_INTERVAL_MS = 5_000L;



    // ==================== 关于重试 ====================


    private static final long RETRY_INTERVAL_MS = 60_000L; // 可重试环境错误的固定重试间隔，默认 60 秒

    private static final long PAUSE_ADD_LOG_INTERVAL_MS = 60_000L; // add 被暂停时的中文日志打印间隔，默认 60 秒

    private final AtomicBoolean environmentFailureRetrying = new AtomicBoolean(false); // 标记当前是否处于环境故障重试状态

    private volatile String environmentFailureRetryReason = ""; // 记录当前环境故障原因，供暂停 add 时打印

    private volatile long lastPauseAddLogTime = 0L; // 控制 add 暂停期间的日志打印频率

    public LogCollector() {
        this.instanceId = UUID.randomUUID().toString().substring(0, 8);
    }


    public LogCollector(ConfigDataSourceConfig configDataSourceConfig,
                        RawMessageStorageStrategy rawMessageStorageStrategy,
                        LogTopicOffsetTracker offsetTracker) {
        this();
        this.dataSource = configDataSourceConfig.initDataSource();
        this.rawMessageStorageStrategy = rawMessageStorageStrategy;
        // 保存 offset 水位线追踪器。
        this.offsetTracker = offsetTracker;
        initScheduler();
    }


    private void initScheduler() {
        logger.info("实例化LogCollector: {}", instanceId);

        // 创建日志队列刷新调度器。
        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            // 创建日志队列刷新线程。
            Thread t = new Thread(r, "log-flush-scheduler");

            // 设置为守护线程。
            t.setDaemon(true);

            // 返回线程。
            return t;
        });

        // 定时刷新 correctQueue 和 errorQueue。
        this.flushScheduler.scheduleAtFixedRate(
                this::checkAndFlushAllQueues,
                1,
                8,
                TimeUnit.SECONDS
        );



        // 创建 offset 持久化调度器。
        this.offsetFlushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            // 创建 offset 持久化线程。
            Thread t = new Thread(r, "log-offset-flush-scheduler");

            // 设置为守护线程。
            t.setDaemon(true);

            // 返回线程。
            return t;
        });

        // 定时持久化连续 offset 水位线。
        this.offsetFlushScheduler.scheduleAtFixedRate(
                this::flushOffsetQuietly,
                OFFSET_FLUSH_INTERVAL_MS,
                OFFSET_FLUSH_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );






        logger.info("✅ LogCollector 已启动: instanceId={}", instanceId);
    }



    // 添加日志消息到内存队列，环境故障期间会阻塞
    public void add(LogKafkaMessage message) throws InterruptedException {
        // 空消息或解析失败消息直接忽略
        if (message == null || message.getSyncLogEntry() == null) {
            return; // 不进入队列
        }

        if (!running.get()) {
            throw new IllegalStateException("日志收集器已经停止，不能继续接收日志");
        }
        // 如果当前正在处理环境故障，则阻塞 Flink processElement，形成反压
        waitIfEnvironmentFailureRetrying();

        // 根据日志结果码选择成功队列或失败队列
        BlockingQueue<LogKafkaMessage> queue = message.getSyncLogEntry().getResultCode() == 0 ? correctQueue : errorQueue; // resultCode=0 写正确日志，否则写错误日志

        // 记录等待写入线程池空闲的次数
        int waitCount = 0;

        // 写入任务积压过多时暂停入队
        while (getPendingTaskCount() >= MAX_PENDING_TASK_COUNT) {
            // 增加等待次数
            waitCount++;

            // 打印积压日志
            logger.warn("写入线程池繁忙，待处理任务个数/最大待处理任务个数： {}/{}，等待第 {}次", getPendingTaskCount(), MAX_PENDING_TASK_COUNT, waitCount);

            // 写入线程池已经关闭时不能继续接收日志
            if (batchExecutor.isShutdown()) {
                throw new IllegalStateException("写入线程池已经关闭"); // 抛出异常给上游
            }

            // 等待一段时间后再次检查
            Thread.sleep(PENDING_TASK_WAIT_INTERVAL_MS);
        }

        queue.offer(message); // 将日志消息放入对应队列
    }

    private void waitIfEnvironmentFailureRetrying() throws InterruptedException { // 环境故障期间暂停接收新日志
        while (running.get() && environmentFailureRetrying.get()) { // 只要 Collector 仍运行且处于环境故障重试，就持续等待
            long now = System.currentTimeMillis(); // 获取当前时间

            if (now - lastPauseAddLogTime >= PAUSE_ADD_LOG_INTERVAL_MS) { // 到达周期日志打印间隔
                lastPauseAddLogTime = now; // 更新上次打印时间

                logger.warn("日志写入处于环境故障重试中，暂停接收新日志，原因：{}",
                        environmentFailureRetryReason); // 打印中文暂停原因
            }

            Thread.sleep(ENVIRONMENT_RETRY_PAUSE_CHECK_INTERVAL_MS); // 每秒检查一次环境故障是否恢复
        }
    }

    // 获取待处理任务数
    private int getPendingTaskCount() {
        if (batchExecutor instanceof ThreadPoolExecutor) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) batchExecutor;
            return executor.getQueue().size();
        }
        return 0;
    }
//
//    public void add(List<SyncLogEntry> syncLogEntries ) {
//        if (syncLogEntries == null || syncLogEntries.isEmpty()) {
//            return;
//        }
//        // ✅ 按结果码分组，直接批量添加到对应队列
//        for (SyncLogEntry entry : syncLogEntries) {
//            if (entry.getResultCode() == 0) {
//                correctQueue.offer(entry);
//            } else {
//                errorQueue.offer(entry);
//            }
//        }
//    }

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
        // 获取当前系统时间，用于计算距离上次刷新成功日志队列已经过去多久
        long currentTime = System.currentTimeMillis();

        // 获取成功日志队列当前积压的日志数量
        int queueSize = correctQueue.size();

        // 数量触发条件：成功日志队列积压数量达到批量刷新阈值
        boolean sizeThreshold = queueSize >= MAX_CORRECT_QUEUE_SIZE;

        // 时间触发条件：距离上次刷新已经超过时间阈值，并且队列中确实有待写入日志
        boolean timeThreshold = (currentTime - lastCorrectFlushTime >= CORRECT_FLUSH_INTERVAL_MS)
                && queueSize > 0;

        // 数量条件或时间条件任意满足时，就需要刷新成功日志队列
        return sizeThreshold || timeThreshold;
    }

    // ==================== 错误数据队列刷新条件判断 ====================
    private boolean shouldFlushErrorQueue() {
        // 获取当前系统时间，用于计算距离上次刷新错误日志队列已经过去多久
        long currentTime = System.currentTimeMillis();

        // 获取错误日志队列当前积压的日志数量
        int queueSize = errorQueue.size();

        // 数量触发条件：错误日志队列积压数量达到批量刷新阈值
        boolean sizeThreshold = queueSize >= MAX_ERROR_QUEUE_SIZE;

        // 时间触发条件：距离上次刷新已经超过时间阈值，并且队列中确实有待写入日志
        boolean timeThreshold = (currentTime - lastErrorFlushTime >= ERROR_FLUSH_INTERVAL_MS)
                && queueSize > 0;

        // 数量条件或时间条件任意满足时，就需要刷新错误日志队列
        return sizeThreshold || timeThreshold;
    }

    // ==================== 批量写入数据库 ====================
    private void flush(BlockingQueue<LogKafkaMessage> queue,String sql, String tableName) {
        List<LogKafkaMessage> batch = new ArrayList<>(100000);

        logger.info("correctQueue待写入日志----------------------->{} 个SyncLogEntry对象", this.correctQueue.size());

        //  提交前记录状态
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
        List<List<LogKafkaMessage>> subBatches = Lists.partition(batch, SUB_BATCH_SIZE);

        // 直接提交所有子任务（不等待完成）
        for (List<LogKafkaMessage> subBatch : subBatches) {
            batchExecutor.submit(() -> executeBatch(subBatch, sql, tableName));
        }

    }



    // 执行一个子批次的日志写入
    private void executeBatch(List<LogKafkaMessage> batch, String sql, String tableName) {
        // 记录子批次开始时间
        long startTime = System.currentTimeMillis();

        // 记录已成功处理或明确跳过的消息数
        long handledCount = 0L;

        // 创建数据库连接和预编译语句
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 关闭自动提交，由代码控制 commit/rollback
            conn.setAutoCommit(false);

            // 保存当前 JDBC batch 对应的消息，即单批次入库数量
            List<LogKafkaMessage> pendingMessages = new ArrayList<>(BATCH_COMMIT_SIZE);

            // 遍历当前子批次中的每条消息
            for (LogKafkaMessage message : batch) {
                // 先加入待处理集合，真正绑定参数放到提交方法内部
                pendingMessages.add(message);

                // 达到一次 JDBC batch 的提交阈值
                if (pendingMessages.size() >= BATCH_COMMIT_SIZE) {
                    // 写入、提交或按策略跳过当前批
                    handledCount += executeAndCommitBatch(conn, ps, pendingMessages, tableName);
                    // 清空当前待处理集合
                    pendingMessages.clear();
                }
            }

            if (!pendingMessages.isEmpty()) { // 处理最后不足 BATCH_COMMIT_SIZE 的剩余消息
                handledCount += executeAndCommitBatch(conn, ps, pendingMessages, tableName); // 写入、提交或按策略跳过剩余批
                pendingMessages.clear(); // 清空剩余集合
            }

            logger.info("日志子批次处理完成，表名={}，已处理={}/{}，耗时={}ms",
                    tableName, handledCount, batch.size(), System.currentTimeMillis() - startTime); // 打印中文完成日志
        } catch (Exception e) { // 连接创建、预编译或其他非 batch 阶段异常会进入这里
            logger.error("日志子批次处理失败，表名={}，原因={}", tableName, e.getMessage(), e); // 打印中文失败日志
        }
    }
    // 执行并提交一个 JDBC batch，按异常类型决定重试或跳过
    private long executeAndCommitBatch(Connection conn,
                                       PreparedStatement ps,
                                       List<LogKafkaMessage> pendingMessages,
                                       String tableName) throws Exception {
        // 可重试环境错误需要无限重试，直到 Collector 停止或写入成功
        while (running.get()) {
            try {
                // 每次尝试前清空 PreparedStatement 中可能残留的 batch
                ps.clearBatch();

                // 重新绑定当前批次所有消息
                for (LogKafkaMessage message : pendingMessages) {
                    // 设置当前消息的 SQL 参数
                    setParameters(ps, message.getSyncLogEntry(), tableName);

                    // 加入 JDBC batch
                    ps.addBatch();
                }

                // 执行 JDBC batch，正常返回即视为当前批全部成功
                ps.executeBatch();

                // 提交事务
                conn.commit();

                // 清理已提交的 batch 状态
                ps.clearBatch();

                // 写入成功后清除环境故障重试状态
                clearEnvironmentFailureRetrying();

                // 推进当前批次 offset
                markMessagesHandled(pendingMessages);

                // 返回当前批处理成功数量
                return pendingMessages.size();
            } catch (SQLException e) {
                // 当前尝试失败时回滚事务
                rollbackQuietly(conn);

                // 清理失败后的 batch 状态，避免下次重试污染
                try {
                    ps.clearBatch();
                } catch (SQLException clearError) {
                    // 清理 batch 失败只记录日志，不改变原异常处理路径
                    logger.warn("清理 JDBC batch 失败，表名={}，原因={}", tableName, clearError.getMessage());
                }

                // 如果确认是日志对象数据本身的问题，不重试
                if (isNonRetryableOracleDataError(e)) {
                    // 日志记录整批跳过信息
                    logSkippedBatch(tableName, pendingMessages, e);

                    // 预留单条重试/坏数据定位入口，当前不实现
                    handleSingleRetryReserved(pendingMessages, tableName, e);

                    // 数据问题不会进入环境故障重试状态
                    clearEnvironmentFailureRetrying();

                    // 明确跳过后推进 offset
                    markMessagesHandled(pendingMessages);

                    // 返回当前批已被处理
                    return pendingMessages.size();
                }

                // 环境或未知数据库问题进入重试状态
                enterEnvironmentFailureRetrying(tableName, pendingMessages, e);

                // 固定间隔后重试同一批数据
                Thread.sleep(RETRY_INTERVAL_MS);
            } catch (Exception e) {
                // 非 SQLException 通常是参数绑定、对象字段、压缩处理或代码路径问题，默认不重试
                rollbackQuietly(conn);

                // 清理失败后的 batch 状态，避免影响后续批次
                try {
                    ps.clearBatch();
                } catch (SQLException clearError) {
                    // 清理 batch 失败只记录日志，不改变当前批跳过策略
                    logger.warn("清理 JDBC batch 失败，表名={}，原因={}", tableName, clearError.getMessage());
                }

                // 记录非 SQL 异常导致的整批跳过信息
                logger.error("日志批次写入失败，判定为非 SQL 数据或程序问题，跳过当前批次并推进 offset。表名={}，批次数量={}，原因={}",
                        tableName, pendingMessages.size(), e.getMessage(), e);

                // 非 SQL 问题不会进入环境故障重试状态
                clearEnvironmentFailureRetrying();

                // 明确跳过后推进 offset
                markMessagesHandled(pendingMessages);

                // 返回当前批已被处理
                return pendingMessages.size();
            }
        }

        // Collector 已停止时返回 0
        return 0L;
    }

    // 判断 Oracle 错误是否属于明确不可重试的数据问题
    private boolean isNonRetryableOracleDataError(SQLException e) {
        SQLException current = e; // 从当前 SQLException 开始遍历异常链

        while (current != null) { // 遍历 SQLException nextException 链
            int errorCode = current.getErrorCode(); // Oracle errorCode 通常是 ORA 编号的数字部分

            if (errorCode == 12899 // ORA-12899 字段值过大
                    || errorCode == 1400 // ORA-01400 不能插入 NULL
                    || errorCode == 1438 // ORA-01438 数值精度超出
                    || errorCode == 1722 // ORA-01722 无效数字
                    || errorCode == 1840 // ORA-01840 日期输入值不够长
                    || errorCode == 1841 // ORA-01841 日期年份非法
                    || errorCode == 1843 // ORA-01843 月份非法
                    || errorCode == 1847 // ORA-01847 日期非法
                    || errorCode == 1858 // ORA-01858 非数字出现在要求数字的位置
                    || errorCode == 1861) { // ORA-01861 日期格式不匹配
                return true; // 命中白名单，视为不可重试数据错误
            }

            current = current.getNextException(); // 继续检查下一个 SQLException
        }

        return false; // 未命中白名单，保守视为需要重试
    }

    private void markMessagesHandled(List<LogKafkaMessage> messages) { // 标记当前消息集合已经处理完成，可以推进 offset
        long minOffset = messages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .min()
                .orElse(-1L); // 计算当前批最小 offset

        long maxOffset = messages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .max()
                .orElse(-1L); // 计算当前批最大 offset

        offsetTracker.markSuccessRange(minOffset, maxOffset); // 将当前批 offset 区间标记为已处理
    }

    // 安静回滚事务
    private void rollbackQuietly(Connection conn) {
        try {
            conn.rollback(); // 回滚当前事务
        } catch (Exception rollbackError) {
            logger.warn("日志写入事务回滚失败，原因={}", rollbackError.getMessage()); // 回滚失败只记录中文日志
        }
    }

    private void enterEnvironmentFailureRetrying(String tableName,
                                                 List<LogKafkaMessage> pendingMessages,
                                                 SQLException e) { // 进入环境故障重试状态
        environmentFailureRetrying.set(true); // 设置环境故障标志，阻塞 add 接收新日志

        environmentFailureRetryReason = buildRetryReason(tableName, pendingMessages, e); // 构造中文重试原因

        logger.error("日志批次写入失败，判定为环境或未知问题，将无限重试，不推进 offset。{}",
                environmentFailureRetryReason, e); // 打印中文错误日志
    }

    private void clearEnvironmentFailureRetrying() { // 清除环境故障重试状态
        if (environmentFailureRetrying.compareAndSet(true, false)) { // 只有从故障状态恢复时打印一次
            logger.info("日志写入环境故障已恢复，继续接收新日志"); // 打印中文恢复日志
        }

        environmentFailureRetryReason = ""; // 清空环境故障原因
    }

    private String buildRetryReason(String tableName,
                                    List<LogKafkaMessage> pendingMessages,
                                    SQLException e) { // 构造环境故障重试原因
        long minOffset = pendingMessages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .min()
                .orElse(-1L); // 当前批最小 offset

        long maxOffset = pendingMessages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .max()
                .orElse(-1L); // 当前批最大 offset

        return "表名=" + tableName
                + "，批次数量=" + pendingMessages.size()
                + "，offset范围=" + minOffset + "-" + maxOffset
                + "，Oracle错误码=" + e.getErrorCode()
                + "，错误信息=" + e.getMessage(); // 返回中文原因
    }

    // 记录不可重试数据错误导致的整批跳过
    private void logSkippedBatch(String tableName,
                                 List<LogKafkaMessage> pendingMessages,
                                 SQLException e) {

        // 当前批最小 offset
        long minOffset = pendingMessages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .min()
                .orElse(-1L);

        // 当前批最大 offset
        long maxOffset = pendingMessages.stream()
                .mapToLong(LogKafkaMessage::getKafkaOffset)
                .max()
                .orElse(-1L);

        logger.error("日志批次写入失败，判定为数据问题，跳过当前批次并推进 offset。表名={}，批次数量={}，offset范围={}-{}，Oracle错误码={}，错误信息={}",
                tableName, pendingMessages.size(), minOffset, maxOffset, e.getErrorCode(), e.getMessage(), e); // 中文记录丢失范围
    }

    private void handleSingleRetryReserved(List<LogKafkaMessage> pendingMessages,
                                           String tableName,
                                           SQLException e) { // 预留单条重试/坏数据定位入口，当前版本不实现
        logger.warn("预留单条重试入口暂未实现，当前按整批跳过处理。表名={}，批次数量={}，Oracle错误码={}",
                tableName, pendingMessages.size(), e.getErrorCode()); // 中文说明当前策略
    }
    /**
     * 判断 JDBC batch 是否全部成功。
     */
    private boolean isAllBatchSuccess(int[] results, int expectedSize) {
        // 返回数量不一致时视为不完全成功。
        if (results == null || results.length != expectedSize) {
            return false;
        }

        // 遍历每条执行结果。
        for (int result : results) {
            // 任意一条失败都视为不完全成功。
            if (result == PreparedStatement.EXECUTE_FAILED) {
                return false;
            }
        }

        // 所有结果均未失败。
        return true;
    }

    /**
     * 统计 JDBC batch 成功数量。
     */
    private long countSuccess(int[] results) {
        // 空结果成功数量为 0。
        if (results == null) {
            return 0L;
        }

        // 统计未失败的结果数量。
        return Arrays.stream(results)
                .filter(result -> result != PreparedStatement.EXECUTE_FAILED)
                .count();
    }

    // ==================== 参数绑定 ====================
    private void setParameters(PreparedStatement ps, SyncLogEntry entry,String tableName) throws Exception {
        ps.setLong(1, entry.getCdcLogId());
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
        // CDC_LOG_ERROR 永远保存原始字符串，方便直接排查错误数据。
        // CDC_LOG_CORRECT 取决于存储策略
        bindRawMessageClob(ps, 15, entry.getRawMessage(), tableName);
    }

    // 根据目标表和存储策略，把 rawMessage 转换成最终写入 CLOB 的文本。
    private String processRawMessage(String rawMessage, String tableName) throws IOException {
        // RAW_MESSAGE 为空时统一写 NULL。
        if (rawMessage == null || rawMessage.isEmpty()) {
            return null;
        }

        // CDC_LOG_ERROR 永远保存原始字符串，方便直接排查错误数据。
        if ("CDC_LOG_ERROR".equals(tableName)) {
            return rawMessage;
        }

        // NONE 策略不保存 CDC_LOG_CORRECT 的 RAW_MESSAGE。
        if (rawMessageStorageStrategy == RawMessageStorageStrategy.NONE) {
            return null;
        }

        // PLAIN 策略保存 CDC_LOG_CORRECT 的原始字符串。
        if (rawMessageStorageStrategy == RawMessageStorageStrategy.PLAIN) {
            return rawMessage;
        }

        // COMPRESS 策略保存 GZIP 后再 Base64 的字符串。
        if (rawMessageStorageStrategy == RawMessageStorageStrategy.COMPRESS) {
            return gzipToBase64(rawMessage);
        }

        // 未知策略直接失败，避免静默写入错误格式。
        throw new IllegalStateException("unsupported rawMessageStorageStrategy: " + rawMessageStorageStrategy);
    }

    // 将 RAW_MESSAGE 文本按 CLOB 语义绑定到 PreparedStatement。
    private void bindRawMessageClob(PreparedStatement ps,
                                    int parameterIndex,
                                    String rawMessage,
                                    String tableName) throws Exception {
        // 计算最终要写入 CLOB 的文本。
        String clobText = processRawMessage(rawMessage, tableName);

        // 没有内容时显式写入 CLOB NULL。
        if (clobText == null) {
            ps.setNull(parameterIndex, Types.CLOB);
            return;
        }

        // 使用字符流写入 CLOB，避免超长字符串触发 VARCHAR 限制。
        ps.setCharacterStream(parameterIndex, new StringReader(clobText), clobText.length());
    }

    // 将字符串按 UTF-8 编码、GZIP 压缩、Base64 编码成 CLOB 可保存文本。
    private String gzipToBase64(String rawMessage) throws IOException {
        // 使用内存字节流承载 GZIP 输出。
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream gzip = new GZIPOutputStream(bos)) {

            // 固定使用 UTF-8 编码，避免受系统默认编码影响。
            gzip.write(rawMessage.getBytes(StandardCharsets.UTF_8));

            // 完成 GZIP 数据写入。
            gzip.finish();

            // 把二进制压缩结果转换成 Base64 字符串。
            return Base64.getEncoder().encodeToString(bos.toByteArray());
        }
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



    /**
     * 安静地持久化 offset 水位线。
     */
    private void flushOffsetQuietly() {
        try {
            // 如果 collector 已停止，则不再持久化 offset。
            if (!running.get()) {
                return;
            }

            // offsetTracker 为空时直接返回。
            if (offsetTracker == null) {
                return;
            }

            // 将连续水位线写入 CDC_LOG_TOPIC_OFFSET。
            offsetTracker.flush();
        } catch (Exception e) {
            // offset 持久化失败只会导致重复消费，记录日志后等待下次重试。
            logger.warn("flush log topic offset failed, err={}", e.getMessage());
        }
    }


    /**
     * 关闭日志收集器。
     */
    public void shutdown() {
        // 标记停止运行。
        running.set(false);

        // 停止定时刷新任务。
        flushScheduler.shutdown();

        // 停止写入线程池。
        batchExecutor.shutdown();

        try {
            // 等待写入线程尽量完成。
            batchExecutor.awaitTermination(60, TimeUnit.SECONDS);

            // 最后尝试持久化一次 offset 水位线。
            if (offsetTracker != null) {
                offsetTracker.flush();
            }
        } catch (Exception e) {
            // 关闭阶段异常只记录日志。
            logger.warn("flush log topic offset on shutdown failed, err={}", e.getMessage());
        }

        // 打印关闭日志。
        logger.info("LogCollector closed");
    }
}
