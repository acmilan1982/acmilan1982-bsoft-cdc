package com.bsoft.client.dispatcher;

import com.bsoft.client.failure.*;
import com.bsoft.client.flink.SharedMiniClusterManager;
import com.bsoft.client.o2k.ClientJobRuntime;
import com.bsoft.client.o2k.LoadConfigResult;
import com.bsoft.client.o2k.LoadConfigResultType;
import com.bsoft.client.o2k.OracleToKafka;
import com.bsoft.client.signal.JobFailureListener;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.enums.JobStatus;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.common.util.IdGenerateUtil;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.util.StackTraceUtils;
import com.bsoft.common.zk.service.ClientJobZkService;
import org.apache.flink.core.execution.JobClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

import com.bsoft.common.enums.ClientProcessStatus;
import com.bsoft.common.zk.service.ClientProcessZkService;
/**
 * 客户端 Flink Job 调度器
 *
 * 运行位置：
 * 1. 运行在 sync-client 自身 JVM 中
 * 2. 作为 Spring Boot 启动后的入口调度器
 *
 * 职责：
 * 1. 应用启动后触发 OracleToKafka.startJob()
 * 2. 负责单个业务库 job 的 stop / wait / restart
 * 3. 负责共享 MiniCluster 的生命周期管理
 *
 * 当前版本设计：
 * 1. 一个 sync-client 进程内只创建一个 SharedMiniClusterManager
 * 2. 多个 BusinessDataSourceConfig 对应的多个 Flink Job，共享这一个 MiniCluster
 * 3. restart / reload 全部按 dataSourceId 维度执行
 *
 * 注意：
 * 1. 当前类不负责具体 job graph 的构建
 * 2. 当前类不直接向 MiniCluster 提交 job
 * 3. 这些工作交由 OracleToKafka 完成
 */
@Component
// 客户端 Flink Job 调度器，负责启动、重启和关闭业务库级 job。
public class ClientJobDispatcher implements CommandLineRunner, JobFailureListener {

    private static final Logger logger = LogManager.getLogger(ClientJobDispatcher.class);

    /**
     * 客户端配置
     */
    @Autowired
    private ClientInfo clientInfo;

    /**
     * Oracle CDC -> Kafka 核心执行器
     *
     * 说明：
     * OracleToKafka 负责：
     * 1. 加载业务配置
     * 2. 构建每个业务库自己的 runtime
     * 3. 为每个业务库构建并提交独立 job
     */
    @Autowired
    private OracleToKafka oracleToKafka;

    /**
     * 进程内共享 MiniCluster 管理器
     *
     * 说明：
     * 1. 整个 sync-client 进程只保留一个
     * 2. 所有业务库 job 共享它
     */
    private SharedMiniClusterManager sharedMiniClusterManager;

    /**
     * client 进程级 ZK 服务
     *
     * 职责：
     * 1. 管理 /clients/{clientId}/status
     * 2. 管理 /clients/{clientId}/ip
     * 3. 管理 /clients/{clientId}/jobs
     *
     * 说明：
     * 1. 这是 sync-client 进程级资源
     * 2. 由 ClientJobDispatcher 持有并负责生命周期管理
     */
    private ClientProcessZkService clientProcessZkService;



    // 自动重启初始退避秒数。
    private static final long INITIAL_RESTART_DELAY_SECONDS = 60L;

    // 自动重启最大退避秒数。
    private static final long MAX_RESTART_DELAY_SECONDS = 600L;

    // 新 job 稳定运行多久后清零连续失败次数。
    private static final long STABLE_RUN_SECONDS = 300L;

    // 时间格式化器，用于写入 ZK detailInfo。
    private static final DateTimeFormatter RESTART_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 失败重启调度器，用于按 dataSourceId 独立安排延迟重启任务。
    private final ScheduledExecutorService failedJobRestartScheduler =
            Executors.newScheduledThreadPool(4, r -> {
                // 创建自动重启调度线程。
                Thread t = new Thread(r, "failed-job-restart-scheduler");

                // 设置为守护线程，避免阻塞 JVM 退出。
                t.setDaemon(true);

                // 返回线程实例。
                return t;
            });

    // 每个 dataSourceId 独立维护一份自动重启状态。
    private final Map<String, RestartState> restartStatesByDataSourceId = new ConcurrentHashMap<>();



    // 作业失败事件仓库，用于写入 CDC_JOB_FAILURE_EVENT。
    @Autowired
    private JobFailureEventRepository jobFailureEventRepository;

    // 作业失败处理记录仓库，用于写入 CDC_JOB_FAILURE_HANDLE_LOG。
    @Autowired
    private JobFailureHandleLogRepository jobFailureHandleLogRepository;
    /**
     * Spring Boot 启动后自动执行
     *
     * 启动流程：
     * 1. 输出调试信息
     * 2. 初始化 client 进程级 ZK 服务
     * 3. 写入 client 进程级状态：INITIALIZING
     * 4. 启动共享 MiniCluster
     * 5. 初始化 OracleToKafka
     * 6. 启动当前客户端下所有业务库对应的 Flink Job
     * 7. 启动成功后写入 client 进程级状态：RUNNING
     *
     * 注意：
     * 1. 这里写入的是 ClientProcessStatus
     * 2. /clients/{clientId}/status 不再表达某个具体 job 的状态
     */
    @Override
    public void run(String... args) {
        try {
            debug();

            // 1. 初始化 client 进程级 ZK 服务。
            this.clientProcessZkService = ClientProcessZkService.open(clientInfo);

            // 2. 初始化持久节点结构。
            this.clientProcessZkService.initClientProcessNodes();

            // 3. 更新 client IP 节点。
            this.clientProcessZkService.updateIp();

             // 4. 注册 client 存活临时节点。
            this.clientProcessZkService.registerAliveNode();

            // 5. 进程进入初始化阶段。
            this.clientProcessZkService.updateStatus(ClientProcessStatus.INITIALIZING);

            // 6. 启动共享 MiniCluster
            //
            // 说明：
            // 1. 当前版本先给一个默认的共享集群规模
            // 2. 后续如果需要，可以再把 numTaskManagers / slots 做成配置项
            // 3. clientInfo.getFlinkUiPort() 现在表示“共享集群唯一的 UI 端口”
            this.sharedMiniClusterManager = new SharedMiniClusterManager(
                    clientInfo.getFlinkUiPort(),
                    1,
                    8
            );
            this.sharedMiniClusterManager.start();

            logger.info("Shared MiniCluster started, uiPort={}",
                    sharedMiniClusterManager.getWebUiPort());

            // 7. 初始化 OracleToKafka，并把 dispatcher 交给它
            oracleToKafka.init(this);

            // 8. 启动所有业务库对应的 Flink Job
            oracleToKafka.startJob();

            // 9. 启动主流程完成，先将 client 进程状态写为 RUNNING
            this.clientProcessZkService.updateStatus(ClientProcessStatus.RUNNING);

        } catch (Exception e) {
            e.printStackTrace();
            // 启动失败时，写入进程级 ERROR 状态
            try {
                if (this.clientProcessZkService != null) {
                    this.clientProcessZkService.updateStatus(ClientProcessStatus.ERROR
                                                             .withDetailInfo(ExceptionDetailUtil.buildRootCauseDetail(e, 100)));
                }
            } catch (Exception zkEx) {
                logger.warn("Failed to update client process status to ERROR", zkEx);
            }

//            throw new RuntimeException("启动客户端 CDC Jobs 失败", e);
        }
    }



    // 接收 JobFailureZkReporter 发来的 job 失败事件。
    @Override
    public void onJobFailed(String dataSourceId, String failedJobId, Throwable throwable) {

        // 校验 dataSourceId，避免无效失败事件进入重启调度。
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            // 写入无效失败事件。
            JobFailureEvent failureEvent =
                    insertJobFailureEventQuietly(dataSourceId, failedJobId, throwable, JobFailureEventResult.IGNORED_INVALID, "dataSourceId is blank");

            // 写入无效事件处理记录。
            insertIgnoredJobFailureHandleLogQuietly(
                    failureEvent,
                    JobFailureHandleStage.JOB_FAILURE_IGNORED_INVALID,
                    "dataSourceId is blank"
            );

            // 记录忽略日志。
            logger.warn("忽略当前job失败事件：because dataSourceId is blank. failedJobId={}", failedJobId);

            // 结束处理。
            return;
        }

        // 校验 failedJobId，避免无法确认旧 job 身份。
        if (failedJobId == null || failedJobId.trim().isEmpty()) {
            // 写入无效失败事件。
            JobFailureEvent failureEvent =
                    insertJobFailureEventQuietly(dataSourceId, failedJobId, throwable, JobFailureEventResult.IGNORED_INVALID, "failedJobId is blank");

            // 写入无效事件处理记录。
            insertIgnoredJobFailureHandleLogQuietly(
                    failureEvent,
                    JobFailureHandleStage.JOB_FAILURE_IGNORED_INVALID,
                    "failedJobId is blank"
            );

            // 记录忽略日志。
            logger.warn("忽略当前job失败事件：because failedJobId is blank. clientId={}, dataSourceId={}",
                    clientInfo.getClientId(), dataSourceId);

            // 结束处理。
            return;
        }

        // 判断失败事件是否仍然对应当前runtime持有的JobClient。
        boolean currentJob = isCurrentJobFailureEvent(dataSourceId, failedJobId);

        // 如果当前 runtime 已经不再指向这个失败 job，说明这是旧回调，直接忽略。
        if (!currentJob) {
            // 写入旧事件忽略记录。
            JobFailureEvent failureEvent =
                    insertJobFailureEventQuietly(dataSourceId, failedJobId, throwable, JobFailureEventResult.IGNORED_STALE, "failed job is no longer current");

            // 写入旧事件处理记录。
            insertIgnoredJobFailureHandleLogQuietly(
                    failureEvent,
                    JobFailureHandleStage.JOB_FAILURE_IGNORED_STALE,
                    "failed job is no longer current"
            );

            // 记录旧事件忽略日志。
            logger.warn("Ignore stale job failure event. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);

            // 结束处理。
            return;
        }

        // 写入已接受的失败事件。 写入 CDC_JOB_FAILURE_EVENT 表。
        JobFailureEvent failureEvent = insertJobFailureEventQuietly(dataSourceId, failedJobId, throwable, JobFailureEventResult.ACCEPTED, null);

        // 写入已接收处理记录。 写入 CDC_JOB_FAILURE_HANDLE_LOG 表。
        insertSimpleJobFailureHandleLogQuietly(
                failureEvent,
                JobFailureHandleStage.JOB_FAILURE_RECEIVED,
                null
        );

        // 获取当前 dataSourceId 对应的重启状态。
        // 每个业务库，始终对应一个RestartState实例
        RestartState restartState = restartStatesByDataSourceId.computeIfAbsent(dataSourceId, id -> new RestartState());

        // 当前失败原因摘要。
        String errorDetail = ExceptionDetailUtil.buildRootCauseDetail(throwable, 100);

        // 安排 失败 job 重启。
        scheduleFailedJobRestart(dataSourceId, failedJobId, failureEvent.getId(), errorDetail, restartState);
    }


    // 判断当前失败事件是否仍然对应 runtime 当前持有的 JobClient。
    private boolean isCurrentJobFailureEvent(String dataSourceId, String failedJobId) {
        // 获取当前 dataSourceId 的 JobClient。
        JobClient currentJobClient = oracleToKafka.getJobClient(dataSourceId);

        // 没有当前 JobClient 时，说明旧引用已经被清理，忽略该失败事件。
        if (currentJobClient == null) {
            return false;
        }

        // 获取当前 runtime 指向的 jobId。
        String currentJobId = currentJobClient.getJobID().toString();

        // 只有 failedJobId 与当前 jobId 一致时，才认为该失败事件有效。
        return failedJobId.equals(currentJobId);
    }

    // 调度失败 job 的自动重启任务。
    private void scheduleFailedJobRestart(String dataSourceId,
                                          String failedJobId,
                                          long failureEventId,
                                          String errorDetail,
                                          RestartState restartState) {
        // 本次重启延迟秒数。
        long delaySeconds = 0L;

        // 本次连续失败次数。
        int consecutiveFailures = 0;

        // 当前累计重启次数。
        long restartCountTotal = 0L;

        // 当前失败事件下的重启尝试次数。
        int attemptNo = 0;

        // 下次重启时间。
        LocalDateTime nextRestartTime = null;

        // 是否为重复失败事件。
        boolean duplicatedFailureEvent = false;



        // 对单个 dataSourceId 的重启状态加锁，避免重复调度。
        synchronized (restartState) {
            // 同一个 dataSourceId 已有待重启或正在重启任务时，只读取快照，不在锁内写库。
            // 这两个属性，初始值都是
            // 添加到调度线程之后，pendingRestart = true
            if (restartState.pendingRestart || restartState.restarting) {
                // 标记当前事件为重复失败事件。
                duplicatedFailureEvent = true;


            } else {
                // 当前失败事件连续失败次数加一。
                restartState.consecutiveFailures++;

                // 当前业务库累计重启次数加一。
                restartState.restartCountTotal++;

                // 保存最近失败 jobId。
                restartState.lastFailedJobId = failedJobId;

                // 保存最近失败原因。
                restartState.lastErrorDetail = errorDetail;

                // 保存最近失败时间。
                restartState.lastFailureTime = LocalDateTime.now();

                // 计算指数退避延迟。
                delaySeconds = calculateRestartDelaySeconds(restartState.consecutiveFailures);

                // 保存本次延迟。
                restartState.currentDelaySeconds = delaySeconds;

                // 计算下次重启时间。
                nextRestartTime = LocalDateTime.now().plusSeconds(delaySeconds);

                // 标记已有待重启任务。
                restartState.pendingRestart = true;

                // 读取当前连续失败次数。
                consecutiveFailures = restartState.consecutiveFailures;

                // 读取当前累计重启次数。
                restartCountTotal = restartState.restartCountTotal;

                // 当前失败事件下的尝试次数使用连续失败次数。
                attemptNo = consecutiveFailures;
            }
        }

        // 重复事件在锁外写处理记录。
        if (duplicatedFailureEvent) {
            // 写入重复失败事件忽略处理记录。
            insertSimpleJobFailureHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    JobFailureHandleStage.DUPLICATED_EVENT_IGNORED,
                    "restart already pending or running"
            );
            // 记录重复事件忽略日志。
            logger.warn("Restart already pending or running, ignore duplicated failure event. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);

            // 结束处理。
            return;
        }

        // 构造等待重启状态详情。
        String detailInfo = buildWaitingRestartDetail(
                failedJobId,
                consecutiveFailures,
                restartCountTotal,
                delaySeconds,
                nextRestartTime,
                errorDetail
        );

        // 写入 ZK：作业失败，等待自动重启。
        updateJobStatusQuietly(dataSourceId, JobStatus.FAILED_WAITING_RESTART, detailInfo);

        // 写入已安排自动重启处理记录。
        insertRestartScheduledHandleLogQuietly(
                buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                attemptNo,
                consecutiveFailures,
                restartCountTotal,
                delaySeconds,
                nextRestartTime,
                errorDetail,
                null
        );

        // 记录调度日志。
        logger.warn("失败的job，即将被重启： clientId={}, dataSourceId={}, failedJobId={}, delaySeconds= 延迟 {} 秒启动, consecutiveFailures= 连续失败 {} 次, restartCountTotal= 累计重启 {} 次",
                clientInfo.getClientId(), dataSourceId, failedJobId, delaySeconds, consecutiveFailures, restartCountTotal);

        // 按 backoff 延迟调度重启任务，不阻塞其它 dataSourceId。
        failedJobRestartScheduler.schedule(
                () -> executeScheduledFailedJobRestart(dataSourceId, failedJobId, failureEventId),
                delaySeconds,
                TimeUnit.SECONDS
        );
    }


    // 执行已调度的失败 job 重启任务。
    private void executeScheduledFailedJobRestart(String dataSourceId, String failedJobId, long failureEventId) {
        // 获取当前 dataSourceId 的重启状态。
        RestartState restartState = restartStatesByDataSourceId.get(dataSourceId);

        // 状态不存在时直接返回。
        if (restartState == null) {
            // 写入计划重启跳过处理记录。
            insertSimpleJobFailureHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    JobFailureHandleStage.SCHEDULED_RESTART_SKIPPED,
                    "restart state not found"
            );

            // 记录状态缺失日志。
            logger.warn("Restart state not found, skip scheduled restart. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);

            // 结束处理。
            return;
        }

        // 记录当前重启开始时间。
        LocalDateTime restartStartTime = LocalDateTime.now();

        // 对单个 dataSourceId 的重启状态加锁。
        synchronized (restartState) {
            // 待重启任务开始执行。
            restartState.pendingRestart = false;

            // 标记正在重启。
            restartState.restarting = true;
        }

        try {
            // 延迟任务真正执行前再次确认失败事件仍对应当前 runtime 的 JobClient。
            if (!shouldExecuteScheduledRestart(dataSourceId, failedJobId, restartState)) {
                // 写入计划重启跳过处理记录。
                insertSimpleJobFailureHandleLogQuietly(
                        buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                        JobFailureHandleStage.SCHEDULED_RESTART_SKIPPED,
                        "failed job is no longer current"
                );

                // 记录旧延迟任务被跳过的日志。
                logger.warn("Skip scheduled restart because failed job is no longer current. clientId={}, dataSourceId={}, failedJobId={}",
                        clientInfo.getClientId(), dataSourceId, failedJobId);

                // 直接返回，不清理 runtime，也不重启 job。
                return;
            }

            // 写入开始执行自动重启处理记录。
            insertRestartExecutionHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    currentAttemptNo(restartState),
                    JobFailureHandleStage.RESTART_STARTED,
                    currentConsecutiveFailures(restartState),
                    currentRestartCountTotal(restartState),
                    restartStartTime,
                    null,
                    null,
                    null,
                    null
            );

            // 构造正在重启状态详情。
            String detailInfo = buildRestartingDetail(dataSourceId, failedJobId, restartState);

            // 写入 ZK：作业正在自动重启。
            updateJobStatusQuietly(dataSourceId, JobStatus.RESTARTING, detailInfo);

            // 记录重启开始日志。
            logger.warn("失败的job正在被重启：clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);

            // 清理旧 job 的运行态引用，保留 runtime 稳定配置。
            oracleToKafka.cleanupFailedJobRuntime(dataSourceId, failedJobId);

            // 使用现有 runtime 重新构建并提交当前 dataSourceId 对应的 job。
            startRuntimeJobAfterFailure(dataSourceId, "auto restart after failed job, failedJobId=" + failedJobId);

            // 获取新 jobId，用于稳定运行检查。
            String newJobId = getCurrentJobId(dataSourceId);


            // 写入新 job 提交成功处理记录。
            insertRestartExecutionHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    currentAttemptNo(restartState),
                    JobFailureHandleStage.NEW_JOB_SUBMIT_SUCCEEDED,
                    currentConsecutiveFailures(restartState),
                    currentRestartCountTotal(restartState),
                    restartStartTime,
                    LocalDateTime.now(),
                    newJobId,
                    null,
                    null
            );

            // 安排稳定运行检查，稳定后清零连续失败次数。
            scheduleStableRunCheck(
                    dataSourceId,
                    newJobId,
                    failureEventId,
                    failedJobId,
                    currentAttemptNo(restartState)
            );

            // 记录重启完成日志。
            logger.info("失败的job重启完成： clientId={}, dataSourceId={}, failedJobId={}, newJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId, newJobId);
        } catch (Exception e) {
            // 构造重启失败原因摘要。
            String submitErrorDetail = ExceptionDetailUtil.buildRootCauseDetail(e, 1000);

            // 写入新 job 提交失败处理记录。
            insertRestartExecutionHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    currentAttemptNo(restartState),
                    JobFailureHandleStage.NEW_JOB_SUBMIT_FAILED,
                    currentConsecutiveFailures(restartState),
                    currentRestartCountTotal(restartState),
                    restartStartTime,
                    LocalDateTime.now(),
                    null,
                    submitErrorDetail,
                    null
            );

            // 重启任务本身失败时记录日志。
            logger.error("失败的job重启失败： clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId, e);

            // 重启提交阶段失败时不会产生新的 JobClient，也不会触发 watch，因此需要由调度器继续安排下一次重启。
            scheduleRestartAfterSubmitFailure(dataSourceId, failedJobId, failureEventId, e, restartState);
        } finally {
            // 无论成功失败，都清理正在重启标记。
            synchronized (restartState) {
                restartState.restarting = false;
            }
        }
    }


    // 判断已调度的重启任务是否仍然允许执行。
    private boolean shouldExecuteScheduledRestart(String dataSourceId, String failedJobId, RestartState restartState) {
        // failedJobId 为空时无法安全判断，直接拒绝执行。
        if (failedJobId == null || failedJobId.trim().isEmpty()) {
            return false;
        }

        // 获取当前 dataSourceId 绑定的 JobClient。
        JobClient currentJobClient = oracleToKafka.getJobClient(dataSourceId);

        // 如果当前仍然持有 JobClient，则必须确认它就是本次失败的 job。
        if (currentJobClient != null) {
            String currentJobId = currentJobClient.getJobID().toString();
            return failedJobId.equals(currentJobId);
        }

        // 如果 JobClient 已经被清理，说明可能是上一次重启提交阶段失败后的续重试。
        if (restartState == null) {
            return false;
        }

        // 在无 JobClient 的情况下，只允许同一个 failedJobId 对应的重启任务继续执行。
        synchronized (restartState) {
            return failedJobId.equals(restartState.lastFailedJobId);
        }
    }
    // 重启提交阶段失败后，继续安排下一次自动重启。
    private void scheduleRestartAfterSubmitFailure(String dataSourceId,
                                                   String failedJobId,
                                                   long failureEventId,
                                                   Exception submitException,
                                                   RestartState restartState) {
        // 构造提交失败原因摘要。
        String errorDetail = ExceptionDetailUtil.buildRootCauseDetail(submitException, 100);

        // 当前失败事件下的重启尝试次数。
        int attemptNo;
        // 本次延迟秒数。
        long delaySeconds;

        // 当前连续失败次数。
        int consecutiveFailures;

        // 当前累计重启次数。
        long restartCountTotal;

        // 下次重启时间。
        LocalDateTime nextRestartTime;

        // 对单个 dataSourceId 的重启状态加锁。
        synchronized (restartState) {
            // 这里把提交失败也计入连续失败次数，避免快速打爆系统。
            restartState.consecutiveFailures++;

            // 提交失败也计入累计重启次数。
            restartState.restartCountTotal++;

            // 保存最近失败 jobId。
            restartState.lastFailedJobId = failedJobId;

            // 保存最近错误详情。
            restartState.lastErrorDetail = errorDetail;

            // 保存最近失败时间。
            restartState.lastFailureTime = LocalDateTime.now();

            // 计算下一次指数退避延迟。
            delaySeconds = calculateRestartDelaySeconds(restartState.consecutiveFailures);

            // 保存当前退避秒数。
            restartState.currentDelaySeconds = delaySeconds;

            // 计算下次重启时间。
            nextRestartTime = LocalDateTime.now().plusSeconds(delaySeconds);

            // 标记已有待重启任务。
            restartState.pendingRestart = true;

            // 读取当前连续失败次数。
            consecutiveFailures = restartState.consecutiveFailures;

            // 读取当前累计重启次数。
            restartCountTotal = restartState.restartCountTotal;

            // 当前失败事件下的尝试次数。
            attemptNo = consecutiveFailures;
        }

        // 构造等待重启详情。
        String detailInfo = buildWaitingRestartDetail(
                failedJobId,
                consecutiveFailures,
                restartCountTotal,
                delaySeconds,
                nextRestartTime,
                errorDetail
        );

        // 写入 ZK：提交失败后继续等待自动重启。
        updateJobStatusQuietly(dataSourceId, JobStatus.FAILED_WAITING_RESTART, detailInfo);

        // 写入提交失败后再次等待重启处理记录。
        insertRestartScheduledHandleLogQuietly(
                buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                attemptNo,
                consecutiveFailures,
                restartCountTotal,
                delaySeconds,
                nextRestartTime,
                errorDetail,
                "rescheduled after restart submit failure"
        );

        // 记录重新调度日志。
        logger.warn("重启失败，将会被再次重启： clientId={}, dataSourceId={}, failedJobId={}, delaySeconds= 延迟 {} 秒启动, consecutiveFailures= 连续失败 {} 次, restartCountTotal= 累计重启 {} 次",
                clientInfo.getClientId(), dataSourceId, failedJobId, delaySeconds, consecutiveFailures, restartCountTotal);

        // 延迟调度下一次重启。
        failedJobRestartScheduler.schedule(
                () -> executeScheduledFailedJobRestart(dataSourceId, failedJobId, failureEventId),
                delaySeconds,
                TimeUnit.SECONDS
        );
    }

    // 计算指数退避延迟秒数，最大不超过 10 分钟。
    private long calculateRestartDelaySeconds(int consecutiveFailures) {
        // 防御非法失败次数。
        int safeFailures = Math.max(consecutiveFailures, 1);

        // 从第 7 次开始，10 * 2^6 = 640，已经超过 600，直接返回最大值。
        if (safeFailures >= 7) {
            return MAX_RESTART_DELAY_SECONDS;
        }

        // 计算指数退避延迟。
        long delaySeconds = INITIAL_RESTART_DELAY_SECONDS * (1L << (safeFailures - 1));

        // 返回不超过最大值的延迟。
        return Math.min(delaySeconds, MAX_RESTART_DELAY_SECONDS);
    }

    // 安排稳定运行检查。
    private void scheduleStableRunCheck(String dataSourceId,
                                        String expectedJobId,
                                        long failureEventId,
                                        String failedJobId,
                                        Integer attemptNo) {
        // 没有新 jobId 时无法做稳定检查。
        if (expectedJobId == null || expectedJobId.trim().isEmpty()) {
            logger.warn("Skip stable run check because expectedJobId is blank. clientId={}, dataSourceId={}",
                    clientInfo.getClientId(), dataSourceId);
            return;
        }

        // 延迟 stableRunSeconds 后检查 job 是否稳定运行。
        failedJobRestartScheduler.schedule(
                () -> checkStableRun(dataSourceId, expectedJobId, failureEventId, failedJobId, attemptNo),
                STABLE_RUN_SECONDS,
                TimeUnit.SECONDS
        );
    }

    // 检查新 job 是否已经稳定运行，只有确认仍是当前 job 且状态为 RUNNING 时才写入稳定通过记录。
    private void checkStableRun(String dataSourceId,
                                String expectedJobId,
                                long failureEventId,
                                String failedJobId,
                                Integer attemptNo) {
        try {
            // 获取当前 dataSourceId 对应的 JobClient。
            JobClient currentJobClient = oracleToKafka.getJobClient(dataSourceId);

            // 当前 JobClient 不存在时，说明运行时已经变化，不记录表 2。
            if (currentJobClient == null) {
                logger.warn("Skip stable run check because current JobClient is null. clientId={}, dataSourceId={}, expectedJobId={}",
                        clientInfo.getClientId(), dataSourceId, expectedJobId);
                return;
            }

            // 获取当前 runtime 持有的 jobId。
            String currentJobId = currentJobClient.getJobID().toString();

            // 当前 jobId 与期望的新 jobId 不一致时，说明已经发生新的运行时变化，不记录表 2。
            if (!expectedJobId.equals(currentJobId)) {
                logger.warn("Skip stable run check because current jobId changed. clientId={}, dataSourceId={}, expectedJobId={}, currentJobId={}",
                        clientInfo.getClientId(), dataSourceId, expectedJobId, currentJobId);
                return;
            }

            // 查询 Flink 当前 job 状态。
            org.apache.flink.api.common.JobStatus flinkJobStatus =
                    currentJobClient.getJobStatus().get(5, TimeUnit.SECONDS);

            // 只有 Flink 状态为 RUNNING，才认为稳定检查通过。
            if (flinkJobStatus != org.apache.flink.api.common.JobStatus.RUNNING) {
                logger.warn("Skip stable run check because current job is not RUNNING. clientId={}, dataSourceId={}, jobId={}, flinkJobStatus={}",
                        clientInfo.getClientId(), dataSourceId, expectedJobId, flinkJobStatus);
                return;
            }

            // 获取当前 dataSourceId 的重启状态。
            RestartState restartState = restartStatesByDataSourceId.get(dataSourceId);

            // 重启状态不存在时，说明状态已经被清理，不记录表 2。
            if (restartState == null) {
                logger.warn("Skip stable run check because restart state is null. clientId={}, dataSourceId={}, jobId={}",
                        clientInfo.getClientId(), dataSourceId, expectedJobId);
                return;
            }

            // 稳定通过前的连续失败次数快照。
            Integer consecutiveFailures;

            // 稳定通过前的累计重启次数快照。
            Long restartCountTotal;

            // 在同一个锁内完成状态保护、快照读取和连续失败状态清零。
            synchronized (restartState) {
                // 如果已经有新的待重启任务或正在重启任务，说明这次稳定检查已经过期，不写表 2，也不清零新状态。
                if (restartState.pendingRestart || restartState.restarting) {
                    logger.warn("Skip stable run check because restart is pending or running. clientId={}, dataSourceId={}, jobId={}, pendingRestart={}, restarting={}",
                            clientInfo.getClientId(), dataSourceId, expectedJobId, restartState.pendingRestart, restartState.restarting);
                    return;
                }

                // 读取连续失败次数快照。
                consecutiveFailures = restartState.consecutiveFailures;

                // 读取累计重启次数快照。
                restartCountTotal = restartState.restartCountTotal;

                // 清零连续失败次数。
                restartState.consecutiveFailures = 0;

                // 清零当前退避秒数。
                restartState.currentDelaySeconds = 0L;

                // 清空最近错误详情。
                restartState.lastErrorDetail = null;
            }

            // 写入稳定运行检查通过记录，其他稳定检查失败场景不写表 2。
            insertRestartExecutionHandleLogQuietly(
                    buildFailureEventRef(failureEventId, dataSourceId, failedJobId),
                    attemptNo,
                    JobFailureHandleStage.STABLE_CHECK_PASSED,
                    consecutiveFailures,
                    restartCountTotal,
                    null,
                    LocalDateTime.now(),
                    expectedJobId,
                    null,
                    null
            );

            // 记录稳定恢复日志。
            logger.info("Job is stable, consecutive failure count reset. clientId={}, dataSourceId={}, jobId={}, stableRunSeconds={}",
                    clientInfo.getClientId(), dataSourceId, expectedJobId, STABLE_RUN_SECONDS);
        } catch (Exception e) {
            // 稳定检查异常只记录日志，不写表 2。
            logger.warn("Stable run check failed. clientId={}, dataSourceId={}, expectedJobId={}",
                    clientInfo.getClientId(), dataSourceId, expectedJobId, e);
        }
    }

    // 获取当前 dataSourceId 对应的 jobId。
    private String getCurrentJobId(String dataSourceId) {
        // 获取当前 JobClient。
        JobClient jobClient = oracleToKafka.getJobClient(dataSourceId);

        // JobClient 不存在时返回 null。
        if (jobClient == null) {
            return null;
        }

        // 返回当前 jobId。
        return jobClient.getJobID().toString();
    }


    // 写 job 状态到 ZK，失败只记录日志。
    private void updateJobStatusQuietly(String dataSourceId, JobStatus jobStatus, String detailInfo) {
        // 打开当前 job 的 ZK 服务。
        ClientJobZkService jobZkService = null;

        try {
            // 获取 job 级 ZK 服务。
            jobZkService = ClientJobZkService.open(clientInfo, dataSourceId);

            // 直接写入状态数据，避免 JobStatus.withDetailInfo 修改 enum 单例。
            jobZkService.updateJobStatusData(buildJobStatusData(jobStatus, detailInfo));
        } catch (Exception e) {
            // ZK 状态写入失败时只记录日志，不阻断重启流程。
            logger.warn("Failed to update job restart status to ZK. clientId={}, dataSourceId={}, status={}, detailInfo={}",
                    clientInfo.getClientId(), dataSourceId, jobStatus, detailInfo, e);
        } finally {
            // 释放 job 级 ZK 服务。
            if (jobZkService != null) {
                jobZkService.close();
            }
        }
    }

    // 构造 job 状态数据，避免通过 JobStatus.withDetailInfo 修改 enum 单例。
    private Map<String, String> buildJobStatusData(JobStatus jobStatus, String detailInfo) {
        // 使用 LinkedHashMap 保持 ZK 内容字段顺序稳定，便于排查。
        Map<String, String> data = new LinkedHashMap<>();

        // 写入状态码。
        data.put("code", jobStatus.getCode());

        // 写入状态描述。
        data.put("description", jobStatus.getDescription());

        // 写入详情，null 时写空字符串。
        data.put("detailInfo", detailInfo == null ? "" : detailInfo);

        // 写入更新时间。
        data.put("updateTime", JsonUtil.formatNow());

        // 返回结构化状态。
        return data;
    }
    // 构造等待重启状态详情。
    private String buildWaitingRestartDetail(String failedJobId,
                                             int consecutiveFailures,
                                             long restartCountTotal,
                                             long delaySeconds,
                                             LocalDateTime nextRestartTime,
                                             String errorDetail) {
        // 拼接等待重启详情。
        return "failedJobId=" + failedJobId
                + ", consecutiveFailures=" + consecutiveFailures
                + ", restartCountTotal=" + restartCountTotal
                + ", nextRestartDelaySeconds=" + delaySeconds
                + ", nextRestartTime=" + nextRestartTime.format(RESTART_TIME_FORMATTER)
                + ", lastError=" + errorDetail;
    }

    // 构造正在重启状态详情。
    private String buildRestartingDetail(String dataSourceId, String failedJobId, RestartState restartState) {
        // 对单个 dataSourceId 的状态加锁，读取一致快照。
        synchronized (restartState) {
            // 拼接正在重启详情。
            return "dataSourceId=" + dataSourceId
                    + ", failedJobId=" + failedJobId
                    + ", consecutiveFailures=" + restartState.consecutiveFailures
                    + ", restartCountTotal=" + restartState.restartCountTotal
                    + ", lastError=" + restartState.lastErrorDetail;
        }
    }

    // 单个 dataSourceId 的失败重启状态。
    private static class RestartState {
        // 连续失败次数，稳定运行后清零。
        private int consecutiveFailures;

        // 累计重启次数，稳定运行后不清零。
        private long restartCountTotal;

        // 是否已有待执行的延迟重启任务。
        private boolean pendingRestart;

        // 是否正在执行重启。
        private boolean restarting;

        // 最近失败的 jobId。
        private String lastFailedJobId;

        // 最近失败时间。
        private LocalDateTime lastFailureTime;

        // 最近失败原因摘要，稳定运行后清零。
        private String lastErrorDetail;

        // 当前退避延迟秒数，稳定运行后清零。
        private long currentDelaySeconds;


        public RestartState() {
        }

    }








    /**
     * 调试方法
     *
     * 用途：
     * 1. 打印关键类的实际加载位置
     * 2. 排查 Debezium / Oracle CDC 相关 jar 冲突
     */
    public void debug() {
        logger.info("CLASSPATH = {}", System.getProperty("java.class.path"));
        logWhere(io.debezium.connector.oracle.logminer.LogMinerAdapter.class, "Which-LogMinerAdapter");
        logWhere(io.debezium.connector.oracle.StreamingAdapter.class, "Which-StreamingAdapter");
        logWhere(io.debezium.pipeline.EventDispatcher.class, "Which-EventDispatcher");
    }

    private void logWhere(Class<?> c, String tag) {
        try {
            logger.info("{} -> {} @ {}", tag, c.getName(), c.getProtectionDomain().getCodeSource().getLocation());
        } catch (Throwable t) {
            logger.info("{} -> {} @ <unknown>", tag, c.getName());
        }
    }


    /**
     * 处理配置加载结果，并将其映射为 client 进程级状态
     *
     * 设计说明：
     * 1. OracleToKafka 负责判断“缺了什么配置”，并提供 detailInfo
     * 2. ClientJobDispatcher 负责决定如何映射到 ClientProcessStatus
     * 3. 当前阶段，配置缺失统一映射为 ClientProcessStatus.CONFIG_MISSING
     * 4. detailInfo 不再在这里拼接，而是直接使用 OracleToKafka 返回的内容
     *
     * @param result OracleToKafka 返回的配置加载结果
     */
    public void handleLoadConfigResult(LoadConfigResult result) throws Exception {
        if (result == null) {
            return;
        }

        LoadConfigResultType type = result.getType();

        if (type == null) {
            logger.warn("LoadConfigResult type is null: {}", result);

            if (clientProcessZkService != null) {
                clientProcessZkService.updateStatus(
                        ClientProcessStatus.ERROR.withDetailInfo("配置加载结果类型为空")
                );
            }
            return;
        }

        switch (type) {
            case SUCCESS:
                // 配置已完整，不在这里写 RUNNING。
                //
                // 说明：
                // 1. SUCCESS 只表示“配置已具备”
                // 2. 不代表整个 client 已经完成 job 提交
                // 3. RUNNING 应由完整启动流程结束后统一写入
                return;

            case BUSINESS_DATA_SOURCE_MISSING:
            case SUBSCRIBER_MAPPING_MISSING:
            case TARGET_DATA_SOURCE_MISSING:
                // 当前 client 进程仍然存活，但缺少必要配置，无法进入正常运行态。
                // 具体缺失原因由 OracleToKafka 提供 detailInfo。
                if (clientProcessZkService != null) {
                    clientProcessZkService.updateStatus(
                            ClientProcessStatus.CONFIG_MISSING.withDetailInfo(result.getDetailInfo())
                    );
                }
                return;

            default:
                // 理论上不应该进入这里。
                // 如果进入，说明有新的 LoadConfigResultType 分支没有被映射。
                logger.warn("Unhandled LoadConfigResult: {}", result);

                if (clientProcessZkService != null) {
                    clientProcessZkService.updateStatus(
                            ClientProcessStatus.ERROR.withDetailInfo(
                                    "出现未处理的配置加载结果类型: " + type
                            )
                    );
                }
        }
    }






    /**
     * Spring 容器关闭前执行
     *
     * 用于：
     * 1. 写入 client 进程级状态：STOPPING
     * 2. 停掉所有 job
     * 3. 关闭共享 MiniCluster
     * 4. 尽量写入 client 进程级状态：STOPPED
     * 5. 释放 client 进程级 ZK 服务
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down ClientJobDispatcher, clientId={}", clientInfo.getClientId());

        // 1. 进入停止中状态
        try {
            if (clientProcessZkService != null) {
                clientProcessZkService.updateStatus(ClientProcessStatus.STOPPING);
            }
        } catch (Exception e) {
            logger.warn("Failed to update client process status to STOPPING", e);
        }

        // 2. 清理 OracleToKafka 状态
        try {
            oracleToKafka.clearState();
        } catch (Exception e) {
            logger.warn("Failed to clear OracleToKafka state", e);
        }

        // 3. 关闭共享 MiniCluster
        try {
            if (sharedMiniClusterManager != null) {
                sharedMiniClusterManager.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close SharedMiniClusterManager", e);
        }

        // 4. 尽量写入 STOPPED
        //
        // 注意：
        // 如果 JVM 很快退出，STOPPED 状态不一定总能成功落到 ZK。
        try {
            if (clientProcessZkService != null) {
                clientProcessZkService.updateStatus(ClientProcessStatus.STOPPED);
            }
        } catch (Exception e) {
            logger.warn("Failed to update client process status to STOPPED", e);
        }

        // 5. 释放 client 进程级 ZK 服务
        try {
            if (clientProcessZkService != null) {
                clientProcessZkService.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close ClientProcessZkService", e);
        }

        // 关闭失败重启线程池。
        try {
            failedJobRestartScheduler.shutdownNow();
        } catch (Exception e) {
            logger.warn("Failed to shutdown failed job restart scheduler", e);
        }
    }

    /**
     * 获取客户端配置
     */
    public ClientInfo getClientInfo() {
        return clientInfo;
    }

    /**
     * 获取 OracleToKafka 执行器
     */
    public OracleToKafka getOracleToKafka() {
        return oracleToKafka;
    }

    /**
     * 获取共享 MiniCluster 管理器
     *
     * 说明：
     * OracleToKafka 后续会通过它访问共享 MiniCluster。
     */
    public SharedMiniClusterManager getSharedMiniClusterManager() {
        return sharedMiniClusterManager;
    }


    /**
     * 获取 client 进程级 ZK 服务
     */
    public ClientProcessZkService getClientProcessZkService() {
        return clientProcessZkService;
    }






    // 使用现有 runtime 重新提交指定 dataSourceId 的 job。
    private void startRuntimeJobAfterFailure(String dataSourceId, String reason) throws Exception {
        // 记录自动重启提交开始日志。
        logger.info("Start runtime job after failure. clientId={}, dataSourceId={}, reason={}",
                clientInfo.getClientId(), dataSourceId, reason);

        // 获取当前 dataSourceId 对应的 runtime。
        ClientJobRuntime runtime = oracleToKafka.requireRuntime(dataSourceId);

        // 重启前从 CDC_DATA_SOURCE_RUN_STATE 刷新启动模式，避免 snapshot 完成后仍按 SNAPSHOT 重启。
        oracleToKafka.refreshRuntimeStartupMode(runtime);

        // 使用刷新后的 runtime 重新构建并提交 job。
        oracleToKafka.startSingleDataSourceJob(runtime);

        // 记录自动重启提交完成日志。
        logger.info("Runtime job submitted after failure. clientId={}, dataSourceId={}, startupMode={}, reason={}",
                clientInfo.getClientId(), dataSourceId, runtime.getStartupMode(), reason);
    }



    // 安静插入作业失败事件，失败只记录日志，不阻断自动重启。
    private JobFailureEvent insertJobFailureEventQuietly(String dataSourceId,
                                                         String failedJobId,
                                                         Throwable throwable,
                                                         JobFailureEventResult eventResult,
                                                         String ignoreReason) {
        // 构造失败事件对象。
        JobFailureEvent event = buildJobFailureEvent(dataSourceId, failedJobId, throwable, eventResult, ignoreReason);

        // 仓库未初始化时直接返回事件对象。
        if (jobFailureEventRepository == null) {
            logger.warn("JobFailureEventRepository is null, skip failure event insert. clientId={}, dataSourceId={}, failedJobId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId);
            return event;
        }

        try {
            // 插入失败事件。
            jobFailureEventRepository.insert(event);
        } catch (Exception e) {
            // 插入失败只记录日志，不影响重启主流程。
            logger.warn("持久化 job failure event 失败. clientId={}, dataSourceId={}, failedJobId={}, eventId={}",
                    clientInfo.getClientId(), dataSourceId, failedJobId, event.getId(), e);
        }

        // 返回事件对象，后续表2可以关联使用。
        return event;
    }


    // 构造作业失败事件对象。
    private JobFailureEvent buildJobFailureEvent(String dataSourceId,
                                                 String failedJobId,
                                                 Throwable throwable,
                                                 JobFailureEventResult eventResult,
                                                 String ignoreReason) {
        // 创建事件对象。
        JobFailureEvent event = new JobFailureEvent();

        // 设置程序生成主键。
        event.setId(IdGenerateUtil.nextId());

        // 设置客户端ID。
        event.setClientId(normalizeRequiredText(clientInfo.getClientId(), "UNKNOWN_CLIENT"));

        // 设置业务数据源ID，避免Oracle把空字符串当成NULL导致非空字段失败。
        event.setDataSourceId(normalizeRequiredText(dataSourceId, "UNKNOWN_DATA_SOURCE"));

        // 设置失败jobId，避免Oracle把空字符串当成NULL导致非空字段失败。
        event.setFailedJobId(normalizeRequiredText(failedJobId, "UNKNOWN_JOB"));

        // 设置失败发生时间。
        event.setFailureTime(LocalDateTime.now());

        // 设置Flink状态，当前回调只在确认FAILED后进入。
        event.setFlinkStatus("FAILED");

        // 设置失败原因摘要。
        event.setFailureReason(ExceptionDetailUtil.buildRootCauseDetail(throwable, 1000));

        // 设置完整异常详情。
        event.setFailureDetail(StackTraceUtils.getSafeStackTraceHead(throwable, Integer.MAX_VALUE, 1000000));



         // 设置最终事件校验结果。
        event.setEventResult(eventResult);

        // 设置忽略原因。
        event.setIgnoreReason(ignoreReason);

        // 返回事件对象。
        return event;
    }

    // 标准化非空入库文本，避免Oracle空字符串被当成NULL。
    private String normalizeRequiredText(String value, String fallbackValue) {
        // 空值或空白字符串使用兜底值。
        if (value == null || value.trim().isEmpty()) {
            return fallbackValue;
        }

        // 返回去除首尾空白后的值。
        return value.trim();
    }

    // 安静插入忽略类失败处理记录，失败只记录日志，不阻断主流程。
    private void insertIgnoredJobFailureHandleLogQuietly(JobFailureEvent failureEvent,
                                                         JobFailureHandleStage handleStage,
                                                         String ignoreReason) {
        // 复用通用处理记录插入方法，忽略类记录不涉及重启相关字段。
        insertSimpleJobFailureHandleLogQuietly(
                failureEvent,
                handleStage,
                ignoreReason
        );
    }









    // 安静插入简单失败处理记录，适用于只需要记录阶段和备注的处理场景。
    private void insertSimpleJobFailureHandleLogQuietly(JobFailureEvent failureEvent,
                                                        JobFailureHandleStage handleStage,
                                                        String remark) {
        // 复用完整参数版本，简单处理记录不写重启相关字段。
        insertJobFailureHandleLogQuietly(
                failureEvent,
                null,
                handleStage,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                remark
        );
    }

    // 安静插入重启调度处理记录，适用于已经计算出退避时间并安排下一次重启的场景。
    private void insertRestartScheduledHandleLogQuietly(JobFailureEvent failureEvent,
                                                        Integer attemptNo,
                                                        Integer consecutiveFailures,
                                                        Long restartCountTotal,
                                                        Long restartDelaySeconds,
                                                        LocalDateTime nextRestartTime,
                                                        String errorDetail,
                                                        String remark) {
        // 复用完整参数版本，重启调度记录只写调度相关字段。
        insertJobFailureHandleLogQuietly(
                failureEvent,
                attemptNo,
                JobFailureHandleStage.RESTART_SCHEDULED,
                consecutiveFailures,
                restartCountTotal,
                restartDelaySeconds,
                nextRestartTime,
                null,
                null,
                null,
                errorDetail,
                remark
        );
    }

    // 安静插入重启执行处理记录，适用于重启开始、新 job 提交成功、新 job 提交失败、稳定检查通过等执行阶段。
    private void insertRestartExecutionHandleLogQuietly(JobFailureEvent failureEvent,
                                                        Integer attemptNo,
                                                        JobFailureHandleStage handleStage,
                                                        Integer consecutiveFailures,
                                                        Long restartCountTotal,
                                                        LocalDateTime restartStartTime,
                                                        LocalDateTime restartEndTime,
                                                        String newJobId,
                                                        String errorDetail,
                                                        String remark) {
        // 复用完整参数版本，重启执行记录不写计划重启时间和退避秒数。
        insertJobFailureHandleLogQuietly(
                failureEvent,
                attemptNo,
                handleStage,
                consecutiveFailures,
                restartCountTotal,
                null,
                null,
                restartStartTime,
                restartEndTime,
                newJobId,
                errorDetail,
                remark
        );
    }





    // 安静插入失败处理记录，失败只记录日志，不阻断自动重启。
    private void insertJobFailureHandleLogQuietly(JobFailureEvent failureEvent,
                                                  Integer attemptNo,
                                                  JobFailureHandleStage handleStage,
                                                  Integer consecutiveFailures,
                                                  Long restartCountTotal,
                                                  Long restartDelaySeconds,
                                                  LocalDateTime nextRestartTime,
                                                  LocalDateTime restartStartTime,
                                                  LocalDateTime restartEndTime,
                                                  String newJobId,
                                                  String errorDetail,
                                                  String remark) {
        // 失败事件为空时直接返回。
        if (failureEvent == null) {
            return;
        }

        // 处理阶段为空时直接返回。
        if (handleStage == null) {
            return;
        }

        // 仓库未初始化时直接返回。
        if (jobFailureHandleLogRepository == null) {
            return;
        }

        try {
            // 创建处理记录。
            JobFailureHandleLog log = new JobFailureHandleLog();

            // 设置主键ID。
            log.id = IdGenerateUtil.nextId();

            // 设置失败事件ID。
            log.failureEventId = failureEvent.getId();

            // 设置客户端ID。
            log.clientId = normalizeRequiredText(clientInfo.getClientId(), "UNKNOWN_CLIENT");

            // 设置业务数据源ID。
            log.dataSourceId = normalizeRequiredText(failureEvent.getDataSourceId(), "UNKNOWN_DATA_SOURCE");

            // 设置失败Job ID。
            log.failedJobId = normalizeRequiredText(failureEvent.getFailedJobId(), "UNKNOWN_JOB");

            // 设置重启尝试次数。
            log.attemptNo = attemptNo;

            // 设置处理阶段。
            log.handleStage = handleStage;

            // 设置处理时间。
            log.handleTime = LocalDateTime.now();

            // 设置连续失败次数。
            log.consecutiveFailures = consecutiveFailures;

            // 设置累计重启次数。
            log.restartCountTotal = restartCountTotal;

            // 设置退避延迟秒数。
            log.restartDelaySeconds = restartDelaySeconds;

            // 设置下次计划重启时间。
            log.nextRestartTime = nextRestartTime;

            // 设置重启开始时间。
            log.restartStartTime = restartStartTime;

            // 设置重启结束时间。
            log.restartEndTime = restartEndTime;

            // 设置新Job ID。
            log.newJobId = newJobId;

            // 设置异常详情。
            log.errorDetail = errorDetail;

            // 设置备注。
            log.remark = remark;

            // 插入处理记录。
            jobFailureHandleLogRepository.insert(log);
        } catch (Exception e) {
            // 插入失败只记录日志，不影响主流程。
            logger.warn("Failed to insert job failure handle log. clientId={}, dataSourceId={}, failedJobId={}, stage={}",
                    clientInfo.getClientId(), failureEvent.getDataSourceId(), failureEvent.getFailedJobId(), handleStage.getCode(), e);
        }
    }

    // 构造只用于关联表2的失败事件引用。
    private JobFailureEvent buildFailureEventRef(long failureEventId, String dataSourceId, String failedJobId) {
        // 创建失败事件引用对象。
        JobFailureEvent event = new JobFailureEvent();

        // 设置失败事件ID。
        event.setId(failureEventId);

        // 设置客户端ID。
        event.setClientId(normalizeRequiredText(clientInfo.getClientId(), "UNKNOWN_CLIENT"));

        // 设置业务数据源ID。
        event.setDataSourceId(normalizeRequiredText(dataSourceId, "UNKNOWN_DATA_SOURCE"));

        // 设置失败Job ID。
        event.setFailedJobId(normalizeRequiredText(failedJobId, "UNKNOWN_JOB"));

        // 返回失败事件引用对象。
        return event;
    }

    // 获取当前尝试次数。
    private Integer currentAttemptNo(RestartState restartState) {
        // 状态为空时返回空。
        if (restartState == null) {
            return null;
        }

        // 加锁读取一致状态。
        synchronized (restartState) {
            // 尝试次数使用连续失败次数。
            return restartState.consecutiveFailures <= 0 ? null : restartState.consecutiveFailures;
        }
    }

    // 获取当前连续失败次数。
    private Integer currentConsecutiveFailures(RestartState restartState) {
        // 状态为空时返回空。
        if (restartState == null) {
            return null;
        }

        // 加锁读取一致状态。
        synchronized (restartState) {
            // 返回当前连续失败次数。
            return restartState.consecutiveFailures;
        }
    }

    // 获取当前累计重启次数。
    private Long currentRestartCountTotal(RestartState restartState) {
        // 状态为空时返回空。
        if (restartState == null) {
            return null;
        }

        // 加锁读取一致状态。
        synchronized (restartState) {
            // 返回当前累计重启次数。
            return restartState.restartCountTotal;
        }
    }
}