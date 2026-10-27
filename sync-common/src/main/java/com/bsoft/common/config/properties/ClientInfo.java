package com.bsoft.common.config.properties;

import com.bsoft.common.enums.JobStartupMode;
import com.bsoft.common.enums.KafkaTopicCleanMode;
import com.bsoft.common.enums.KafkaWriteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;

/**
 * 客户端配置对象
 *
 * 说明：
 * 1. 对应 application.yml 中的 client-info 前缀配置
 * 2. 当前版本只服务于“一个 sync-client 进程承载多个业务库 job”的模型
 * 3. 不再兼容旧版单 job 路径
 *
 * 路径模型：
 * 1. 客户端级路径
 *    - /clients/{clientId}/status
 *    - /clients/{clientId}/ip
 *    - /clients/{clientId}/jobs
 *
 * 2. job 级路径（每个业务库一个 job）
 *    - /clients/{clientId}/jobs/{dataSourceId}/status
 *    - /clients/{clientId}/jobs/{dataSourceId}/signals
 *    - /clients/{clientId}/jobs/{dataSourceId}/scn
 */
@Component
@ConfigurationProperties(prefix = "client-info")
public class ClientInfo extends SimpleClientInfo implements Serializable {

    private static final Logger logger = LogManager.getLogger(ClientInfo.class);


    /**
     * 默认启动模式
     *
     * 注意：
     * 1. 这是客户端级默认值
     * 2. 在多 job 场景下，最终应由每个业务库自己的 scopedExecutionPlan 推导
     * 3. 该字段保留，主要用于配置承接和默认语义表达
     */
    private JobStartupMode jobStartupMode;

    /**
     * 是否检查 Oracle CDC 前置条件
     */
    private boolean oracleCdcPreRequisites;

    /**
     * 客户端级状态路径
     *
     * 路径：
     * /clients/{clientId}/status
     *
     * 说明：
     * 这是客户端进程级状态，不是具体某个业务库 job 的状态。
     */
    private String clientStatusPath;

    /**
     * 客户端级 IP 路径
     *
     * 路径：
     * /clients/{clientId}/ip
     */
    private String clientIpPath;

    /**
     * 客户端下所有 job 的根路径
     *
     * 路径：
     * /clients/{clientId}/jobs
     */
    private String clientJobsRootPath;

    /**
     * client 存活探针路径。
     */
    private String clientAlivePath;


    // Kafka 写入模式配置，支持 parallel / ordered
    private String kafkaWriteMode = "parallel";


    // snapshot=last 控制信号出现后，延迟多少秒再标记业务库快照完成
    private long snapshotCompleteMarkDelaySeconds = 60L;

    /**
     * SNAPSHOT 模式启动前是否自动删除并重建当前业务库对应的 Kafka topics。
     */
    private boolean cleanTopicsBeforeSnapshot = false;

    // SNAPSHOT 模式启动前清理 Kafka topic 的方式
    private String cleanTopicsBeforeSnapshotMode = "recreate-topic";

    /**
     * SNAPSHOT 模式清理 Kafka topics 的最大等待分钟数。
     */
    private long cleanTopicsBeforeSnapshotTimeoutMinutes = 60L;

    /**
     * SNAPSHOT 模式清理 Kafka topics 的重试间隔秒数。
     */
    private long cleanTopicsBeforeSnapshotRetryIntervalSeconds = 5L;

    public ClientInfo() {
        logger.info("ClientInfo initialized for multi-datasource mode");
    }


    /**
     * 危险操作执行前的倒计时秒数。
     *
     * 用途：
     * 1. 给运维人员一个观察实时日志、手动终止进程的窗口
     * 2. 避免删除 topic、清理状态等危险动作在启动后立即执行
     *
     * 说明：
     * 1. 该倒计时不是交互式确认
     * 2. 倒计时结束后会自动继续执行
     * 3. 设置为 0 表示跳过倒计时
     */
    private long dangerousOperationCountdownSeconds = 60L;

    /**
     * 危险操作倒计时日志提示间隔秒数。
     *
     * 例如：
     * countdown=60, interval=10 时，会每 10 秒输出一次剩余时间。
     */
    private long dangerousOperationCountdownLogIntervalSeconds = 10L;
    /**
     * 初始化客户端级固定路径
     *
     * 注意：
     * 1. 这里只初始化客户端级路径
     * 2. 业务库 job 级路径依赖 dataSourceId，因此通过方法动态生成
     */
    @PostConstruct
    public void init() {
        // 初始化 client 进程状态节点路径。
        this.clientStatusPath = "/clients/" + getClientId() + "/status";

        // 初始化 client IP 节点路径。
        this.clientIpPath = "/clients/" + getClientId() + "/ip";

        // 初始化 client jobs 根节点路径。
        this.clientJobsRootPath = "/clients/" + getClientId() + "/jobs";

        // 初始化 client 存活临时节点路径。
        this.clientAlivePath = "/clients/" + getClientId() + "/alive";
    }

    // -------------------------------------------------------------------------
    // job 级路径生成方法
    // -------------------------------------------------------------------------

    /**
     * 获取指定业务库 job 的根路径
     *
     * 路径：
     * /clients/{clientId}/jobs/{dataSourceId}
     */
    public String getJobRootPath(String dataSourceId) {
        return clientJobsRootPath + "/" + dataSourceId;
    }

    /**
     * 获取指定业务库 job 的状态路径
     *
     * 路径：
     * /clients/{clientId}/jobs/{dataSourceId}/status
     */
    public String getJobStatusPath(String dataSourceId) {
        return getJobRootPath(dataSourceId) + "/status";
    }

    /**
     * 获取指定业务库 job 的停止信号路径
     *
     * 路径：
     * /clients/{clientId}/jobs/{dataSourceId}/signals
     */
    public String getJobStopSignalPath(String dataSourceId) {
        return getJobRootPath(dataSourceId) + "/signals";
    }

    /**
     * 获取指定业务库 job 的 SCN 路径
     *
     * 路径：
     * /clients/{clientId}/jobs/{dataSourceId}/scn
     */
    public String getJobScnPath(String dataSourceId) {
        return getJobRootPath(dataSourceId) + "/scn";
    }

    /**
     * 获取指定业务库 job 的 active 路径
     *
     * 路径：
     * /clients/{clientId}/jobs/{dataSourceId}/alive
     */
    public String getJobAlivePath(String dataSourceId) {
        return getJobRootPath(dataSourceId) + "/alive";
    }

    // -------------------------------------------------------------------------
    // Getter / Setter
    // -------------------------------------------------------------------------


    public JobStartupMode getJobStartupMode() {
        return jobStartupMode;
    }

    public void setJobStartupMode(JobStartupMode jobStartupMode) {
        this.jobStartupMode = jobStartupMode;
    }

    public boolean isOracleCdcPreRequisites() {
        return oracleCdcPreRequisites;
    }

    public void setOracleCdcPreRequisites(boolean oracleCdcPreRequisites) {
        this.oracleCdcPreRequisites = oracleCdcPreRequisites;
    }

    public String getClientStatusPath() {
        return clientStatusPath;
    }

    public void setClientStatusPath(String clientStatusPath) {
        this.clientStatusPath = clientStatusPath;
    }

    public String getClientIpPath() {
        return clientIpPath;
    }

    public void setClientIpPath(String clientIpPath) {
        this.clientIpPath = clientIpPath;
    }

    public String getClientJobsRootPath() {
        return clientJobsRootPath;
    }

    public void setClientJobsRootPath(String clientJobsRootPath) {
        this.clientJobsRootPath = clientJobsRootPath;
    }

    // 获取 Kafka 写入模式原始配置
    public String getKafkaWriteMode() {
        // 返回原始配置值
        return kafkaWriteMode;
    }

    // 设置 Kafka 写入模式原始配置
    public void setKafkaWriteMode(String kafkaWriteMode) {
        // 保存原始配置值
        this.kafkaWriteMode = kafkaWriteMode;
    }

    // 解析 Kafka 写入模式枚举
    public KafkaWriteMode resolveKafkaWriteMode() {
        // 将配置字符串解析为枚举
        return KafkaWriteMode.fromConfig(kafkaWriteMode);
    }


    // 获取 snapshot 完成标记延迟秒数
    public long getSnapshotCompleteMarkDelaySeconds() {
        return snapshotCompleteMarkDelaySeconds;
    }

    // 设置 snapshot 完成标记延迟秒数
    public void setSnapshotCompleteMarkDelaySeconds(long snapshotCompleteMarkDelaySeconds) {
        // 小于 0 的配置没有意义，兜底为 0
        this.snapshotCompleteMarkDelaySeconds = Math.max(snapshotCompleteMarkDelaySeconds, 0L);
    }

    /**
     * 判断 SNAPSHOT 模式启动前是否自动清理 Kafka topics。
     */
    public boolean isCleanTopicsBeforeSnapshot() {
        return cleanTopicsBeforeSnapshot;
    }

    /**
     * 设置 SNAPSHOT 模式启动前是否自动清理 Kafka topics。
     */
    public void setCleanTopicsBeforeSnapshot(boolean cleanTopicsBeforeSnapshot) {
        this.cleanTopicsBeforeSnapshot = cleanTopicsBeforeSnapshot;
    }

    /**
     * 获取 SNAPSHOT 模式清理 Kafka topics 的最大等待分钟数。
     */
    public long getCleanTopicsBeforeSnapshotTimeoutMinutes() {
        return cleanTopicsBeforeSnapshotTimeoutMinutes;
    }

    /**
     * 设置 SNAPSHOT 模式清理 Kafka topics 的最大等待分钟数。
     */
    public void setCleanTopicsBeforeSnapshotTimeoutMinutes(long cleanTopicsBeforeSnapshotTimeoutMinutes) {
        this.cleanTopicsBeforeSnapshotTimeoutMinutes = Math.max(cleanTopicsBeforeSnapshotTimeoutMinutes, 1L);
    }

    /**
     * 获取 SNAPSHOT 模式清理 Kafka topics 的重试间隔秒数。
     */
    public long getCleanTopicsBeforeSnapshotRetryIntervalSeconds() {
        return cleanTopicsBeforeSnapshotRetryIntervalSeconds;
    }

    /**
     * 设置 SNAPSHOT 模式清理 Kafka topics 的重试间隔秒数。
     */
    public void setCleanTopicsBeforeSnapshotRetryIntervalSeconds(long cleanTopicsBeforeSnapshotRetryIntervalSeconds) {
        this.cleanTopicsBeforeSnapshotRetryIntervalSeconds = Math.max(cleanTopicsBeforeSnapshotRetryIntervalSeconds, 1L);
    }




    public long getDangerousOperationCountdownSeconds() {
        return dangerousOperationCountdownSeconds;
    }

    public void setDangerousOperationCountdownSeconds(long dangerousOperationCountdownSeconds) {
        // 小于 0 没有实际意义，统一兜底为 0，表示不等待。
        this.dangerousOperationCountdownSeconds = Math.max(dangerousOperationCountdownSeconds, 0L);
    }

    public long getDangerousOperationCountdownLogIntervalSeconds() {
        return dangerousOperationCountdownLogIntervalSeconds;
    }

    public void setDangerousOperationCountdownLogIntervalSeconds(long dangerousOperationCountdownLogIntervalSeconds) {
        // 日志提示间隔至少为 1 秒，避免死循环或过密日志。
        this.dangerousOperationCountdownLogIntervalSeconds =
                Math.max(dangerousOperationCountdownLogIntervalSeconds, 1L);
    }


    /**
     * 获取 client 存活临时节点路径。
     */
    public String getClientAlivePath() {
        // 返回 /clients/{clientId}/alive。
        return clientAlivePath;
    }

    /**
     * 设置 client 存活临时节点路径。
     */
    public void setClientAlivePath(String clientAlivePath) {
        // 允许外部配置或测试覆盖路径。
        this.clientAlivePath = clientAlivePath;
    }

    // 获取 SNAPSHOT 模式启动前清理 Kafka topic 的原始配置
    public String getCleanTopicsBeforeSnapshotMode() {
        // 返回原始配置值
        return cleanTopicsBeforeSnapshotMode;
    }

    // 设置 SNAPSHOT 模式启动前清理 Kafka topic 的原始配置
    public void setCleanTopicsBeforeSnapshotMode(String cleanTopicsBeforeSnapshotMode) {
        // 保存原始配置值
        this.cleanTopicsBeforeSnapshotMode = cleanTopicsBeforeSnapshotMode;
    }

    // 解析 SNAPSHOT 模式启动前清理 Kafka topic 的方式
    public KafkaTopicCleanMode resolveCleanTopicsBeforeSnapshotMode() {
        // 将配置文本解析为枚举
        return KafkaTopicCleanMode.fromConfig(cleanTopicsBeforeSnapshotMode);
    }
}