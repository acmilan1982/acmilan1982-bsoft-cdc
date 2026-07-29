package com.bsoft.client.safety;

import com.bsoft.common.bean.ClientTopicInfo;
import com.bsoft.common.config.properties.ClientInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 危险操作倒计时工具。
 *
 * 设计目标：
 * 1. 在真正执行危险动作之前，给运维人员一个可观察、可中断的时间窗口
 * 2. 倒计时期间只打印日志，不做交互式确认
 * 3. 如果进程无人值守，倒计时结束后自动继续执行
 *
 * 典型危险操作：
 * 1. 删除并重建 Kafka topic
 * 2. 清理 checkpoint / savepoint
 * 3. 重置运行状态
 */
public final class DangerousOperationCountdown {

    private static final Logger logger = LogManager.getLogger(DangerousOperationCountdown.class);

    private DangerousOperationCountdown() {
        // 工具类不允许实例化。
    }

    /**
     * 在删除并重建 Kafka topics 前执行倒计时。
     *
     * @param clientInfo 当前客户端配置
     * @param dataSourceId 当前业务库 ID
     * @param topicInfos 即将被删除并重建的 topic 信息
     */
    public static void beforeRecreateKafkaTopics(ClientInfo clientInfo,
                                                 String dataSourceId,
                                                 List<ClientTopicInfo> topicInfos) {
        Objects.requireNonNull(clientInfo, "clientInfo must not be null");

        List<String> topics = topicInfos == null
                ? java.util.Collections.emptyList()
                : topicInfos.stream()
                .map(ClientTopicInfo::getTopic)
                .distinct()
                .collect(Collectors.toList());

        countdown(
                clientInfo,
                "删除并重建 Kafka topics",
                dataSourceId,
                topics
        );
    }

    /**
     * 执行通用危险操作倒计时。
     *
     * 说明：
     * 1. countdownSeconds <= 0 时直接跳过
     * 2. 每隔 logIntervalSeconds 打印一次剩余时间
     * 3. 如果线程被中断，恢复中断标记，并抛出异常阻止后续危险操作
     */
    private static void countdown(ClientInfo clientInfo,
                                  String operationName,
                                  String dataSourceId,
                                  List<String> targets) {
        long countdownSeconds = clientInfo.getDangerousOperationCountdownSeconds();
        long logIntervalSeconds = clientInfo.getDangerousOperationCountdownLogIntervalSeconds();

        if (countdownSeconds <= 0L) {
            logger.warn(
                    "危险操作倒计时已关闭，即将立即执行操作. operation={}, clientId={}, dataSourceId={}, targets={}",
                    operationName,
                    clientInfo.getClientId(),
                    dataSourceId,
                    targets
            );
            return;
        }

        logger.warn(
                "即将在 {} 秒后执行危险操作. operation={}, clientId={}, dataSourceId={}, targets={}",
                countdownSeconds,
                operationName,
                clientInfo.getClientId(),
                dataSourceId,
                targets
        );

        long remainingSeconds = countdownSeconds;

        while (remainingSeconds > 0L) {
            long sleepSeconds = Math.min(logIntervalSeconds, remainingSeconds);

            try {
                Thread.sleep(sleepSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                // 被中断时不要继续执行危险操作，让上层启动流程失败或退出。
                throw new IllegalStateException(
                        "危险操作倒计时被中断，已取消后续操作. operation="
                                + operationName + ", dataSourceId=" + dataSourceId,
                        e
                );
            }

            remainingSeconds -= sleepSeconds;

            if (remainingSeconds > 0L) {
                logger.warn(
                        "危险操作倒计时中，剩余 {} 秒. operation={}, clientId={}, dataSourceId={}",
                        remainingSeconds,
                        operationName,
                        clientInfo.getClientId(),
                        dataSourceId
                );
            }
        }

        logger.warn(
                "危险操作倒计时结束，开始执行操作. operation={}, clientId={}, dataSourceId={}, targets={}",
                operationName,
                clientInfo.getClientId(),
                dataSourceId,
                targets
        );
    }
}