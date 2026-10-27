package com.bsoft.client.safety;

import com.bsoft.common.bean.ClientTopicInfo;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.enums.KafkaTopicCleanMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// 危险操作倒计时工具类
public final class DangerousOperationCountdown {

    // 日志对象
    private static final Logger logger = LogManager.getLogger(DangerousOperationCountdown.class);

    // 禁止实例化工具类
    private DangerousOperationCountdown() {
        // 工具类不允许创建实例
    }

    // 删除并重建 Kafka topics 前执行倒计时
    public static void beforeRecreateKafkaTopics(ClientInfo clientInfo,
                                                 String dataSourceId,
                                                 List<ClientTopicInfo> topicInfos) {
        // 校验客户端配置不能为空
        Objects.requireNonNull(clientInfo, "clientInfo must not be null");

        // 提取 topic 名称列表
        List<String> topics = extractTopicNames(topicInfos);

        // 执行通用倒计时
        countdown(
                clientInfo,
                "删除并重建 Kafka topics",
                dataSourceId,
                topics
        );
    }

    // Kafka topic 清理前执行倒计时
    public static void beforeCleanKafkaTopics(ClientInfo clientInfo,
                                              String dataSourceId,
                                              List<ClientTopicInfo> topicInfos,
                                              KafkaTopicCleanMode cleanMode) {
        // 校验客户端配置不能为空
        Objects.requireNonNull(clientInfo, "clientInfo must not be null");

        // 校验清理方式不能为空
        Objects.requireNonNull(cleanMode, "cleanMode must not be null");

        // 提取 topic 名称列表
        List<String> topics = extractTopicNames(topicInfos);

        // 根据清理方式构造操作名称
        String operationName = buildKafkaTopicCleanOperationName(cleanMode);

        // 执行通用倒计时
        countdown(
                clientInfo,
                operationName,
                dataSourceId,
                topics
        );
    }

    // 提取 topic 名称列表
    private static List<String> extractTopicNames(List<ClientTopicInfo> topicInfos) {
        // 空列表直接返回空集合
        if (topicInfos == null || topicInfos.isEmpty()) {
            return Collections.emptyList();
        }

        // 提取非空 topic 并去重
        return topicInfos.stream()
                .filter(Objects::nonNull)
                .map(ClientTopicInfo::getTopic)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(topic -> !topic.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    // 根据 Kafka topic 清理方式构造操作名称
    private static String buildKafkaTopicCleanOperationName(KafkaTopicCleanMode cleanMode) {
        // 删除并重建 topic
        if (cleanMode == KafkaTopicCleanMode.RECREATE_TOPIC) {
            return "删除并重建 Kafka topics";
        }

        // 保留 topic 但清理数据
        if (cleanMode == KafkaTopicCleanMode.CLEAR_DATA) {
            return "清空 Kafka topic 数据并保留 topics";
        }

        // 未知清理方式直接返回兜底名称
        return "清理 Kafka topics";
    }

    // 执行通用危险操作倒计时
    private static void countdown(ClientInfo clientInfo,
                                  String operationName,
                                  String dataSourceId,
                                  List<String> targets) {
        // 读取倒计时秒数
        long countdownSeconds = clientInfo.getDangerousOperationCountdownSeconds();

        // 读取日志输出间隔秒数
        long logIntervalSeconds = clientInfo.getDangerousOperationCountdownLogIntervalSeconds();

        // 倒计时关闭时直接返回
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

        // 记录倒计时开始日志
        logger.warn(
                "即将在 {} 秒后执行危险操作. operation={}, clientId={}, dataSourceId={}, targets={}",
                countdownSeconds,
                operationName,
                clientInfo.getClientId(),
                dataSourceId,
                targets
        );

        // 初始化剩余秒数
        long remainingSeconds = countdownSeconds;

        // 循环等待直到倒计时结束
        while (remainingSeconds > 0L) {
            // 计算本轮睡眠秒数
            long sleepSeconds = Math.min(logIntervalSeconds, remainingSeconds);

            // 睡眠等待
            sleepSeconds(operationName, dataSourceId, sleepSeconds);

            // 扣减剩余秒数
            remainingSeconds -= sleepSeconds;

            // 输出倒计时中间日志
            logCountdownProgress(clientInfo, operationName, dataSourceId, remainingSeconds);
        }

        // 记录倒计时结束日志
        logger.warn(
                "危险操作倒计时结束，开始执行操作. operation={}, clientId={}, dataSourceId={}, targets={}",
                operationName,
                clientInfo.getClientId(),
                dataSourceId,
                targets
        );
    }

    // 睡眠指定秒数
    private static void sleepSeconds(String operationName,
                                     String dataSourceId,
                                     long sleepSeconds) {
        // 执行线程睡眠
        try {
            Thread.sleep(sleepSeconds * 1000L);
        } catch (InterruptedException e) {
            // 恢复中断标记
            Thread.currentThread().interrupt();

            // 中断时阻止后续危险操作
            throw new IllegalStateException(
                    "危险操作倒计时被中断，已取消后续操作. operation="
                            + operationName + ", dataSourceId=" + dataSourceId,
                    e
            );
        }
    }

    // 输出倒计时进度日志
    private static void logCountdownProgress(ClientInfo clientInfo,
                                             String operationName,
                                             String dataSourceId,
                                             long remainingSeconds) {
        // 倒计时结束后不再输出进度日志
        if (remainingSeconds <= 0L) {
            return;
        }

        // 输出剩余时间日志
        logger.warn(
                "危险操作倒计时中，剩余 {} 秒. operation={}, clientId={}, dataSourceId={}",
                remainingSeconds,
                operationName,
                clientInfo.getClientId(),
                dataSourceId
        );
    }
}