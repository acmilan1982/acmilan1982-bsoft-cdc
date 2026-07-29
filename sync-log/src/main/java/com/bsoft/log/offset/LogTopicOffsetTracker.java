package com.bsoft.log.offset;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * 日志 topic offset 连续水位线追踪器。
 */
public class LogTopicOffsetTracker {

    /**
     * sync-log 实例 id。
     */
    private final String logId;

    /**
     * 日志 Kafka topic。
     */
    private final String kafkaTopic;

    /**
     * offset 仓储。
     */
    private final LogTopicOffsetRepository repository;

    /**
     * 当前可安全提交的 nextOffset。
     */
    private long nextCommitOffset;

    /**
     * 上一次已经持久化到数据库的 nextOffset。
     */
    private long lastPersistedOffset;

    /**
     * 已成功写库但尚未形成连续水位线的 offset 区间。
     */
    private final TreeMap<Long, Long> successRanges = new TreeMap<>();

    /**
     * 创建 offset 水位线追踪器。
     */
    public LogTopicOffsetTracker(String logId,
                                 String kafkaTopic,
                                 long initialNextOffset,
                                 LogTopicOffsetRepository repository) {
        // 保存 sync-log 实例 id
        this.logId = requireText(logId, "logId");

        // 保存日志 topic
        this.kafkaTopic = requireText(kafkaTopic, "kafkaTopic");

        // 初始化当前可提交 offset，最初始的状态是 0
        this.nextCommitOffset = Math.max(0L, initialNextOffset);

        // 初始化已持久化 offset
        this.lastPersistedOffset = this.nextCommitOffset;

        // 保存 offset 仓储
        this.repository = repository;
    }

    /**
     * 标记一个写库成功的 offset 区间。
     */
    public synchronized void markSuccessRange(long startOffset, long endOffset) {
        // 忽略非法区间。
        if (startOffset < 0L || endOffset < startOffset) {
            return;
        }

        // 已经低于当前水位线的区间不需要重复记录。
        if (endOffset < nextCommitOffset) {
            return;
        }

        // 将区间起点裁剪到当前水位线之后。
        long normalizedStart = Math.max(startOffset, nextCommitOffset);

        // 合并当前成功区间
        mergeSuccessRange(normalizedStart, endOffset);

        // 尝试推进连续水位线
        advanceContinuousWatermark();
    }


    /**
     * 尽量把当前连续水位线持久化到 CDC_LOG_TOPIC_OFFSET。
     */
    public synchronized void flush() throws Exception {
        // 水位线没有推进时不写库。
        if (nextCommitOffset <= lastPersistedOffset) {
            return;
        }

        // 保存即将持久化的 offset。
        long offsetToPersist = nextCommitOffset;

        // 更新当前 topic 的 nextOffset。
        repository.updateNextOffsets(
                logId,
                Collections.singletonMap(kafkaTopic, offsetToPersist)
        );

        // 更新已持久化 offset。
        lastPersistedOffset = offsetToPersist;
    }

    /**
     * 获取当前可安全提交的 nextOffset。
     */
    public synchronized long getNextCommitOffset() {
        // 返回当前连续水位线。
        return nextCommitOffset;
    }

    /**
     * 合并成功 offset 区间。
     */
    private void mergeSuccessRange(long startOffset, long endOffset) {
        //  找当前 新区间 左侧，是否有一个可以和它重叠或相邻的成功区间
        Map.Entry<Long, Long> floor = successRanges.floorEntry(startOffset);  // 小于等于 key 的最大 key 对应的 entry

        // 如果前一个区间与当前区间相邻或重叠，则合并。
        if (floor != null && floor.getValue() + 1 >= startOffset) {
            // 使用更早的起点。
            startOffset = floor.getKey();

            // 使用更大的终点。
            endOffset = Math.max(endOffset, floor.getValue());

            // 删除旧区间。
            successRanges.remove(floor.getKey());
        }

        // 合并后续所有相邻或重叠区间。
        Map.Entry<Long, Long> next = successRanges.ceilingEntry(startOffset);  // 大于等于 key 的最小 key 对应的 entry
        while (next != null && next.getKey() <= endOffset + 1) {
            // 扩大当前区间终点。
            endOffset = Math.max(endOffset, next.getValue());

            // 删除已合并区间。
            successRanges.remove(next.getKey());

            // 继续查找下一个区间。
            next = successRanges.ceilingEntry(startOffset);
        }

        // 写入合并后的区间。
        successRanges.put(startOffset, endOffset);
    }

    /**
     * 根据已成功区间推进连续水位线。
     */
    private void advanceContinuousWatermark() {
        // 循环尝试推进 nextCommitOffset。
        while (true) {
            // 查找覆盖当前 nextCommitOffset 的区间。
            Map.Entry<Long, Long> range = successRanges.floorEntry(nextCommitOffset);

            // 如果没有覆盖当前水位线的区间，则不能推进。
            if (range == null || range.getValue() < nextCommitOffset) {
                return;
            }

            // 推进到当前成功区间之后。
            nextCommitOffset = range.getValue() + 1;

            // 删除已经被水位线消费掉的区间。
            successRanges.remove(range.getKey());
        }
    }

    /**
     * 校验文本字段不能为空。
     */
    private String requireText(String value, String field) {
        // 空值或空白字符串都视为非法。
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }

        // 返回去除首尾空白后的值。
        return value.trim();
    }
}