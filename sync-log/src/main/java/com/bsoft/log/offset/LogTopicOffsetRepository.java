package com.bsoft.log.offset;

import java.util.Collection;
import java.util.Map;

/**
 * sync-log 日志 topic offset 仓储接口。
 */
public interface LogTopicOffsetRepository {

    /**
     * 读取指定 logId 下指定日志 topic 的 nextOffset，并初始化缺失记录。
     */
    Map<String, Long> loadNextOffsets(String logId, Collection<String> kafkaTopics) throws Exception;

    /**
     * 更新指定 logId 下多个日志 topic 的 nextOffset。
     */
    void updateNextOffsets(String logId, Map<String, Long> offsets) throws Exception;
}