package com.bsoft.server.offset;

import java.util.Map;

/**
 * Topic offset 仓储接口。
 *
 * 约定：
 * 1. 表里保存的是 nextOffset，而不是 lastProcessedOffset
 * 2. key 维度是 serverId + kafkaTopic
 */
public interface TopicOffsetRepository {

    /**
     * 读取某个 server 当前保存的全部 topic -> nextOffset。
     */
    Map<String, Long> loadNextOffsets(String serverId) throws Exception;

    /**
     * 批量写入 topic -> nextOffset。
     */
    void upsertNextOffsets(String serverId, Map<String, Long> offsets) throws Exception;

    /**
     * 单条写入，便于调试或单 topic 更新。
     */
    void upsertNextOffset(String serverId, String kafkaTopic, long nextOffset) throws Exception;
}