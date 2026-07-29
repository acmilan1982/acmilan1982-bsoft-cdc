package com.bsoft.server.sink;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.server.offset.JdbcTopicOffsetRepository;
import com.bsoft.server.offset.TopicOffsetCommitEvent;
import com.bsoft.server.offset.TopicOffsetRepository;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 将 TopicOffsetCommitEvent 批量持久化到 CDC_TOPIC_OFFSET 表。
 */
public class TopicOffsetCommitJdbcSink extends RichSinkFunction<TopicOffsetCommitEvent> {

    // 日志记录器
    private static final Logger logger = LogManager.getLogger(TopicOffsetCommitJdbcSink.class);

//    // 单次批量刷库的 topic 数阈值
//    private static final int BATCH_SIZE = 1000;

    // 最长刷库时间间隔
    private static final long FLUSH_INTERVAL_MS = 10_000L;

    // 配置库连接配置
    private final ConfigDataSourceConfig configDataSourceConfig;

    // offset 持久化仓储
    private transient TopicOffsetRepository topicOffsetRepository;

    // 待刷新的 offset 缓冲，按 serverId 分组
    // key: serverId
    // value: key: topic
    //        value: next offset
    private transient Map<String, Map<String, Long>> pendingOffsetsByServerId;

    // 上次刷库时间
    private transient long lastFlushTs;

    /**
     * 构造 sink，并注入配置库连接参数。
     */
    public TopicOffsetCommitJdbcSink(ConfigDataSourceConfig configDataSourceConfig) {
        // 保存配置库连接配置
        this.configDataSourceConfig = configDataSourceConfig;
    }

    /**
     * 初始化仓储和内存缓冲。
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 先执行父类初始化
        super.open(parameters);

        // 初始化配置库 DataSource
        DataSource dataSource = configDataSourceConfig.initDataSource();

        // 创建 JDBC offset 仓储
        this.topicOffsetRepository = new JdbcTopicOffsetRepository(dataSource);

        // 初始化待刷库缓冲
        this.pendingOffsetsByServerId = new LinkedHashMap<>();

        // 初始化上次刷库时间
        this.lastFlushTs = System.currentTimeMillis();
    }

    /**
     * 接收单条 offset 事件，并按条件批量刷库。
     */
    @Override
    public synchronized void invoke(TopicOffsetCommitEvent value,        // 单个主题的 下次应开始读取的 offset
                                    Context context) throws Exception {
        // 获取当前 serverId 对应的 topic offset 缓冲
        Map<String, Long> offsetsByTopic = pendingOffsetsByServerId.computeIfAbsent(
                value.getServerId(),
                key -> new LinkedHashMap<>()
        );

        // 读取当前 topic 已缓冲的 nextOffset
        Long current = offsetsByTopic.get(value.getKafkaTopic());

        // 只保留更大的 nextOffset，避免内存中发生位点回退
        if (current == null || value.getNextOffset() > current) {
            offsetsByTopic.put(value.getKafkaTopic(), value.getNextOffset());
        }

        // 达到刷库条件时执行批量持久化
        if (shouldFlush()) {
            flushPendingOffsets();
        }
    }

    /**
     * 关闭时补做一次刷库，避免缓冲数据丢失。
     */
    @Override
    public synchronized void close() throws Exception {
        try {
            // 关闭前尽量把剩余 offset 全部落库
            flushPendingOffsets();
        } finally {
            // 最后执行父类关闭逻辑
            super.close();
        }
    }

    /**
     * 判断当前是否需要刷库。
     */
    private boolean shouldFlush() {
        // 统计当前待刷库的 topic 数
        int pendingTopicCount = 0;
        for (Map<String, Long> offsetsByTopic : pendingOffsetsByServerId.values()) {
            // 累加每个 server 下的 topic 数
            pendingTopicCount += offsetsByTopic.size();
        }

        // 获取当前时间
        long now = System.currentTimeMillis();

//        // 满足数量阈值或时间阈值就触发刷库
//        return pendingTopicCount >= BATCH_SIZE
//                || (pendingTopicCount > 0 && now - lastFlushTs >= FLUSH_INTERVAL_MS);
        return  (pendingTopicCount > 0 && now - lastFlushTs >= FLUSH_INTERVAL_MS);
    }

    /**
     * 将内存中的 offset 缓冲批量写入数据库。
     */
    private void flushPendingOffsets() throws Exception {
        // 没有待刷库数据时直接返回
        if (pendingOffsetsByServerId == null || pendingOffsetsByServerId.isEmpty()) {
            return;
        }

        // 遍历每个 serverId 的 offset 缓冲
        for (Map.Entry<String, Map<String, Long>> entry : pendingOffsetsByServerId.entrySet()) {
            // 取出 serverId
            String serverId = entry.getKey();

            // 取出该 serverId 下所有 topic 的 nextOffset
            Map<String, Long> offsetsByTopic = entry.getValue();

            // 当前 server 没有待刷数据则跳过
            if (offsetsByTopic == null || offsetsByTopic.isEmpty()) {
                continue;
            }

            // 批量 upsert 当前 server 的 topic offset
            topicOffsetRepository.upsertNextOffsets(serverId, offsetsByTopic);

            // 记录刷库日志
            logger.info("Persisted topic offsets, serverId={}, topicCount={}", serverId, offsetsByTopic.size());
        }

        // 清空内存缓冲
        pendingOffsetsByServerId.clear();

        // 更新最近一次刷库时间
        lastFlushTs = System.currentTimeMillis();
    }
}