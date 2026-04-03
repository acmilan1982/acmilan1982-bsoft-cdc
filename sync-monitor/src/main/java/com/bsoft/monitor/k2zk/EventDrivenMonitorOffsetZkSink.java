package com.bsoft.monitor.k2zk;

import com.bsoft.common.config.properties.MonitorServerInfo;
import com.bsoft.common.constants.MetricKeys;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.metric.MonitorMetricEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * OFFSET 事件驱动提交 Sink：
 * 1. 每条事件先 updateOffset 到 GlobalZooKeeperManager 内存缓冲
 * 2. 达到条数阈值或时间阈值时触发 flushOffsets()
 * 3. close() 时做最终 flush
 */
public class EventDrivenMonitorOffsetZkSink extends RichSinkFunction<MonitorMetricEvent> {
    private static final Logger logger = LogManager.getLogger(EventDrivenMonitorOffsetZkSink.class);

    private final MonitorServerInfo monitorServerInfo;

    private transient GlobalZooKeeperManager globalZooKeeperManager;
    private transient int pendingOffsetCount;
    private transient long lastFlushTs;

    // 统计日志字段（便于排查）
    private transient long totalReceived;
    private transient long totalFlushCount;
    private transient long totalFlushDurationMs;
    private transient long lastProgressLogTs;

    public EventDrivenMonitorOffsetZkSink(MonitorServerInfo monitorServerInfo) {
        this.monitorServerInfo = monitorServerInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(monitorServerInfo);

        this.pendingOffsetCount = 0;
        this.lastFlushTs = System.currentTimeMillis();

        this.totalReceived = 0L;
        this.totalFlushCount = 0L;
        this.totalFlushDurationMs = 0L;
        this.lastProgressLogTs = this.lastFlushTs;

        logger.info(
                "EventDrivenMonitorOffsetZkSink opened. serverId={}, batchSize={}, intervalMs={}",
                monitorServerInfo.getServerId(),
                OffsetFlushDefaults.BATCH_SIZE,
                OffsetFlushDefaults.INTERVAL_MS
        );
    }

    @Override
    public void invoke(MonitorMetricEvent event, Context context) throws Exception {
        if (event == null) {
            logger.warn("Received null MonitorMetricEvent, ignored.");
            return;
        }

        try {
            // 内存中更新zk偏移量
            globalZooKeeperManager.updateOffset(event);

            // 偏移量 监控数据 消息总数
            totalReceived++;
            // 挂起的 偏移量 监控数据 消息总数(未更新到zk的)
            pendingOffsetCount++;

            // 挂起数量 或 间隔时间 阈值判断
            long now = System.currentTimeMillis();
            boolean reachBatch = pendingOffsetCount >= OffsetFlushDefaults.BATCH_SIZE;
            boolean reachTime = (now - lastFlushTs) >= OffsetFlushDefaults.INTERVAL_MS;


            if (reachBatch || reachTime) {
                flushOffsets(reachBatch ? "batch-threshold" : "time-threshold", now);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void flushOffsets(String reason, long now) throws Exception {
        long begin = System.currentTimeMillis();
        int flushSize = pendingOffsetCount;

        logger.info(
                "Start flush offsets. reason={}, flushSize={}, sinceLastFlushMs={}, receivedTotal={}",
                reason, flushSize, (now - lastFlushTs), totalReceived
        );

        globalZooKeeperManager.flushOffsetMetrics();

        long cost = System.currentTimeMillis() - begin;
        totalFlushCount++;
        totalFlushDurationMs += cost;
        pendingOffsetCount = 0;
        lastFlushTs = now;

        logger.info(
                "Flush offsets finished. reason={}, flushed={}, costMs={}, flushCount={}, avgFlushMs={}",
                reason,
                flushSize,
                cost,
                totalFlushCount,
                totalFlushDurationMs / totalFlushCount
        );
    }

    @Override
    public void close() throws Exception {
        logger.info(
                "Closing EventDrivenMonitorOffsetZkSink. receivedTotal={}, pending={}, flushCount={}",
                totalReceived, pendingOffsetCount, totalFlushCount
        );

        try {
            if (pendingOffsetCount > 0) {
                flushOffsets("sink-close", System.currentTimeMillis());
            }
        } finally {
            logger.info(
                    "EventDrivenMonitorOffsetZkSink closed. finalReceivedTotal={}, finalFlushCount={}, avgFlushMs={}",
                    totalReceived,
                    totalFlushCount,
                    totalFlushCount == 0 ? 0 : (totalFlushDurationMs / totalFlushCount)
            );
            super.close();
        }
    }


}