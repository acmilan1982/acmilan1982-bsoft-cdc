package com.bsoft.server.batch;

/**
 * ClassName: BatchSizeManager
 * Package:   com.bsoft.server.k2db.batch
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/10/11 - 20:24
 */

import com.bsoft.common.enums.DatabaseType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 批次大小动态管理器
 * 根据不同数据库类型和数据特征动态调整批次大小
 */
public class BatchSizeManager implements Serializable {

    private static final Logger logger = LogManager.getLogger(BatchSizeManager.class);

    // 改为静态单例，避免重复创建
    private  ObjectMapper mapper;

    // 目标库类型
    private final DatabaseType databaseType;
    // 是否启用动态批次调整，仅Doris支持，关系型数据库使用固定值
    private final boolean enableDynamicBatching;

    // JDBC 每批次处理的记录数，关系型数据库使用固定值
    private static final int DEFAULT_JDBC_BATCH_SIZE = 500;

    // Doris Stream Load 相关配置
    private static final int DEFAULT_STREAM_LOAD_BATCH_SIZE = 1000;
    private static final int MIN_STREAM_LOAD_BATCH_SIZE = 500;

//    private static final int DEFAULT_STREAM_LOAD_BATCH_SIZE = 100;
//    private static final int MIN_STREAM_LOAD_BATCH_SIZE = 100;
    private static final int MAX_STREAM_LOAD_BATCH_SIZE = 10000;
    private static final long TARGET_BATCH_BYTES = 100 * 1024 * 1024; // 100MB目标大小


    // 上次刷新时间
    private transient long lastFlushTime;

    // 60秒超时刷新，超过60秒，无论批次大小，均强制刷新(前提：从kafka读取到数据，会触发这个时间检查)
    private final long flushIntervalMs = 60 * 1000L;

//    // 30秒强制刷新,即使没有从kafka读取到数据，也会触发这个时间
//    public static final long FORCE_FLUSH_THRESHOLD_MS = 30000;

    private int currentBatchSize;




    // 实时新增数据批量写入的最大等待时间。
    // 只用于 TargetTableWriteProcessFunction 的 processing-time timer。
    // 不复用 lastFlushTime，避免和快照 flush 逻辑耦合。
    private static final long REALTIME_INSERT_FLUSH_INTERVAL_MS = 60 * 1000L;



    public BatchSizeManager(DatabaseType databaseType, int initialBatchSize) {
        this.databaseType = databaseType;
        this.enableDynamicBatching = (databaseType == DatabaseType.DORIS);

        if (enableDynamicBatching) {
            this.currentBatchSize = initialBatchSize > 0 ? initialBatchSize : DEFAULT_STREAM_LOAD_BATCH_SIZE;
        } else {
            this.currentBatchSize = initialBatchSize > 0 ? initialBatchSize : DEFAULT_JDBC_BATCH_SIZE;
        }

        this.mapper = new ObjectMapper();
        logger.info("📋 初始化批次管理器: 数据库类型={}, 初始批次大小={}, 动态调整={}",
                databaseType, currentBatchSize, enableDynamicBatching ? "启用" : "禁用");
    }


    /**
     * 根据执行性能和数据大小综合调整批次大小
     * @param executionTimeMs 执行时间(毫秒)
     * @param batchItems 批次数据项(用于计算数据大小和记录数)
     */
    public void adjustBatchSizeByPerformance(long executionTimeMs, List<BatchInsertItem> batchItems) {
        if (!enableDynamicBatching || batchItems == null || batchItems.isEmpty()) {
            return;
        }

        // 批大小
        int processedCount = batchItems.size();
        // 小批次不参与调整(阈值可配置,建议100-500条)
        if (processedCount < 100) {
            logger.debug("⏭️ 批次过小({} 条),跳过批次大小调整", processedCount);
            return;
        }

        long batchSizeBytes = calculateBatchBytes(batchItems);

        double avgTimePerRecord = (double) executionTimeMs / processedCount;
        double avgBytesPerRecord = (double) batchSizeBytes / processedCount;

        int oldSize = currentBatchSize;

        // 同时考虑时间和数据大小
        if (avgTimePerRecord > 100 || batchSizeBytes > TARGET_BATCH_BYTES) {
            currentBatchSize = Math.max(MIN_STREAM_LOAD_BATCH_SIZE,
                    (int) (currentBatchSize * 0.8));
            logger.info("🔧 调整批次大小: {} -> {} (时间:{}ms/条, 大小:{}KB/条)",
                    oldSize, currentBatchSize,
                    String.format("%.2f", avgTimePerRecord),
                    String.format("%.2f", avgBytesPerRecord / 1024));
        }
        else if (avgTimePerRecord < 10 && batchSizeBytes < TARGET_BATCH_BYTES * 0.5) {
            currentBatchSize = Math.min(MAX_STREAM_LOAD_BATCH_SIZE,
                    (int) (currentBatchSize * 1.2));
            logger.info("🚀 增大批次大小: {} -> {} (时间:{}ms/条, 大小:{}KB/条)",
                    oldSize, currentBatchSize,
                    String.format("%.2f", avgTimePerRecord),
                    String.format("%.2f", avgBytesPerRecord / 1024));
        }
    }




    /**
     * 计算批次数据的总字节数
     */
    private long calculateBatchBytes(List<BatchInsertItem> batchItems) {
        long totalBytes = 0;
        for (BatchInsertItem item : batchItems) {
            try {
                String json = convertMapToJsonString(item.getData());
                if (json != null) {
                    totalBytes += json.getBytes(StandardCharsets.UTF_8).length;
                }
            } catch (Exception e) {
                // 估算值：默认200字节
                totalBytes += 200;
            }
        }
        return totalBytes;
    }



    // 将 Map 转换为 JSON 字符串用于大小估算
    private String convertMapToJsonString(Map<String, Object> data) {
        try {

            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            // 如果 Jackson 不可用，使用粗略估算
            return data.toString();
        }
    }



    /**
     * 获取当前批次大小
     */
    public int getCurrentBatchSize() {
        return currentBatchSize;
    }

    /**
     * 重置批次大小到默认值
     */
    public void resetToDefault() {
        int oldSize = currentBatchSize;
        if (enableDynamicBatching) {
            currentBatchSize = DEFAULT_STREAM_LOAD_BATCH_SIZE;
        } else {
            currentBatchSize = DEFAULT_JDBC_BATCH_SIZE;
        }

        if (oldSize != currentBatchSize) {
            logger.info("🔄 重置批次大小: {} -> {}", oldSize, currentBatchSize);
        }
    }

    /**
     * 获取批次管理器的状态信息
     */
    public String getStatusInfo() {
        return String.format("BatchSizeManager[数据库=%s, 当前批次=%d, 动态调整=%s]",
                databaseType, currentBatchSize, enableDynamicBatching ? "启用" : "禁用");
    }

    public long getLastFlushTime() {
        return lastFlushTime;
    }

    public void setLastFlushTime(long lastFlushTime) {
        this.lastFlushTime = lastFlushTime;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    // 获取实时新增数据批量 flush 间隔。
    // 当前固定 60 秒，不从配置文件读取。
    public long getRealtimeInsertFlushIntervalMs() {
        return REALTIME_INSERT_FLUSH_INTERVAL_MS;
    }
}
