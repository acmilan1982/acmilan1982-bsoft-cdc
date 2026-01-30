package com.bsoft.server.sink;

/**
 * ClassName: OperationResultStatisticsSink
 * Package:   com.bsoft.server.k2m
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:38
 */


import com.bsoft.server.k2db.TableOperationResultStats;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

/**
 * 操作结果统计输出Sink
 */
public class OperationResultStatisticsSink implements SinkFunction<TableOperationResultStats> {

    private static final Logger LOG = Logger.getLogger(OperationResultStatisticsSink.class.getName());
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void invoke(TableOperationResultStats stats, Context context) throws Exception {
        LocalDateTime windowStart = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(stats.getWindowStart()), ZoneId.systemDefault());
        LocalDateTime windowEnd = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(stats.getWindowEnd()), ZoneId.systemDefault());

//        String logMessage = String.format(
//                "📈 [MySQL执行统计] 表: %s | 窗口: %s ~ %s\n" +
//                        "   ✅ 成功: %d (%.1f%%) | ❌ 失败: %d | 📊 总计: %d\n" +
//                        "   📝 插入: %d成功/%d失败 | 🔄 更新: %d成功/%d失败 | 🗑️ 删除: %d成功/%d失败\n" +
//                        "   ⏱️ 平均执行时间: %.2fms | 🚀 吞吐量: %.2f ops/s",
//                stats.getTableName(),
//                windowStart.format(FORMATTER), windowEnd.format(FORMATTER),
//                stats.getTotalSuccess(), stats.getSuccessRate(), stats.getTotalFailure(), stats.getTotalOperations(),
//                stats.getInsertSuccess(), stats.getInsertFailure(),
//                stats.getUpdateSuccess(), stats.getUpdateFailure(),
//                stats.getDeleteSuccess(), stats.getDeleteFailure(),
//                stats.getAvgExecutionTime(), stats.getThroughputPerSecond()
//        );

//        LOG.info(logMessage);
    }
}