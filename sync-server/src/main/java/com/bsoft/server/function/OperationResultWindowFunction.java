package com.bsoft.server.function;

/**
 * ClassName: OperationResultWindowFunction
 * Package:   com.bsoft.server.function
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:38
 */

import com.bsoft.server.k2db.TableOperationResultStats;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 操作结果统计窗口函数
 */
public class OperationResultWindowFunction implements WindowFunction<TableOperationResultStats, TableOperationResultStats, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<TableOperationResultStats> input, Collector<TableOperationResultStats> out) throws Exception {
        TableOperationResultStats stats = input.iterator().next();
        stats.setWindowStart(window.getStart());
        stats.setWindowEnd(window.getEnd());
        out.collect(stats);
    }
}