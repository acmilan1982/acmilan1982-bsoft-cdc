package com.bsoft.server.function;

/**
 * ClassName: OperationResultStatsAggregator
 * Package:   com.bsoft.server.function
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:36
 */
import com.bsoft.common.bean.OperationResult;
import com.bsoft.server.k2db.InjectOffsetIntoMessageSchema;
import com.bsoft.server.k2db.TableOperationResultStats;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 操作结果统计聚合器 - 统计处理后的实际结果
 */
public class OperationResultStatsAggregator implements AggregateFunction<OperationResult, TableOperationResultStats, TableOperationResultStats> {

    private static final Logger logger = LogManager.getLogger(OperationResultStatsAggregator.class);

    // 当前主题已处理的记录总数
    private long recordCount = 0L;

    private long totalDeserializeTime = 0L;  // 累计反序列化时间(毫秒)

    private String name;


    public OperationResultStatsAggregator() {
    }

    public OperationResultStatsAggregator(String name) {
        this.name = name;
    }

    @Override
    public TableOperationResultStats createAccumulator() {
        return new TableOperationResultStats();
    }

    @Override
    public TableOperationResultStats add(OperationResult value, TableOperationResultStats accumulator) {
        // ✅ 记录方法开始时间
        long startTime = System.currentTimeMillis();

        accumulator.setTableName(value.getTableName());

        String op = value.getOperation();
        // 成功/失败 标志
        boolean success = value.isSuccess();

        // 累加 增删改 对应成功失败的次数
        switch (op) {
            case "r":
            case "c":
                accumulator.incrementInsert(success);
                break;
            case "u":
                accumulator.incrementUpdate(success);
                break;
            case "d":
                accumulator.incrementDelete(success);
                break;
        }

        // 单次操作的执行时间，这里累加
        accumulator.addExecutionTime(value.getExecutionTimeMs());

        // 服务端id
        accumulator.setServerId(value.getServerId());
        // 源 数据源id
        accumulator.setBusinessDataSourceId(value.getDataFromSourceId());
        // 目标 数据源id
        accumulator.setTargetDateSourceId(value.getDateToSourceId());

        // ✅ 累计执行时间
        totalDeserializeTime += (System.currentTimeMillis() - startTime);
        ++recordCount;


//        if(recordCount % 10000 == 0) {
//
//            logger.info("{}---TableOperationResultStats次数：{} 次，总耗时: {}s, 平均单条耗时: {}s",
//                    this.name,
//                    recordCount,
//                    totalDeserializeTime / 1000.0,
//                    (totalDeserializeTime / recordCount) / 1000.0);
//
//        }



        return accumulator;
    }

    @Override
    public TableOperationResultStats getResult(TableOperationResultStats accumulator) {
        return accumulator;
    }

    @Override
    public TableOperationResultStats merge(TableOperationResultStats a, TableOperationResultStats b) {
        TableOperationResultStats merged = new TableOperationResultStats();

        // 合并成功统计
        merged.setInsertSuccess(a.getInsertSuccess() + b.getInsertSuccess());
        merged.setInsertFailure(a.getInsertFailure() + b.getInsertFailure());
        merged.setUpdateSuccess(a.getUpdateSuccess() + b.getUpdateSuccess());
        merged.setUpdateFailure(a.getUpdateFailure() + b.getUpdateFailure());
        merged.setDeleteSuccess(a.getDeleteSuccess() + b.getDeleteSuccess());
        merged.setDeleteFailure(a.getDeleteFailure() + b.getDeleteFailure());

        // 合并执行时间统计
        merged.setTotalExecutionTime(a.getTotalExecutionTime() + b.getTotalExecutionTime());
        merged.setOperationCount(a.getOperationCount() + b.getOperationCount());

        return merged;
    }
}
