package com.bsoft.server.sink;

/**
 * ClassName: ZooKeeperStatisticsSink
 * Package:   com.bsoft.server.k2m
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/28 - 20:55
 */


import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.enums.WindowTimeUnit;
import com.bsoft.server.k2db.TableOperationResultStats;

import com.bsoft.common.global.GlobalZooKeeperManager;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.time.format.DateTimeFormatter;

/**
 * 将统计数据写入ZooKeeper的Sink
 */
public class ZooKeeperStatisticsSink extends RichSinkFunction<TableOperationResultStats> {


    private static final Logger logger = LogManager.getLogger(ZooKeeperStatisticsSink.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private ServerInfo serverInfo;

    // zk中监控数据路径
    private String monitorDataPath;

    private WindowTimeUnit windowTimeUnit;


    private transient GlobalZooKeeperManager globalOffsetManager;

    public ZooKeeperStatisticsSink(ServerInfo serverInfo, WindowTimeUnit windowTimeUnit) {
        this.serverInfo = serverInfo;
        this.windowTimeUnit = windowTimeUnit;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 使用全局的ZK管理器
        this.globalOffsetManager = GlobalZooKeeperManager.getInstance(serverInfo);

    }

    @Override
    public void invoke(TableOperationResultStats stats, Context context)  throws Exception {
        try {

            // 构建ZK节点路径
            monitorDataPath= serverInfo.getMonitorDataPath()  + "/" + stats.getTargetDateSourceId() + "/" + stats.getTableName() + "/" + windowTimeUnit.getName();

            // 添加时间戳信息到统计数据
            StatsWithTimestamp statsWithTime = new StatsWithTimestamp(stats);

            // 使用全局管理器即时更新统计数据
            globalOffsetManager.updateStatisticsImmediately(monitorDataPath, statsWithTime, CreateMode.PERSISTENT);





//            logger.info(String.format("📊 统计数据已写入ZK: 表=%s, 窗口=%s, 成功率=%.1f%%, 吞吐量=%.2f ops/s",
//                    stats.getTableName(), statsWithTime.getCreateTime(),
//                    stats.getSuccessRate(), stats.getThroughputPerSecond()));

        } catch (Exception e) {
            logger.info("❌ 写入ZooKeeper失败: " + e.getMessage());
//            throw e;
        }
    }



    @Override
    public void close() throws Exception {
        // 移除原有的zooKeeperNodeManager相关代码
        // 不需要关闭全局管理器，它会在应用关闭时统一关闭
        super.close();
        logger.info("🔌 ZooKeeperStatisticsSink已关闭");
    }

    /**
     * 带时间戳的统计数据
     */
    public static class StatsWithTimestamp extends TableOperationResultStats {
        private String createTime;

        public StatsWithTimestamp() {
            super();
            this.createTime = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");

        }

        public StatsWithTimestamp(TableOperationResultStats stats) {
            this();
            // 复制所有字段
            this.setTableName(stats.getTableName());
            this.setInsertSuccess(stats.getInsertSuccess());
            this.setInsertFailure(stats.getInsertFailure());
            this.setUpdateSuccess(stats.getUpdateSuccess());
            this.setUpdateFailure(stats.getUpdateFailure());
            this.setDeleteSuccess(stats.getDeleteSuccess());
            this.setDeleteFailure(stats.getDeleteFailure());
            this.setTotalExecutionTime(stats.getTotalExecutionTime());
            this.setOperationCount(stats.getOperationCount());
            this.setWindowStart(stats.getWindowStart());
            this.setWindowEnd(stats.getWindowEnd());
            this.setBusinessDataSourceId(stats.getBusinessDataSourceId());
            this.setTargetDateSourceId(stats.getTargetDateSourceId());
            this.setWindowStartFormatted(DateFormatUtils.format(stats.getWindowStart(), "yyyy-MM-dd HH:mm:ss"));
            this.setWindowEndFormatted(DateFormatUtils.format(stats.getWindowEnd(), "yyyy-MM-dd HH:mm:ss"));
        }

        public String getCreateTime() {
            return createTime;
        }

        public void setCreateTime(String createTime) {
            this.createTime = createTime;
        }
    }


}