package com.bsoft.client.failure;

import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.bsoft.common.constants.CdcSqlConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;

// 基于JDBC的作业失败处理记录仓库实现。
@Component
public class JdbcJobFailureHandleLogRepository implements JobFailureHandleLogRepository {

    // 配置库连接配置对象。
    @Autowired
    private ConfigDataSourceConfig configDataSourceConfig;

    // 配置库DataSource。
    private DataSource dataSource;

    // 初始化配置库DataSource。
    @PostConstruct
    public void initDataSource() {
        // 通过配置对象初始化配置库连接池，底层由DataSourceFactory缓存。
        this.dataSource = configDataSourceConfig.initDataSource();
    }

    // 插入作业失败处理记录。
    @Override
    public void insert(JobFailureHandleLog log) throws Exception {
        // 创建数据库连接并执行插入。
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.INSERT_JOB_FAILURE_HANDLE_LOG)) {
            // 设置主键ID。
            ps.setLong(1, log.id);

            // 设置关联失败事件ID。
            ps.setLong(2, log.failureEventId);

            // 设置客户端ID。
            ps.setString(3, log.clientId);

            // 设置业务数据源ID。
            ps.setString(4, log.dataSourceId);

            // 设置失败的Flink Job ID。
            ps.setString(5, log.failedJobId);

            // 设置重启尝试次数。
            setInteger(ps, 6, log.attemptNo);

            // 设置处理阶段。
            ps.setString(7, log.handleStage.getCode());

            // 设置处理时间。
            ps.setTimestamp(8, Timestamp.valueOf(log.handleTime));

            // 设置连续失败次数。
            setInteger(ps, 9, log.consecutiveFailures);

            // 设置累计重启次数。
            setLong(ps, 10, log.restartCountTotal);

            // 设置退避延迟秒数。
            setLong(ps, 11, log.restartDelaySeconds);

            // 设置下次计划重启时间。
            setTimestamp(ps, 12, log.nextRestartTime);

            // 设置重启开始时间。
            setTimestamp(ps, 13, log.restartStartTime);

            // 设置重启结束时间。
            setTimestamp(ps, 14, log.restartEndTime);

            // 设置新Job ID。
            ps.setString(15, log.newJobId);

            // 设置异常详情。
            setClobString(ps, 16, log.errorDetail);

            // 设置备注。
            ps.setString(17, log.remark);

            // 执行插入。
            ps.executeUpdate();
        }
    }

    // 设置可空Integer参数。
    private void setInteger(PreparedStatement ps, int index, Integer value) throws Exception {
        // 空值写入SQL NULL。
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
            return;
        }

        // 非空值写入整数。
        ps.setInt(index, value);
    }

    // 设置可空Long参数。
    private void setLong(PreparedStatement ps, int index, Long value) throws Exception {
        // 空值写入SQL NULL。
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
            return;
        }

        // 非空值写入长整数。
        ps.setLong(index, value);
    }

    // 设置可空时间参数。
    private void setTimestamp(PreparedStatement ps, int index, LocalDateTime value) throws Exception {
        // 空值写入SQL NULL。
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
            return;
        }

        // 非空值写入时间戳。
        ps.setTimestamp(index, Timestamp.valueOf(value));
    }

    // 设置CLOB字符串参数。
    private void setClobString(PreparedStatement ps, int index, String value) throws Exception {
        // 空值直接设置为NULL。
        if (value == null) {
            ps.setCharacterStream(index, null, 0);
            return;
        }

        // 使用字符流写入CLOB。
        ps.setCharacterStream(index, new StringReader(value), value.length());
    }
}