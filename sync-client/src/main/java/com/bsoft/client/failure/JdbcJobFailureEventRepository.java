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
import java.util.Objects;

// 基于JDBC的作业失败事件仓库实现。
@Component
public class JdbcJobFailureEventRepository implements JobFailureEventRepository {

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

    // 插入作业失败事件。
    @Override
    public void insert(JobFailureEvent event) throws Exception {
        // 事件对象不能为空。
        Objects.requireNonNull(event, "event");

        // 创建数据库连接并执行插入。
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.INSERT_JOB_FAILURE_EVENT)) {
            // 设置主键ID。
            ps.setLong(1, event.getId());

            // 设置客户端ID。
            ps.setString(2, event.getClientId());

            // 设置业务数据源ID。
            ps.setString(3, event.getDataSourceId());

            // 设置失败的Flink Job ID。
            ps.setString(4, event.getFailedJobId());

            // 设置失败发生时间。
            ps.setTimestamp(5, Timestamp.valueOf(event.getFailureTime()));

            // 设置Flink作业状态。
            ps.setString(6, event.getFlinkStatus());

            // 设置失败原因摘要。
            ps.setString(7, event.getFailureReason());

            // 设置完整失败异常详情。
            setClobString(ps, 8, event.getFailureDetail());

            // 设置事件处理结果。
            ps.setString(9, event.getEventResult().getCode());

            // 设置忽略原因。
            ps.setString(10, event.getIgnoreReason());

            // 执行插入。
            ps.executeUpdate();
        }
    }

    // 设置CLOB字符串参数。
    private void setClobString(PreparedStatement ps, int parameterIndex, String value) throws Exception {
        // 空值直接设置为null。
        if (value == null) {
            ps.setCharacterStream(parameterIndex, null, 0);
            return;
        }

        // 使用字符流写入CLOB，避免长异常详情受VARCHAR限制。
        ps.setCharacterStream(parameterIndex, new StringReader(value), value.length());
    }
}