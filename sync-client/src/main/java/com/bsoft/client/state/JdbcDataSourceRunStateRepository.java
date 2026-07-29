package com.bsoft.client.state;

import com.bsoft.common.constants.CdcSqlConstants;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

// 基于 JDBC 的业务库运行状态仓库实现
public class JdbcDataSourceRunStateRepository implements DataSourceRunStateRepository {

    // snapshot 正在运行状态
    private static final String SNAPSHOT_RUNNING = "SNAPSHOT_RUNNING";

    // snapshot 已完成状态
    private static final String SNAPSHOT_COMPLETED = "SNAPSHOT_COMPLETED";

    // 配置库数据源
    private final DataSource dataSource;

    // 创建 JDBC 仓库实现
    public JdbcDataSourceRunStateRepository(DataSource dataSource) {
        // 保存配置库数据源
        this.dataSource = dataSource;
    }

    // 确保当前 client + dataSource 存在一条运行状态记录
    @Override
    public void ensureRunStateExists(String clientId, String dataSourceId) throws Exception {
        // 如果记录已经存在，则不重复插入
        if (exists(clientId, dataSourceId)) {
            return;
        }

        // 如果记录不存在，则插入默认的 snapshot running 状态
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.INSERT_DATA_SOURCE_RUN_STATE)) {
            // 设置客户端 ID
            ps.setString(1, clientId);
            // 设置业务库 ID
            ps.setString(2, dataSourceId);
            // 设置初始状态
            ps.setString(3, SNAPSHOT_RUNNING);
            // 执行插入
            ps.executeUpdate();
        }
    }

    // 判断当前业务库 snapshot 是否已经完成
    @Override
    public boolean isSnapshotCompleted(String clientId, String dataSourceId) throws Exception {
        // 查询当前业务库的 snapshot 状态
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SOURCE_SNAPSHOT_STATUS)) {
            // 设置客户端 ID
            ps.setString(1, clientId);
            // 设置业务库 ID
            ps.setString(2, dataSourceId);

            // 执行查询
            try (ResultSet rs = ps.executeQuery()) {
                // 没有记录时视为 snapshot 未完成
                if (!rs.next()) {
                    return false;
                }

                // 读取 snapshot 状态
                String status = rs.getString("SNAPSHOT_STATUS");

                // 判断是否已经完成
                return SNAPSHOT_COMPLETED.equalsIgnoreCase(status);
            }
        }
    }

    // 标记当前业务库 snapshot 正在运行
    @Override
    public void markSnapshotRunning(String clientId, String dataSourceId) throws Exception {
        // 执行状态更新
        int updated = updateSnapshotStatus(
                CdcSqlConstants.UPDATE_DATA_SOURCE_SNAPSHOT_RUNNING,
                SNAPSHOT_RUNNING,
                clientId,
                dataSourceId
        );

        // 如果没有更新到记录，说明启动初始化流程没有正确执行
        if (updated == 0) {
            throw new IllegalStateException("CDC_DATA_SOURCE_RUN_STATE record not found, clientId="
                    + clientId + ", dataSourceId=" + dataSourceId);
        }
    }

    // 标记当前业务库 snapshot 已经完成
    @Override
    public void markSnapshotCompleted(String clientId, String dataSourceId) throws Exception {
        // 执行状态更新
        int updated = updateSnapshotStatus(
                CdcSqlConstants.UPDATE_DATA_SOURCE_SNAPSHOT_COMPLETED,
                SNAPSHOT_COMPLETED,
                clientId,
                dataSourceId
        );

        // 如果没有更新到记录，说明启动初始化流程没有正确执行
        if (updated == 0) {
            throw new IllegalStateException("CDC_DATA_SOURCE_RUN_STATE record not found, clientId="
                    + clientId + ", dataSourceId=" + dataSourceId);
        }
    }

    // 判断当前 client + dataSource 的运行状态记录是否存在
    private boolean exists(String clientId, String dataSourceId) throws Exception {
        // 查询记录数量
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.CHECK_DATA_SOURCE_RUN_STATE_EXISTS)) {
            // 设置客户端 ID
            ps.setString(1, clientId);
            // 设置业务库 ID
            ps.setString(2, dataSourceId);

            // 执行查询
            try (ResultSet rs = ps.executeQuery()) {
                // 查询结果正常情况下必有一行 COUNT
                if (!rs.next()) {
                    return false;
                }

                // COUNT 大于 0 表示记录存在
                return rs.getInt(1) > 0;
            }
        }
    }

    // 按指定 SQL 更新 snapshot 状态
    private int updateSnapshotStatus(String sql,
                                     String snapshotStatus,
                                     String clientId,
                                     String dataSourceId) throws Exception {
        // 创建数据库连接并执行更新
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            // 设置 snapshot 状态
            ps.setString(1, snapshotStatus);
            // 设置客户端 ID
            ps.setString(2, clientId);
            // 设置业务库 ID
            ps.setString(3, dataSourceId);

            // 返回更新行数
            return ps.executeUpdate();
        }
    }
}