package com.bsoft.pipeline.plan.state;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

/**
 * JDBC-backed repository for plan run state.
 *
 * Expected table columns:
 * PLAN_ID, RUN_ID, PHASE, SNAPSHOT_SCN, SNAPSHOT_START_TIME, SNAPSHOT_END_TIME,
 * INCREMENTAL_START_TIME, LAST_ERROR, UPDATED_AT
 */
public class JdbcPipelinePlanRunStateRepository implements PipelinePlanRunStateRepository {

    private static final String DEFAULT_TABLE_NAME = "CDC_PIPELINE_RUN_STATE";

    private final DataSource dataSource;
    private final String tableName;

    public JdbcPipelinePlanRunStateRepository(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE_NAME);
    }

    public JdbcPipelinePlanRunStateRepository(DataSource dataSource, String tableName) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.tableName = requireText(tableName, "tableName");
    }

    @Override
    public Optional<PipelinePlanRunState> findByPlanId(String planId) {
        if (planId == null || planId.trim().isEmpty()) {
            return Optional.empty();
        }

        String sql = "SELECT PLAN_ID, RUN_ID, PHASE, SNAPSHOT_SCN, SNAPSHOT_START_TIME, SNAPSHOT_END_TIME, "
                + "INCREMENTAL_START_TIME, LAST_ERROR "
                + "FROM " + tableName + " WHERE PLAN_ID = ? "
                + "ORDER BY UPDATED_AT DESC NULLS LAST, SNAPSHOT_START_TIME DESC NULLS LAST";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, planId.trim());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(mapRow(rs));
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to query plan run state, planId=" + planId, e);
        }
    }
    @Override
    public PipelinePlanRunState insert(PipelinePlanRunState state) {
        // 首次插入，不删除旧记录
        Objects.requireNonNull(state, "state");
        requireText(state.getPlanId(), "state.planId");
        requireText(state.getRunId(), "state.runId");
        Objects.requireNonNull(state.getPhase(), "state.phase");

        String sql = "INSERT INTO " + tableName + " ("
                + "PLAN_ID, RUN_ID, PHASE, SNAPSHOT_SCN, SNAPSHOT_START_TIME, SNAPSHOT_END_TIME, "
                + "INCREMENTAL_START_TIME, LAST_ERROR, UPDATED_AT"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            bindState(ps, state);
            ps.executeUpdate();
            return state;
        } catch (Exception e) {
            throw new IllegalStateException("failed to insert plan run state, planId=" + state.getPlanId(), e);
        }
    }

    @Override
    public PipelinePlanRunState update(PipelinePlanRunState state) {
        // 后续状态推进只做 update
        Objects.requireNonNull(state, "state");
        requireText(state.getPlanId(), "state.planId");
        requireText(state.getRunId(), "state.runId");
        Objects.requireNonNull(state.getPhase(), "state.phase");

        String sql = "UPDATE " + tableName + " SET "
                + "RUN_ID = ?, "
                + "PHASE = ?, "
                + "SNAPSHOT_SCN = ?, "
                + "SNAPSHOT_START_TIME = ?, "
                + "SNAPSHOT_END_TIME = ?, "
                + "INCREMENTAL_START_TIME = ?, "
                + "LAST_ERROR = ?, "
                + "UPDATED_AT = ? "
                + "WHERE PLAN_ID = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, state.getRunId());
            ps.setString(2, state.getPhase().name());

            if (state.getSnapshotScn() == null) {
                ps.setNull(3, Types.BIGINT);
            } else {
                ps.setLong(3, state.getSnapshotScn());
            }

            setTimestamp(ps, 4, state.getSnapshotStartTime());
            setTimestamp(ps, 5, state.getSnapshotEndTime());
            setTimestamp(ps, 6, state.getIncrementalStartTime());
            ps.setString(7, state.getLastError());
            ps.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(9, state.getPlanId());

            int updated = ps.executeUpdate();
            if (updated == 0) {
                throw new IllegalStateException("plan run state not found when update, planId=" + state.getPlanId());
            }

            return state;
        } catch (Exception e) {
            throw new IllegalStateException("failed to update plan run state, planId=" + state.getPlanId(), e);
        }
    }

    private void bindState(PreparedStatement ps, PipelinePlanRunState state) throws Exception {
        // 绑定 insert 参数
        ps.setString(1, state.getPlanId());
        ps.setString(2, state.getRunId());
        ps.setString(3, state.getPhase().name());

        if (state.getSnapshotScn() == null) {
            ps.setNull(4, Types.BIGINT);
        } else {
            ps.setLong(4, state.getSnapshotScn());
        }

        setTimestamp(ps, 5, state.getSnapshotStartTime());
        setTimestamp(ps, 6, state.getSnapshotEndTime());
        setTimestamp(ps, 7, state.getIncrementalStartTime());
        ps.setString(8, state.getLastError());
        ps.setTimestamp(9, Timestamp.valueOf(LocalDateTime.now()));
    }


    private PipelinePlanRunState mapRow(ResultSet rs) throws Exception {
        PipelinePlanRunState state = new PipelinePlanRunState();
        state.setPlanId(rs.getString("PLAN_ID"));
        state.setRunId(rs.getString("RUN_ID"));
        state.setPhase(PipelinePlanPhase.valueOf(rs.getString("PHASE")));

        long snapshotScn = rs.getLong("SNAPSHOT_SCN");
        if (!rs.wasNull()) {
            state.setSnapshotScn(snapshotScn);
        }

        state.setSnapshotStartTime(toLocalDateTime(rs.getTimestamp("SNAPSHOT_START_TIME")));
        state.setSnapshotEndTime(toLocalDateTime(rs.getTimestamp("SNAPSHOT_END_TIME")));
        state.setIncrementalStartTime(toLocalDateTime(rs.getTimestamp("INCREMENTAL_START_TIME")));
        state.setLastError(rs.getString("LAST_ERROR"));
        return state;
    }

    private void setTimestamp(PreparedStatement ps, int index, LocalDateTime value) throws Exception {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(index, Timestamp.valueOf(value));
        }
    }

    private LocalDateTime toLocalDateTime(Timestamp ts) {
        return ts == null ? null : ts.toLocalDateTime();
    }

    private String requireText(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }
        return value.trim();
    }
}
