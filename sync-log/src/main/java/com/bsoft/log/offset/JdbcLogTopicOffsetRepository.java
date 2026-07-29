package com.bsoft.log.offset;

import com.bsoft.common.constants.CdcSqlConstants;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 基于 Oracle 配置库 CDC_LOG_TOPIC_OFFSET 表的日志 topic offset 仓储实现。
 */
public class JdbcLogTopicOffsetRepository implements LogTopicOffsetRepository {

    /**
     * 配置库数据源。
     */
    private final DataSource dataSource;

    /**
     * 创建日志 topic offset JDBC 仓储。
     */
    public JdbcLogTopicOffsetRepository(DataSource dataSource) {
        // 校验数据源不能为空。
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * 读取指定 logId 下指定日志 topic 的 nextOffset，并初始化缺失记录。
     */
    @Override
    public Map<String, Long> loadNextOffsets(String logId, Collection<String> kafkaTopics) throws Exception {
        // 标准化 logId。
        String normalizedLogId = requireText(logId, "logId");

        // 标准化 topic 集合。
        Set<String> normalizedTopics = normalizeTopics(kafkaTopics);

        // topic 不能为空，否则仓储层不知道需要初始化哪些记录。
        if (normalizedTopics.isEmpty()) {
            throw new IllegalArgumentException("kafkaTopics is empty");
        }

        // 打开数据库连接。
        try (Connection conn = dataSource.getConnection()) {
            // 关闭自动提交，保证“查询缺失记录 + 插入缺失记录 + 再查询”在同一个事务中完成。
            conn.setAutoCommit(false);

            try {
                // 先查询当前已存在的 offset。
                Map<String, Long> existingOffsets = queryExistingOffsets(conn, normalizedLogId, normalizedTopics);

                // 找出缺失的 topic。
                Set<String> missingTopics = findMissingTopics(normalizedTopics, existingOffsets);

                // 插入缺失 topic 的初始 offset。
                insertMissingOffsets(conn, normalizedLogId, missingTopics);

                // 提交初始化事务。
                conn.commit();

                // 如果没有缺失记录，直接返回已查询结果。
                if (missingTopics.isEmpty()) {
                    return existingOffsets;
                }

                // 有新插入记录时，重新查询一次，确保返回值来自数据库最终状态。
                return queryExistingOffsets(conn, normalizedLogId, normalizedTopics);
            } catch (Exception e) {
                // 发生异常时回滚事务。
                conn.rollback();

                // 将异常继续抛给调用方。
                throw e;
            } finally {
                // 恢复自动提交，避免连接池复用时污染后续调用。
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * 更新指定 logId 下多个日志 topic 的 nextOffset。
     */
    @Override
    public void updateNextOffsets(String logId, Map<String, Long> offsets) throws Exception {
        // 标准化 logId。
        String normalizedLogId = requireText(logId, "logId");

        // 空 offset 直接返回。
        if (offsets == null || offsets.isEmpty()) {
            return;
        }

        // 打开数据库连接并准备 UPDATE SQL。
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.UPDATE_LOG_TOPIC_NEXT_OFFSET_SQL)) {

            // 遍历每个 topic 的 nextOffset。
            for (Map.Entry<String, Long> entry : offsets.entrySet()) {
                // 标准化 topic。
                String kafkaTopic = requireText(entry.getKey(), "kafkaTopic");

                // 校验 nextOffset 不能为空。
                Long nextOffset = Objects.requireNonNull(entry.getValue(), "nextOffset");

                // 校验 nextOffset 不能为负数。
                if (nextOffset < 0L) {
                    throw new IllegalArgumentException("nextOffset cannot be negative, topic=" + kafkaTopic);
                }

                // 绑定 nextOffset。
                ps.setLong(1, nextOffset);

                // 绑定 logId。
                ps.setString(2, normalizedLogId);

                // 绑定 Kafka topic。
                ps.setString(3, kafkaTopic);

                // 加入批处理。
                ps.addBatch();
            }

            // 执行批量更新。
            int[] updateCounts = ps.executeBatch();

            // 校验所有 topic 都确实更新到了已有记录。
            validateUpdateCounts(offsets, updateCounts);
        }
    }

    /**
     * 查询当前已存在的 topic offset。
     */
    private Map<String, Long> queryExistingOffsets(Connection conn,
                                                   String logId,
                                                   Collection<String> kafkaTopics) throws Exception {
        // 构造返回结果。
        Map<String, Long> result = new LinkedHashMap<>();

        // 准备查询 SQL。
        try (PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_LOG_TOPIC_NEXT_OFFSET_SQL)) {
            // 遍历每个 topic 单独查询，避免动态拼 IN SQL。
            for (String kafkaTopic : kafkaTopics) {
                // 绑定 logId。
                ps.setString(1, logId);

                // 绑定 Kafka topic。
                ps.setString(2, kafkaTopic);

                // 执行查询。
                try (ResultSet rs = ps.executeQuery()) {
                    // 如果存在记录，则读取 nextOffset。
                    if (rs.next()) {
                        // 写入 topic -> nextOffset。
                        result.put(kafkaTopic, rs.getLong("NEXT_OFFSET"));
                    }
                }

                // 清理参数，避免下一轮绑定污染。
                ps.clearParameters();
            }
        }

        // 返回已存在 offset。
        return result;
    }

    /**
     * 找出尚未初始化 offset 的 topic。
     */
    private Set<String> findMissingTopics(Collection<String> kafkaTopics,
                                          Map<String, Long> existingOffsets) {
        // 创建缺失 topic 集合。
        Set<String> missingTopics = new LinkedHashSet<>();

        // 遍历所有期望 topic。
        for (String kafkaTopic : kafkaTopics) {
            // 如果查询结果中不存在该 topic，则加入缺失集合。
            if (!existingOffsets.containsKey(kafkaTopic)) {
                missingTopics.add(kafkaTopic);
            }
        }

        // 返回缺失 topic。
        return missingTopics;
    }

    /**
     * 插入缺失 topic 的初始 offset 记录。
     */
    private void insertMissingOffsets(Connection conn,
                                      String logId,
                                      Collection<String> missingTopics) throws Exception {
        // 没有缺失 topic 时直接返回。
        if (missingTopics == null || missingTopics.isEmpty()) {
            return;
        }

        // 准备插入 SQL。
        try (PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.INSERT_LOG_TOPIC_INITIAL_OFFSET_SQL)) {
            // 遍历缺失 topic。
            for (String kafkaTopic : missingTopics) {
                // 绑定 logId。
                ps.setString(1, logId);

                // 绑定 Kafka topic。
                ps.setString(2, kafkaTopic);

                // 绑定初始 nextOffset，默认从 0 开始消费。
                ps.setLong(3, 0L);

                // 加入批处理。
                ps.addBatch();
            }

            // 执行批量插入。
            ps.executeBatch();
        }
    }

    /**
     * 校验批量更新结果，避免启动初始化缺失导致 offset 没有真正更新。
     */
    private void validateUpdateCounts(Map<String, Long> offsets, int[] updateCounts) {
        // 校验 JDBC 返回数量是否匹配。
        if (updateCounts == null || updateCounts.length != offsets.size()) {
            throw new IllegalStateException("log topic offset update count mismatch");
        }

        // 准备遍历 topic。
        int index = 0;

        // 遍历 offset 输入集合。
        for (String kafkaTopic : offsets.keySet()) {
            // 读取当前 topic 的更新行数。
            int updateCount = updateCounts[index++];

            // SUCCESS_NO_INFO 表示驱动不提供具体行数，但执行成功。
            if (updateCount == PreparedStatement.SUCCESS_NO_INFO) {
                continue;
            }

            // EXECUTE_FAILED 表示执行失败。
            if (updateCount == PreparedStatement.EXECUTE_FAILED) {
                throw new IllegalStateException("failed to update log topic offset, topic=" + kafkaTopic);
            }

            // 更新行数为 0 说明记录不存在，不允许静默忽略。
            if (updateCount == 0) {
                throw new IllegalStateException("log topic offset row not found, topic=" + kafkaTopic);
            }
        }
    }

    /**
     * 标准化 topic 集合。
     */
    private Set<String> normalizeTopics(Collection<String> kafkaTopics) {
        // 使用 LinkedHashSet 去重并保留输入顺序。
        Set<String> normalizedTopics = new LinkedHashSet<>();

        // 空集合直接返回空结果。
        if (kafkaTopics == null) {
            return normalizedTopics;
        }

        // 遍历原始 topic。
        for (String kafkaTopic : kafkaTopics) {
            // 标准化 topic。
            String normalizedTopic = requireText(kafkaTopic, "kafkaTopic");

            // 加入结果集合。
            normalizedTopics.add(normalizedTopic);
        }

        // 返回标准化 topic 集合。
        return normalizedTopics;
    }

    /**
     * 校验文本字段不能为空，并返回 trim 后的值。
     */
    private String requireText(String value, String field) {
        // 空值或空白字符串都视为非法。
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }

        // 返回 trim 后文本。
        return value.trim();
    }
}