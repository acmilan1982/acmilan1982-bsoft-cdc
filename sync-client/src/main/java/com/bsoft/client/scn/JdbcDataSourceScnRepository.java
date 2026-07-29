package com.bsoft.client.scn;

import com.bsoft.common.constants.CdcSqlConstants;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 基于 Oracle 配置库的 topic offset 仓储实现。
 */
public class JdbcDataSourceScnRepository implements DataSourceScnRepository {


    // 复用配置库 DataSource，不额外创建连接池
    private final DataSource dataSource;

    /**
     * 构造仓储实现。
     */
    public JdbcDataSourceScnRepository(DataSource dataSource) {
        // DataSource 不能为空，否则无法读写配置库
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    /**
     * 读取某个 server 的全部 topic -> nextOffset。
     */
    @Override
    public Map<String, Long> loadNextScns(String clientId) throws Exception {
        // 标准化 clientId，避免空白字符串进入 SQL
        String normalizedClientId = requireText(clientId, "clientId");

        // 使用 LinkedHashMap，便于调试时保持查询顺序
        Map<String, Long> result = new LinkedHashMap<>();

        // 直接查询当前 server 持有的全部 offset
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SOURCE_NEXT_SCNS_SQL)) {

            // 绑定 serverId
            ps.setString(1, normalizedClientId);

            // 遍历结果集，填充 topic -> nextOffset
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String dataSourceId = rs.getString("DATA_SOURCE_ID");
                    long nextScn = rs.getLong("SCN");
                    result.put(dataSourceId, nextScn);
                }
            }
        }

        return result;
    }

    /**
     * 批量 upsert 多个 topic 的 nextOffset。
     */
    @Override
    public void upsertNextScns(String clientId, Map<String, Long> scns) throws Exception {
        // 标准化 clientId，避免空白字符串进入 SQL
        String normalizedClientId = requireText(clientId, "clientId");

        // 空集合直接返回，避免无意义数据库调用
        if (scns == null || scns.isEmpty()) {
            return;
        }

        // 复用一个 PreparedStatement 执行批量 MERGE
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.MERGE_DATA_SOURCE_NEXT_SCN_SQL)) {

            // 遍历每个 topic 的 nextOffset
            for (Map.Entry<String, Long> entry : scns.entrySet()) {
                // topic 不能为空
                String dataSourceId = requireText(entry.getKey(), "dataSourceId");

                // nextOffset 不能为空
                Long nextScn = Objects.requireNonNull(entry.getValue(), "scn");

                // nextOffset 不允许负数
                if (nextScn < 0L) {
                    throw new IllegalArgumentException(
                            "nextOffset cannot be negative, scn=" + nextScn
                    );
                }

                // 按 MERGE SQL 的顺序绑定参数
                ps.setString(1, normalizedClientId);
                ps.setString(2, dataSourceId);
                ps.setLong(3, nextScn);

                // 累加入批处理
                ps.addBatch();
            }

            // 一次性执行批量写入
            ps.executeBatch();
        }
    }
//
//    /**
//     * 单条 upsert，一个 topic 一条记录。
//     */
//    @Override
//    public void upsertNextOffset(String serverId, String kafkaTopic, long nextOffset) throws Exception {
//        // nextOffset 不允许负数
//        if (nextOffset < 0L) {
//            throw new IllegalArgumentException("nextOffset cannot be negative");
//        }
//
//        // 复用批量接口，保持单条和批量的写入语义一致
//        Map<String, Long> single = new LinkedHashMap<>();
//        single.put(requireText(kafkaTopic, "kafkaTopic"), nextOffset);
//        upsertNextOffsets(serverId, single);
//    }

    /**
     * 校验并标准化文本字段。
     */
    private String requireText(String value, String field) {
        // 空值或空白字符串都视为非法输入
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(field + " is blank");
        }

        // 返回 trim 后的值，避免主键中混入首尾空格
        return value.trim();
    }
}
