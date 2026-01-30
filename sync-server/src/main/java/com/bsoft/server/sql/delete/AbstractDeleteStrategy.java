package com.bsoft.server.sql.delete;

import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.util.CdcSqlConstants;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ClassName: AbstractDeleteStrategy
 * Package:   com.bsoft.server.sql.delete
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/30 - 15:36
 */
public abstract class AbstractDeleteStrategy implements DeleteStrategy {

    private static final Logger logger = LogManager.getLogger(AbstractDeleteStrategy.class);

    protected DataSourceHolder dataSourceHolder;


    public AbstractDeleteStrategy() {
    }

    protected AbstractDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }


    @Override
    public String buildDeleteSql(String tableName, Set<String> primaryKeySet) {
        // 没有主键，无法安全删除
        if (primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("Primary key set is empty, unsafe to delete from table: " + tableName);
        }

        // 4. WHERE：固定顺序（字典序），避免 Set 迭代顺序漂移
        List<String> primaryKeyList = new ArrayList<>(primaryKeySet);
        primaryKeyList.sort(String::compareTo);
        // 物理删除

        String whereClause = primaryKeyList.stream()
                .map(pk -> pk + " = ?")
                .collect(Collectors.joining(" AND "));
        return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);

    }

    @Override
    public void setParameters(PreparedStatement ps,
                              Map<String, Object> beforeData,
                              Set<String> primaryKeySet) throws SQLException {
        // 仅绑定 WHERE 子句的主键参数（单条删除）
        int index = 1;

        List<String> primaryKeyList = new ArrayList<>(primaryKeySet);
        primaryKeyList.sort(String::compareTo);

        for (String pk : primaryKeyList) {
            Object value = beforeData.get(pk);
            ps.setObject(index++, value);
        }
    }

    @Override
    public void executeDelete(String tableName, KafkaPayload payload) throws Exception {
        Map<String, Object> beforeData = payload.getBeforeData();
        Set<String> primaryKeySet = payload.getPrimaryKeySet();

        // 用源表的主键删除，可能存在以下问题:1.源表没有主键 2.多表合并，主键冲突，因此统一用ROWID来删除
        primaryKeySet.clear();
        primaryKeySet.add(CdcSqlConstants.CDC_ROWID_FIELD);


        // 检查beforeData是否包含所有主键值
        for (String pk : primaryKeySet) {
            if (!beforeData.containsKey(pk.toLowerCase()) || beforeData.get(pk.toLowerCase()) == null) {
                logger.info("⚠️ Primary key value missing in beforeData: " + pk + " for table: " + tableName);
                return;
            }
        }

        // 构建SQL
        String sql = buildDeleteSql(tableName, primaryKeySet);

        boolean success = true;
        String logDetail = "";

        try (Connection conn = dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 设置参数
            setParameters(ps, beforeData, primaryKeySet);

            // 执行操作
            int affectedRows = ps.executeUpdate();

            if (affectedRows == 0) {
                logger.warn("⚠️ No rows affected for table: {}, primaryKeys: {}",
                        tableName, primaryKeySet);
            } else {
                logger.debug("✅ Deleted {} rows in table: {}", affectedRows, tableName);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw  e;  // 抛出异常前，try-with-resources 自动关闭连接
        }
    }
}
