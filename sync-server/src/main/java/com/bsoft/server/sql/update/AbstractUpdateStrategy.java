package com.bsoft.server.sql.update;


import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.constants.CdcSqlConstants;

import com.bsoft.server.sql.adapter.DataSourceHolder;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 更新策略抽象基类
 * 封装通用的更新流程逻辑
 */
public abstract class AbstractUpdateStrategy  implements UpdateStrategy {

    private static final Logger logger = LogManager.getLogger(AbstractUpdateStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected DatabaseAdapter adapter;

    public AbstractUpdateStrategy() {
    }

    protected AbstractUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }

    @Override
    public void executeUpdate(String tableName, KafkaPayload payload) throws Exception {
        Map<String, Object> afterData = payload.getAfterData();
        // 被更新的字段列表
        List<String> keys = new ArrayList<>(afterData.keySet());
        Set<String> primaryKeySet = payload.getPrimaryKeySet();


        // 用源表的主键更新，可能存在以下问题:1.源表没有主键 2.多表合并，主键冲突，因此统一用ROWID来更新
        primaryKeySet.clear();
        primaryKeySet.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);



        // 过滤掉主键字段
        List<String> updateKeys = keys.stream()
                .filter(key -> !primaryKeySet.contains(key))
                .collect(Collectors.toList());

        // 检查是否有可更新的字段
        if (updateKeys.isEmpty()) {
            logger.warn("⚠️ No update columns in afterData for table: {}", tableName);
            return;
        }

        // 添加CDC字段
        keys.add("cdc_op");
        keys.add("cdc_update_time");
        afterData.put("cdc_op", payload.getOp());
        afterData.put("cdc_update_time", LocalDateTime.now());

        Set<String> dateFieldSet = payload.getDateFieldSet();
        appendCdcTimeFields(dateFieldSet);

        // 构建SQL，抽象方法，由子类实现具体逻辑
        String sql = buildUpdateSql(tableName, keys, primaryKeySet,dateFieldSet);

        // 执行更新
        try (Connection conn = dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 设置参数,抽象方法，由子类实现具体逻辑
            setParameters(ps, afterData, keys, primaryKeySet, dateFieldSet);

            // 执行更新
            int affectedRows = ps.executeUpdate();

            if (affectedRows == 0) {
                logger.warn("⚠️ No rows affected for table: {}, primaryKeys: {}",
                        tableName, primaryKeySet);
            } else {
                logger.debug("✅ Updated {} rows in table: {}", affectedRows, tableName);
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw  e;
        }
    }



    /**
     * 校验更新操作的必要参数
     * @param tableName 表名
     * @param keys 所有字段列表
     * @param primaryKeySet 主键字段集合
     * @throws IllegalArgumentException 参数不合法时抛出异常
     */
    protected void validateUpdateParameters(String tableName, List<String> keys, Set<String> primaryKeySet) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName is empty");
        }

        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("keys is empty for table: " + tableName);
        }

        if (primaryKeySet == null || primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("primaryKeys is empty, unsafe to update: " + tableName);
        }
    }

    private void appendCdcTimeFields(Set<String> dateFieldSet) {
        dateFieldSet.add("cdc_update_time");

    }

}
