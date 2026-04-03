package com.bsoft.server.sql.select;

/**
 * ClassName: OracleInsertStrategy
 * Package:   com.bsoft.server.sql
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 19:24
 */


import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.insert.InsertStrategy;
import com.bsoft.server.sql.select.SelectStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;


public abstract class AbstractSelectStrategy implements SelectStrategy {


    private static final Logger logger = LogManager.getLogger(AbstractSelectStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected  DatabaseAdapter adapter;


    public AbstractSelectStrategy() {
    }

    protected AbstractSelectStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }

    /**
     * 检查目标表是否存在
     * @return true-表存在, false-表不存在
     */
    public boolean checkTableExists(String tableName) throws Exception {

        // 1.构建SQL,buildCheckTableExistsSql是抽象方法，由具体数据库实现类实现
        String sql = buildCheckTableExistsSql();

        try (Connection conn = this.dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 2.设置参数，setParameters是抽象方法，由具体数据库实现类实现
            setParameters(ps,tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int count = rs.getInt(1);
                    boolean exists = count > 0;
                    logger.info("🔍 表 {} 存在检查结果: {}", tableName, exists);
                    return exists;
                }
            }

        } catch (Exception e) {
            logger.error("❌ 检查表是否存在失败: {}", tableName, e);
            throw e;
        }

        return false;
    }



}