package com.bsoft.server.sql.update;

import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.log.LogCollector;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.doris.http.DorisStreamLoadHelper;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import com.bsoft.server.sql.doris.http.StreamLoadResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Doris 数据库更新策略实现（单条更新；WHERE 使用传入的 primaryKeys）
 *
 * 约定：
 * - keys：本次写入涉及的列（包含主键列与待更新列）
 * - primaryKeys：用于定位行的主键列集合（若无主键，你会把 ROWID 放进来）
 *
 * 生成：
 *   UPDATE `table` SET `col1`=?,`col2`=? ... WHERE `pk1`=? AND `pk2`=? ...
 *
 * 绑定顺序：
 *   1) 先绑定所有“非主键列”的值（按 keys 的顺序过滤）
 *   2) 再绑定所有“主键列”的值（按 primaryKeys 的迭代顺序；建议使用 LinkedHashSet 以保持稳定）
 */
public class DorisUpdateStrategy extends AbstractUpdateStrategy {

    private static final Logger logger = LogManager.getLogger(DorisUpdateStrategy.class);

    private DorisStreamLoadHelper streamLoadHelper;
    private DatabaseAdapter adapter;

    public DorisUpdateStrategy() {
    }

    public DorisUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.streamLoadHelper = new DorisStreamLoadHelper(targetDataSourceConfig, this.adapter);
    }

    @Override
    public void executeUpdate(String tableName, KafkaPayload payload) throws Exception {
        long startTime = System.currentTimeMillis();

        Map<String, Object> afterData = payload.getAfterData();
        if (afterData == null || afterData.isEmpty()) {
            logger.warn("No afterData for table: {}", tableName);
            return;
        }

        // 获取主键
        Set<String> primaryKeySet = streamLoadHelper.resolvePrimaryKeySet(payload.getPrimaryKeySet());
        List<String> pkList = new ArrayList<>(primaryKeySet);


        // 待更新的字段
        List<String> updateKeys = new ArrayList<>();
        for (String key : afterData.keySet()) {
            if (!primaryKeySet.contains(key)) {
                updateKeys.add(key);
            }
        }

        if (updateKeys.isEmpty()) {
            logger.warn("No update columns in afterData for table: {}", tableName);
            return;
        }

        afterData.put("cdc_op", payload.getOp());
        afterData.put("cdc_update_time", LocalDateTime.now());

        Set<String> dateFieldSet = payload.getDateFieldSet();
        appendCdcTimeFields(dateFieldSet);

        // 需要被更新的列->列值
        Map<String, Object> row = new HashMap<>();
        // 包含pk
        for (String pk : pkList) {
            Object pkValue = afterData.get(pk);
            if (pkValue == null) {
                throw new IllegalArgumentException("missing primary key value: " + pk);
            }
            row.put(pk, pkValue);
        }
        // 包含一般列
        for (String key : updateKeys) {
            row.put(key, afterData.get(key));
        }
        row.put("cdc_op", afterData.get("cdc_op"));
        row.put("cdc_update_time", afterData.get("cdc_update_time"));

        // 处理后的 列->列值，时间做了转换
        Map<String, Object> processedRow = streamLoadHelper.processDataForStreamLoad(row, dateFieldSet);
        List<Map<String, Object>> rows = Collections.singletonList(processedRow);

        /*
            http请求中的列：columns后的列名
              curl --location-trusted -u root: -H "partial_columns:true" -H "column_separator:," -H "columns:order_id,order_status"
              -T /tmp/update.csv http://127.0.0.1:8030/api/db1/order_tbl/_stream_load
         */
        List<String> columns = new ArrayList<>();
        columns.addAll(pkList);
        columns.addAll(updateKeys);
        columns.add("cdc_op");
        columns.add("cdc_update_time");

        StreamLoadResult result = new StreamLoadResult(false, 0, 0, "pre_execute_failed");

        try {
            // 转换成json字符串
            String jsonArray = streamLoadHelper.toJson(rows);
            // 发送请求，执行更新操作
            result = streamLoadHelper.executePartialUpdate(tableName, jsonArray, columns);

            if (result.isSuccess()) {
                logger.info("Doris Stream Load 更新成功: 本组记录共 {} 条，实际成功 {} 条 过滤 {} 条 执行耗时 {} 秒",
                        1,
                        result.getNumberLoadedRows(),
                        result.getNumberFilteredRows(),
                        (System.currentTimeMillis() - startTime) / 1000.0);
            } else {
                logger.info("Doris Stream Load 更新失败，错误信息 {}", result.getResponseJson());
            }
        } catch (Exception e) {
            result = new StreamLoadResult(
                    false, 0, 0,
                    "执行异常: " + LogCollector.getSafeStackTraceHead(e, 40, 4000));
            throw e;
        }
    }



    private void appendCdcTimeFields(Set<String> dateFieldSet) {
        if (dateFieldSet == null) {
            return;
        }
        dateFieldSet.add("cdc_update_time");
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> keys, Set<String> primaryKeySet, Set<String> dateFieldSet) {
        return "";
    }

    @Override
    public void setParameters(java.sql.PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet) {
    }
}