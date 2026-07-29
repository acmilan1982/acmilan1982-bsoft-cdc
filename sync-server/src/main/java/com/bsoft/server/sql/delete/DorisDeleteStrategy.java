package com.bsoft.server.sql.delete;

/**
 * ClassName: DorisDeleteStrategy
 * Package:   com.bsoft.server.sql.delete
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/25 - 10:54
 */

/**
 * Doris 删除策略
 *
 * 规则：
 * 1. TABLE_CDC 场景：按 tableRowDeleteStrategy 决定物理删除或逻辑删除
 * 2. PIPELINE_SQL_RESULT 场景：上层会强制走物理删除，不使用逻辑删除
 */
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.TableRowDeleteStrategy;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.doris.http.DorisStreamLoadHelper;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import com.bsoft.server.sql.doris.http.StreamLoadResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
public class DorisDeleteStrategy extends AbstractDeleteStrategy {

    private static final Logger logger = LogManager.getLogger(DorisDeleteStrategy.class);
    private static final DateTimeFormatter DORIS_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DorisStreamLoadHelper streamLoadHelper;
    private DatabaseAdapter adapter;

    public DorisDeleteStrategy() {
    }

    public DorisDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig, TableRowDeleteStrategy tableRowDeleteStrategy) {
        super(targetDataSourceConfig, tableRowDeleteStrategy);
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.streamLoadHelper = new DorisStreamLoadHelper(targetDataSourceConfig, this.adapter);
    }

    // 强制物理删除
    @Override
    public void executePhysicalDelete(String tableName, KafkaPayload payload) throws Exception {
        List<KafkaPayload> payloads = new ArrayList<>(1);
        payloads.add(payload);
        executeBatchPhysicalDeleteHttp(tableName, payloads);
    }


    @Override
    public Map<Long, String> executeBatchDelete(String tableName, List<KafkaPayload> payloads) throws Exception {
        if (payloads == null || payloads.isEmpty()) {
            return Collections.emptyMap();
        }

        // 逻辑删除
        if (tableRowDeleteStrategy == TableRowDeleteStrategy.DELETE_FLAG) {
            return executeBatchLogicalDeleteHttp(tableName, payloads);
        }

        // 物理删除
        return executeBatchPhysicalDeleteHttp(tableName, payloads);
    }

    private Map<Long, String> executeBatchPhysicalDeleteHttp(String tableName, List<KafkaPayload> payloads) throws Exception {
        long startTime = System.currentTimeMillis();
        Map<Long, String> statusMap = new HashMap<>();

        List<KafkaPayload> validPayloads = new ArrayList<>();
        List<Map<String, Object>> deleteRows = new ArrayList<>();

        Set<String> primaryKeySet = streamLoadHelper.resolvePrimaryKeySet(payloads.get(0).getPrimaryKeySet());
        List<String> pkList = new ArrayList<>(primaryKeySet);


        for (KafkaPayload payload : payloads) {
            Long offset = payload.getOffset();
            // 获取消息中的数据
            Map<String, Object> beforeData = payload.getBeforeData();

            if (beforeData == null) {
                statusMap.put(offset, "FAILED: beforeData is null");
                continue;
            }

            // 删除的唯一键信息
            Map<String, Object> row = new HashMap<>();
            boolean missingPk = false;
            for (String pk : pkList) {
                Object pkValue = beforeData.get(pk);
                if (pkValue == null) {
                    missingPk = true;
                    break;
                }
                row.put(pk, pkValue);
            }

            if (missingPk) {
                statusMap.put(offset, "FAILED: missing primary key value");
                continue;
            }

            deleteRows.add(row);
            validPayloads.add(payload);
        }

        if (deleteRows.isEmpty()) {
            return statusMap;
        }

        StreamLoadResult result = new StreamLoadResult(false, 0, 0, "pre_execute_failed");
        try {
            String jsonArray = streamLoadHelper.toJson(deleteRows);
            result = streamLoadHelper.executeDelete(tableName, jsonArray, pkList);

            if (result.isSuccess()) {
                logger.info("Doris Stream Load 删除成功: 本组记录共 {} 条，实际成功 {} 条 过滤 {} 条 执行耗时 {} 秒",
                        validPayloads.size(),
                        result.getNumberLoadedRows(),
                        result.getNumberFilteredRows(),
                        (System.currentTimeMillis() - startTime) / 1000.0);
            } else {
                logger.info("Doris Stream Load 删除失败，错误信息 {}", result.getResponseJson());
            }
        } catch (Exception e) {
            result = new StreamLoadResult(
                    false, 0, 0,
                    "执行异常: " + ExceptionDetailUtil.buildRootCauseDetail(e, 2000));
            throw e;
        } finally {
            boolean success = result.isSuccess();
            for (KafkaPayload payload : validPayloads) {
                Long offset = payload.getOffset();
                statusMap.put(offset, success ? "SUCCESS" : "FAILED: " + result.getResponseJson());
            }
        }

        return statusMap;
    }

    // 逻辑删除
    private Map<Long, String> executeBatchLogicalDeleteHttp(String tableName, List<KafkaPayload> payloads) throws Exception {
        long startTime = System.currentTimeMillis();
        Map<Long, String> statusMap = new HashMap<>();

        // 获取主键
        Set<String> primaryKeySet = streamLoadHelper.resolvePrimaryKeySet(payloads.get(0).getPrimaryKeySet());

        List<String> pkList = new ArrayList<>(primaryKeySet);


        List<Map<String, Object>> rows = new ArrayList<>();
        List<KafkaPayload> validPayloads = new ArrayList<>();


        // 处理每一条消息
        for (KafkaPayload payload : payloads) {
            Long offset = payload.getOffset();
            Map<String, Object> beforeData = payload.getBeforeData();

            if (beforeData == null) {
                statusMap.put(offset, "FAILED: beforeData is null");
                continue;
            }

            boolean missingPk = false;
            for (String pk : pkList) {
                Object pkValue = beforeData.get(pk);
                if (pkValue == null) {
                    missingPk = true;
                    break;
                }
            }
            if (missingPk) {
                statusMap.put(offset, "FAILED: missing primary key value");
                continue;
            }

            Map<String, Object> row = new HashMap<>();
            for (String pk : pkList) {
                row.put(pk, beforeData.get(pk));
            }

            row.put("cdc_op", "d");
            row.put("cdc_delete_flag", 1);
            String now = LocalDateTime.now().format(DORIS_DATETIME_FORMATTER);
            row.put("cdc_delete_time", now);
            row.put("cdc_update_time", now);

            Set<String> dateFieldSet = payload.getDateFieldSet();
            appendCdcTimeFields(dateFieldSet);

            rows.add(streamLoadHelper.processDataForStreamLoad(row, dateFieldSet));
            validPayloads.add(payload);
        }

        if (rows.isEmpty()) {
            return statusMap;
        }

        StreamLoadResult result = new StreamLoadResult(false, 0, 0, "pre_execute_failed");
        try {
            List<String> columns = new ArrayList<>();
            columns.addAll(pkList);
            columns.add("cdc_op");
            columns.add("cdc_delete_flag");
            columns.add("cdc_delete_time");
            columns.add("cdc_update_time");

            String jsonArray = streamLoadHelper.toJson(rows);
            result = streamLoadHelper.executePartialUpdate(tableName, jsonArray, columns);

            if (result.isSuccess()) {
                logger.info("Doris Stream Load 逻辑删除成功: 本组记录共 {} 条，实际成功 {} 条 过滤 {} 条 执行耗时 {} 秒",
                        validPayloads.size(),
                        result.getNumberLoadedRows(),
                        result.getNumberFilteredRows(),
                        (System.currentTimeMillis() - startTime) / 1000.0);
            } else {
                logger.info("Doris Stream Load 逻辑删除失败，错误信息 {}", result.getResponseJson());
            }
        } catch (Exception e) {
            result = new StreamLoadResult(
                    false, 0, 0,
                    "执行异常: " + ExceptionDetailUtil.buildRootCauseDetail(e, 2000));
            throw e;
        } finally {
            boolean success = result.isSuccess();
            for (KafkaPayload payload : validPayloads) {
                Long offset = payload.getOffset();
                statusMap.put(offset, success ? "SUCCESS" : "FAILED: " + result.getResponseJson());
            }
        }

        return statusMap;
    }


    private void appendCdcTimeFields(Set<String> dateFieldSet) {
        if (dateFieldSet == null) {
            return;
        }
        dateFieldSet.add("cdc_dml_Time");
        dateFieldSet.add("cdc_kafka_enqueue_time");
        dateFieldSet.add("cdc_insert_time");
        dateFieldSet.add("cdc_update_time");
        dateFieldSet.add("cdc_delete_time");
    }

    @Override
    public String buildDeleteSql(String tableName, Set<String> primaryKeySet) {
        validatePrimaryKeySet(tableName, primaryKeySet);
        String whereClause = buildWhereClause(primaryKeySet);

        switch (tableRowDeleteStrategy) {
            case DELETE:
                return String.format("delete from %s where %s", quote(tableName), whereClause);
            case DELETE_FLAG:
                return String.format(
                        "update %s set cdc_op = 'd' ,%s = 1, %s = NOW(), %s = NOW()  where %s",
                        quote(tableName),
                        quote("cdc_delete_flag"),
                        quote("cdc_delete_time"),
                        quote("cdc_update_time"),
                        whereClause
                );
            default:
                throw new IllegalArgumentException("unsupported delete strategy: " + tableRowDeleteStrategy);
        }
    }

    private String quote(String ident) {
        if (ident == null) return "``";
        return "`" + ident.replace("`", "") + "`";
    }
}