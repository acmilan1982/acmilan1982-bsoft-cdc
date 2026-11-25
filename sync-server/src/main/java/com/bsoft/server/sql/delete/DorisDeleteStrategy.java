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
import com.bsoft.common.enums.DorisTableModel;
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
    // 服务端生成的 CDC 时间按本地字段时间输出到秒。
    private static final DateTimeFormatter DORIS_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

    private DorisStreamLoadHelper streamLoadHelper;
    private DatabaseAdapter adapter;

    public DorisDeleteStrategy() {
    }

    public DorisDeleteStrategy(TargetDataSourceConfig targetDataSourceConfig, TableRowDeleteStrategy tableRowDeleteStrategy) {
        super(targetDataSourceConfig, tableRowDeleteStrategy);
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.streamLoadHelper = new DorisStreamLoadHelper(targetDataSourceConfig, this.adapter);
    }

//    // 强制物理删除
//    @Override
//    public void executePhysicalDelete(String tableName, KafkaPayload payload) throws Exception {
//        List<KafkaPayload> payloads = new ArrayList<>(1);
//        payloads.add(payload);
//        executeBatchPhysicalDeleteHttp(tableName, payloads);
//    }

    // 单条删除复用严格的 HTTP 批量入口，失败时向调用方抛出异常。
    @Override
    public void executeDelete(String tableName, KafkaPayload payload) throws Exception {
        // 单条消息不依赖 offset 映射判断结果。
        executeBatchDelete(tableName, Collections.singletonList(payload), false);
    }

    // 单条强制物理删除复用同一 HTTP 批量入口。
    @Override
    public void executePhysicalDelete(String tableName, KafkaPayload payload)
            throws Exception {
        // 显式覆盖逻辑删除配置。
        executeBatchDelete(tableName, Collections.singletonList(payload), true);
    }

    // 保留旧双参数接口，仅供尚未迁移的调用使用。
// 旧返回类型无法表达重复 offset，因此在发送请求前明确拒绝这种输入。
    @Override
    public Map<Long, String> executeBatchDelete(String tableName,
                                                List<KafkaPayload> payloads)
            throws Exception {
        // 保留旧入口对空批次的处理。
        Map<Long, String> statusMap = new LinkedHashMap<>();
        if (payloads == null || payloads.isEmpty()) {
            return statusMap;
        }

        // 在写库之前检查旧返回类型能否正确表达每条消息。
        Set<Long> offsets = new HashSet<>();
        for (KafkaPayload payload : payloads) {
            if (payload == null || payload.getOffset() == null) {
                throw new IllegalArgumentException(
                        "旧批量删除接口要求消息及 offset 非空");
            }
            if (!offsets.add(payload.getOffset())) {
                throw new IllegalArgumentException(
                        "旧批量删除接口不能处理重复 offset，请使用三参数批量入口");
            }
        }

        // 真正执行逻辑统一到新入口，失败直接抛出异常。
        executeBatchDelete(tableName, payloads, false);

        // 只有整批正常返回后才构造旧接口所需的成功结果。
        for (KafkaPayload payload : payloads) {
            statusMap.put(payload.getOffset(), "SUCCESS");
        }
        return statusMap;
    }
//    @Override
//    public Map<Long, String> executeBatchDelete(String tableName, List<KafkaPayload> payloads) throws Exception {
//        if (payloads == null || payloads.isEmpty()) {
//            return Collections.emptyMap();
//        }
//
//        // 逻辑删除
//        if (tableRowDeleteStrategy == TableRowDeleteStrategy.DELETE_FLAG) {
//            return executeBatchLogicalDeleteHttp(tableName, payloads);
//        }
//
//        // 物理删除
//        return executeBatchPhysicalDeleteHttp(tableName, payloads);
//    }

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

//    // 逻辑删除
//    private Map<Long, String> executeBatchLogicalDeleteHttp(String tableName, List<KafkaPayload> payloads) throws Exception {
//        long startTime = System.currentTimeMillis();
//        Map<Long, String> statusMap = new HashMap<>();
//
//        // 获取主键
//        Set<String> primaryKeySet = streamLoadHelper.resolvePrimaryKeySet(payloads.get(0).getPrimaryKeySet());
//
//        List<String> pkList = new ArrayList<>(primaryKeySet);
//
//
//        List<Map<String, Object>> rows = new ArrayList<>();
//        List<KafkaPayload> validPayloads = new ArrayList<>();
//
//
//        // 处理每一条消息
//        for (KafkaPayload payload : payloads) {
//            Long offset = payload.getOffset();
//            Map<String, Object> beforeData = payload.getBeforeData();
//
//            if (beforeData == null) {
//                statusMap.put(offset, "FAILED: beforeData is null");
//                continue;
//            }
//
//            boolean missingPk = false;
//            for (String pk : pkList) {
//                Object pkValue = beforeData.get(pk);
//                if (pkValue == null) {
//                    missingPk = true;
//                    break;
//                }
//            }
//            if (missingPk) {
//                statusMap.put(offset, "FAILED: missing primary key value");
//                continue;
//            }
//
//            Map<String, Object> row = new HashMap<>();
//            for (String pk : pkList) {
//                row.put(pk, beforeData.get(pk));
//            }
//
//            row.put("cdc_op", "d");
//            row.put("cdc_delete_flag", 1);
//            String now = LocalDateTime.now().format(DORIS_DATETIME_FORMATTER);
//            row.put("cdc_delete_time", now);
//            row.put("cdc_update_time", now);
//
//            Set<String> dateFieldSet = payload.getDateFieldSet();
//            appendCdcTimeFields(dateFieldSet);
//
//            rows.add(streamLoadHelper.processDataForStreamLoad(row, dateFieldSet));
//            validPayloads.add(payload);
//        }
//
//        if (rows.isEmpty()) {
//            return statusMap;
//        }
//
//        StreamLoadResult result = new StreamLoadResult(false, 0, 0, "pre_execute_failed");
//        try {
//            List<String> columns = new ArrayList<>();
//            columns.addAll(pkList);
//            columns.add("cdc_op");
//            columns.add("cdc_delete_flag");
//            columns.add("cdc_delete_time");
//            columns.add("cdc_update_time");
//
//            String jsonArray = streamLoadHelper.toJson(rows);
//            result = streamLoadHelper.executePartialUpdate(tableName, jsonArray, columns);
//
//            if (result.isSuccess()) {
//                logger.info("Doris Stream Load 逻辑删除成功: 本组记录共 {} 条，实际成功 {} 条 过滤 {} 条 执行耗时 {} 秒",
//                        validPayloads.size(),
//                        result.getNumberLoadedRows(),
//                        result.getNumberFilteredRows(),
//                        (System.currentTimeMillis() - startTime) / 1000.0);
//            } else {
//                logger.info("Doris Stream Load 逻辑删除失败，错误信息 {}", result.getResponseJson());
//            }
//        } catch (Exception e) {
//            result = new StreamLoadResult(
//                    false, 0, 0,
//                    "执行异常: " + ExceptionDetailUtil.buildRootCauseDetail(e, 2000));
//            throw e;
//        } finally {
//            boolean success = result.isSuccess();
//            for (KafkaPayload payload : validPayloads) {
//                Long offset = payload.getOffset();
//                statusMap.put(offset, success ? "SUCCESS" : "FAILED: " + result.getResponseJson());
//            }
//        }
//
//        return statusMap;
//    }


//    private void appendCdcTimeFields(Set<String> dateFieldSet) {
//        if (dateFieldSet == null) {
//            return;
//        }
//        dateFieldSet.add("cdc_dml_Time");
//        dateFieldSet.add("cdc_kafka_enqueue_time");
//        dateFieldSet.add("cdc_insert_time");
//        dateFieldSet.add("cdc_update_time");
//        dateFieldSet.add("cdc_delete_time");
//    }

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



    // 通过 HTTP 执行整批删除，适用于当前约定的 Doris UNIQUE、Merge-on-Write 表。
// 正常返回表示整批确认成功或按 DUPLICATE 表规则明确跳过。
    @Override
    public void executeBatchDelete(String tableName,
                                   List<KafkaPayload> payloads,
                                   boolean forcePhysicalDelete) throws Exception {
        // 校验输入，空批次无需发送请求。
        Objects.requireNonNull(payloads, "删除批次不能为 null");
        if (payloads.isEmpty()) {
            return;
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("删除的目标表名不能为空");
        }

        // 检查策略初始化状态。
        if (streamLoadHelper == null
                || dataSourceHolder == null
                || dataSourceHolder.getTargetDataSourceConfig() == null) {
            throw new IllegalStateException("Doris 删除策略尚未初始化");
        }

        // 按目标表模型决定是否允许删除。
        TargetDataSourceConfig targetConfig =
                dataSourceHolder.getTargetDataSourceConfig();
        DorisTableModel model = targetConfig.getTableModel(tableName);
        if (model == DorisTableModel.DUPLICATE) {
            logger.info(
                    "Doris DUPLICATE 表跳过 DELETE，未执行实际写库，table={}, rows={}",
                    tableName, payloads.size());
            return;
        }
        if (model != DorisTableModel.UNIQUE) {
            throw new IllegalStateException(
                    "仅允许删除 Doris UNIQUE 表，table="
                            + tableName + "，tableModel=" + model);
        }

        // 强制物理删除优先，否则必须使用明确的删除配置。
        if (!forcePhysicalDelete
                && tableRowDeleteStrategy != TableRowDeleteStrategy.DELETE
                && tableRowDeleteStrategy != TableRowDeleteStrategy.DELETE_FLAG) {
            throw new IllegalStateException(
                    "不支持的删除策略: " + tableRowDeleteStrategy);
        }
        boolean logicalDelete = !forcePhysicalDelete
                && tableRowDeleteStrategy == TableRowDeleteStrategy.DELETE_FLAG;

        // 固定写入列，逻辑删除只覆盖原有 CDC 字段范围。
        List<String> columns = new ArrayList<>();
        columns.add("cdc_rowid");
        if (logicalDelete) {
            columns.add("cdc_op");
            columns.add("cdc_delete_flag");
            columns.add("cdc_delete_time");
            columns.add("cdc_update_time");
        }

        // 同一批次共用服务端当前时间，不读取源日期字段及其单位映射。
        String now = logicalDelete
                ? LocalDateTime.now().format(DORIS_DATETIME_FORMATTER)
                : null;

        // 在发送请求前构造并校验全部记录。
        List<Map<String, Object>> rows = new ArrayList<>(payloads.size());
        Set<String> rowIds = new HashSet<>();
        for (KafkaPayload payload : payloads) {
            // 仅接受删除消息。
            if (payload == null || !"d".equals(payload.getOp())) {
                throw new IllegalArgumentException("删除批次包含空消息或非删除消息");
            }

            // 删除仅从 before 中提取行身份。
            Map<String, Object> beforeData = payload.getBeforeData();
            if (beforeData == null) {
                throw new IllegalArgumentException("删除消息缺少 before 数据");
            }

            // 忽略字段名大小写，拒绝大小写重复的主键字段。
            Object rowIdValue = null;
            boolean rowIdFound = false;
            for (Map.Entry<String, Object> entry : beforeData.entrySet()) {
                if ("cdc_rowid".equalsIgnoreCase(entry.getKey())) {
                    if (rowIdFound) {
                        throw new IllegalArgumentException(
                                "删除消息包含重复的 cdc_rowid 字段");
                    }
                    rowIdFound = true;
                    rowIdValue = entry.getValue();
                }
            }

            // 主键值必须是非空字符串，并保留原始内容。
            if (!(rowIdValue instanceof String)
                    || ((String) rowIdValue).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "删除消息必须提供非空字符串 cdc_rowid");
            }
            String rowId = (String) rowIdValue;

            // 上游负责按顺序拆批，本层防止重复主键意外进入同一请求。
            if (!rowIds.add(rowId)) {
                throw new IllegalArgumentException(
                        "同一 Doris 删除批次包含重复 cdc_rowid，请上游按事件顺序拆批");
            }

            // 创建独立请求数据，不修改原始 before 数据。
            Map<String, Object> requestRow = new LinkedHashMap<>();
            requestRow.put("cdc_rowid", rowId);
            if (logicalDelete) {
                // 时间已经是服务端生成的秒级字符串，无需再做日期单位转换。
                requestRow.put("cdc_op", "d");
                requestRow.put("cdc_delete_flag", 1);
                requestRow.put("cdc_delete_time", now);
                requestRow.put("cdc_update_time", now);
            }
            rows.add(requestRow);
        }

        // 按配置发送一次 HTTP 请求，不进行单条循环写入。
        String jsonArray = streamLoadHelper.toJson(rows);
        StreamLoadResult result;
        if (logicalDelete) {
            // 复用禁止新 key 的局部更新入口，目标不存在时失败。
            result = streamLoadHelper.executePartialUpdate(
                    tableName, jsonArray, columns);
        } else {
            // 物理删除仅提交 cdc_rowid。
            result = streamLoadHelper.executeDelete(
                    tableName, jsonArray, columns);
        }

        // HTTP 失败或缺少结果时抛出异常，避免上层误记为成功。
        if (result == null || !result.isSuccess()) {
            String detail = result == null
                    ? "未返回 Stream Load 结果"
                    : result.getResponseJson();
            throw new IllegalStateException(
                    "Doris 批量删除失败或结果未确认，table="
                            + tableName + "，detail=" + detail);
        }

        // 不接受过滤部分输入后仍将整个批次标记为成功。
        if (result.getNumberFilteredRows() != 0
                || result.getNumberLoadedRows() != rows.size()) {
            throw new IllegalStateException(
                    "Doris 批量删除返回数量异常，table=" + tableName
                            + "，submittedRows=" + rows.size()
                            + "，loadedRows=" + result.getNumberLoadedRows()
                            + "，filteredRows=" + result.getNumberFilteredRows());
        }
    }

    private String quote(String ident) {
        if (ident == null) return "``";
        return "`" + ident.replace("`", "") + "`";
    }
}