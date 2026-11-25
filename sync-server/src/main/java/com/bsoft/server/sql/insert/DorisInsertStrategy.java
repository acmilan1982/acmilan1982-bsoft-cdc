package com.bsoft.server.sql.insert;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.util.ExceptionDetailUtil;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.sql.doris.DorisCustomColumnMappingSupport;
import com.bsoft.server.sql.doris.http.DorisStreamLoadHelper;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import com.bsoft.server.sql.doris.http.StreamLoadResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

// Doris 插入策略，支持按明确日期单位转换后通过 Stream Load 或 JDBC 写入。
public class DorisInsertStrategy extends AbstractInsertStrategy {

    private static final Logger logger = LogManager.getLogger(DorisInsertStrategy.class);

    private DorisStreamLoadHelper streamLoadHelper;

    public DorisInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.streamLoadHelper = new DorisStreamLoadHelper(targetDataSourceConfig, this.adapter);
    }

    @Override
    public String buildInsertSql(String tableName, List<String> keys, Set<String> primaryKeys, Set<String> dateFieldSet) {
        String columns = String.join(",", keys);
        String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(","));
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName.toLowerCase(), columns, placeholders);
    }

    // 保留原接口入口，不为非空源日期字段猜测单位。
    @Override
    public void setParameters(java.sql.PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keyList,
                              Set<String> dateFieldSet) throws java.sql.SQLException {
        // 无单位入口仅能处理普通字段、空值及服务端生成的 CDC 时间。
        setParameters(
                ps,
                data,
                keyList,
                dateFieldSet,
                Collections.emptyMap()
        );
    }

    // 按插入字段顺序绑定参数，并将当前消息的单位映射传给 DorisAdapter。
    @Override
    public void setParameters(java.sql.PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keyList,
                              Set<String> dateFieldSet,
                              Map<String, String> dateFieldUnits) throws java.sql.SQLException {
        // 保留现有字段列表校验。
        if (keyList == null || keyList.isEmpty()) {
            throw new IllegalArgumentException("columns(keys) is empty");
        }

        // 校验数据和日期元数据，没有日期字段时允许使用空集合。
        Objects.requireNonNull(data, "data 不能为 null");
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // JDBC 参数索引从 1 开始。
        int parameterIndex = 1;

        // 按 SQL 中的字段顺序逐个绑定。
        for (String fieldName : keyList) {
            // DorisAdapter 按单位解释日期数值，并统一输出到秒。
            this.adapter.setParameterValue(
                    ps,
                    parameterIndex,
                    data.get(fieldName),
                    fieldName,
                    dateFieldSet,
                    dateFieldUnits
            );

            // 移动到下一个参数位置。
            parameterIndex++;
        }
    }

    // Stream Load批量插入
    @Override
    public Map<Long, String> executeBatchInsert(String tableName,
                                                List<BatchInsertItem> batchItems,
                                                BatchSizeManager batchSizeManager,
                                                List<ColumnInfo> allColumns,
                                                boolean needPadding) throws Exception {
        // 批处理开始时间
        long startTime = System.currentTimeMillis();
        // 记录每条数据的处理状态，value为状态码或错误信息
        Map<Long, String> statusMap = new HashMap<>();

        // Stream Load执行结果
        // 初始化默认失败结果，避免空指针
        StreamLoadResult streamLoadResult = new StreamLoadResult(false, 0, 0, "执行前异常");

        try {
            // 如果需要字段补齐,先执行补齐逻辑
            if (needPadding && allColumns != null && !allColumns.isEmpty()) {
                logger.info("开始字段补齐 目标表:{} 共:{} 个字段", tableName, allColumns.size());
                padFieldsForBatch(batchItems, allColumns);
            }

            // 执行 Stream Load
            streamLoadResult = executeStreamLoad(batchItems, tableName,allColumns);

            // 根据执行成功与否输出日志
            if (streamLoadResult.isSuccess()) {
                int loadedRows = streamLoadResult.getNumberLoadedRows();
                int filteredRows = streamLoadResult.getNumberFilteredRows();
                logger.info("Doris Stream Load 成功: 本组记录共 {} 条，实际成功 {} 条 过滤 {} 条 执行耗时 {} 秒",
                        batchItems.size(), loadedRows, filteredRows,
                        (System.currentTimeMillis() - startTime) / 1000.0);
            } else {
                logger.info("Doris Stream Load 失败，错误信息 {}", streamLoadResult.getResponseJson());
            }

        } catch (Exception e) {
            streamLoadResult = new StreamLoadResult(
                    false, 0, 0,
                    "执行异常: " + ExceptionDetailUtil.buildRootCauseDetail(e, 2000));
        } finally {
            batchSizeManager.adjustBatchSizeByPerformance(System.currentTimeMillis() - startTime, batchItems);

            boolean isSuccess = streamLoadResult.isSuccess();
            StreamLoadResult finalStreamLoadResult = streamLoadResult;
            batchItems.forEach(item -> {
                long offset = item.getPayload().getOffset();
                if (isSuccess) {
                    statusMap.put(offset, "SUCCESS");
                } else {
                    statusMap.put(offset, "FAILED: " + finalStreamLoadResult.getResponseJson());
                }
            });
        }

        return statusMap;
    }

//    private StreamLoadResult executeStreamLoad(List<BatchInsertItem> batchItems, String tableName,List<ColumnInfo> allColumns) throws Exception {
//        StringBuilder jsonArrayData = new StringBuilder();
//        jsonArrayData.append("[");
//
//        // map数据 转换为json字符串
//        for (int i = 0; i < batchItems.size(); i++) {
//            if (i > 0) {
//                jsonArrayData.append(",");
//            }
//
//            BatchInsertItem item = batchItems.get(i);
//            Map<String, Object> originalData = item.getData();
//            Set<String> dateFieldSet = item.getPayload().getDateFieldSet();
//            appendCdcTimeFields(dateFieldSet);
//
//            // 使用 DorisInsertStrategy 处理数据格式转换，主要是时间格式
//            Map<String, Object> processedData = streamLoadHelper.processDataForStreamLoad(originalData, dateFieldSet);
//
//            // 处理 Doris custom_column_mapping
//            // Doris 且当前表建表属性未声明 cdc_rowid 时，插入前移除该字段
//            preprocessInsertData(tableName, processedData);
//
//            // 再按目标表字段做最终归一化，1.processedData中去掉表中不存在的字段，2.processedData中key的大小写与表字段实际大小写保持一致
//            Map<String, Object> normalizedData = normalizeDataByTargetColumns(processedData, allColumns);
//
//
//            // 直接使用 JSON 序列化，避免手动构建
//            String jsonObject = streamLoadHelper.toJson(normalizedData);
//            jsonArrayData.append(jsonObject);
//        }
//        jsonArrayData.append("]");
//
//        return streamLoadHelper.executeStreamLoad(tableName, jsonArrayData.toString(), Collections.emptyMap());
//    }



    // 通过 Stream Load 执行批量新增，按输入位置返回结果。
    // 日期转换、字段映射及目标字段归一化继续复用现有实现。
    @Override
    public List<String> executeBatchInsertOrdered(
            String tableName,
            List<BatchInsertItem> batchItems,
            BatchSizeManager batchSizeManager,
            List<ColumnInfo> allColumns,
            boolean needPadding) throws Exception {
        // 校验输入，空批次无需发送请求。
        Objects.requireNonNull(batchItems, "新增批次不能为 null");
        if (batchItems.isEmpty()) {
            return new ArrayList<>();
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("新增的目标表名不能为空");
        }

        // 检查必需组件，避免未初始化时进入 HTTP 流程。
        if (streamLoadHelper == null) {
            throw new IllegalStateException("Doris 新增策略尚未初始化");
        }
        Objects.requireNonNull(batchSizeManager, "batchSizeManager 不能为 null");

        // 必须具备目标字段元数据，才能可靠过滤和补齐字段。
        if (allColumns == null || allColumns.isEmpty()) {
            throw new IllegalArgumentException("Doris 新增的目标字段列表不能为空");
        }

        // 复制待写数据，避免字段补齐改写调用方持有的数据。
        List<BatchInsertItem> workingItems = new ArrayList<>(batchItems.size());
        for (BatchInsertItem item : batchItems) {
            // 每条输入必须保留对应位置，不能静默跳过。
            if (item == null || item.getPayload() == null || item.getData() == null) {
                throw new IllegalArgumentException("新增批次包含空 item、payload 或 data");
            }

            // 消息只读使用，待写字段使用独立可修改 Map。
            workingItems.add(new BatchInsertItem(
                    item.getPayload(),
                    new LinkedHashMap<>(item.getData())));
        }

        // 记录耗时，继续沿用 Doris 新增的动态批量大小管理。
        long startTime = System.currentTimeMillis();
        try {
            // 保留上层确定的字段补齐开关，不重新分组或拆批。
            if (needPadding) {
                padFieldsForBatch(workingItems, allColumns);
            }

            // 每条消息使用自己的日期单位，转换后执行映射和目标字段过滤。
            StreamLoadResult result =
                    executeStreamLoad(workingItems, tableName, allColumns);

            // 失败或缺少响应时，不能返回成功列表。
            if (result == null || !result.isSuccess()) {
                String detail = result == null
                        ? "未返回 Stream Load 结果"
                        : result.getResponseJson();
                throw new IllegalStateException(
                        "Doris 批量新增失败或结果未确认，table="
                                + tableName + "，detail=" + detail);
            }

            // 无法定位过滤记录时，禁止将整个输入列表标记为成功。
            if (result.getNumberFilteredRows() != 0
                    || result.getNumberLoadedRows() != workingItems.size()) {
                throw new IllegalStateException(
                        "Doris 批量新增返回数量异常，table=" + tableName
                                + "，submittedRows=" + workingItems.size()
                                + "，loadedRows=" + result.getNumberLoadedRows()
                                + "，filteredRows=" + result.getNumberFilteredRows());
            }

            // 仅在整批结果确认后生成成功列表，长度与输入完全一致。
            List<String> statuses = new ArrayList<>(
                    Collections.nCopies(batchItems.size(), "SUCCESS"));

            // 记录批次摘要，不使用 offset 关联结果。
            logger.info(
                    "Doris 批量新增成功，table={}, rows={}, elapsedMs={}",
                    tableName, workingItems.size(),
                    System.currentTimeMillis() - startTime);

            // 相同 offset 的消息各自占据独立结果位置。
            return statuses;
        } finally {
            // 沿用现有按本次耗时及数据量调整批量大小的行为。
            // 调节异常不能覆盖写入异常，也不能把已确认成功改为失败。
            try {
                batchSizeManager.adjustBatchSizeByPerformance(
                        System.currentTimeMillis() - startTime, workingItems);
            } catch (Exception adjustmentException) {
                logger.warn(
                        "Doris 新增批量大小调整失败，table={}, err={}",
                        tableName, adjustmentException.getMessage());
            }
        }
    }

    // 逐条按日期单位转换数据，完成字段映射和归一化后执行 Stream Load。
    private StreamLoadResult executeStreamLoad(List<BatchInsertItem> batchItems,
                                               String tableName,
                                               List<ColumnInfo> allColumns) throws Exception {
        // 收集处理后的行数据，由 JSON 序列化器统一生成数组。
        List<Map<String, Object>> rows = new ArrayList<>(batchItems.size());

        // 每条消息使用自身的日期字段集合及单位映射。
        for (BatchInsertItem item : batchItems) {
            // 获取当前待写入数据。
            Map<String, Object> originalData = item.getData();

            // 复制日期字段集合，补充 CDC 字段时不修改原消息元数据。
            Set<String> dateFieldSet = new HashSet<>(item.getPayload().getDateFieldSet());
            appendCdcTimeFields(dateFieldSet);

            // 获取当前消息的单位映射，不依赖首条消息。
            Map<String, String> dateFieldUnits = item.getPayload().getDateFieldUnits();

            // 按明确单位解释日期数值，并统一转换为秒级字符串。
            Map<String, Object> processedData = streamLoadHelper.processDataForStreamLoad(originalData,
                                                                                          dateFieldSet,
                                                                                          dateFieldUnits);

            // 日期转换后执行自定义字段映射及 cdc_rowid 保留策略。
            preprocessInsertData(tableName, processedData);

            // 按目标表实际字段过滤数据，并恢复目标字段的大小写。
            Map<String, Object> normalizedData =
                    normalizeDataByTargetColumns(processedData, allColumns);

            // 保持输入顺序加入待加载数组。
            rows.add(normalizedData);
        }

        // 一次序列化完整数组，避免手动拼接 JSON。
        String jsonArrayData = streamLoadHelper.toJson(rows);

        // 严格校验输入，不允许过滤部分记录后提交剩余记录。
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("strict_mode", "true");
        headers.put("max_filter_ratio", "0");

        // 覆盖公共 HTTP 方法的默认配置。
        return streamLoadHelper.executeStreamLoad(
                tableName,
                jsonArrayData,
                headers);

    }

    @Override
    public boolean supportsBatch() {
        return false;
    }

    /**
     * 字段补齐逻辑(Doris专用)
     * @param batchItems 待补齐的数据
     * @param allColumns 目标表所有字段
     */
    private void padFieldsForBatch(List<BatchInsertItem> batchItems, List<ColumnInfo> allColumns) {
        // 当前表的所有字段，
        Set<String> allColumnNames = allColumns.stream()
                .map(ColumnInfo::getColumnName)
                .collect(Collectors.toSet());

        for (BatchInsertItem item : batchItems) {
            Map<String, Object> data = item.getData();
            for (String columnName : allColumnNames) {
                data.putIfAbsent(columnName, null);
            }
        }

        logger.debug("✓ 字段补齐完成: {} 条记录 {} 个字段",
                batchItems.size(), allColumnNames.size());
    }








    @Override
    protected Map<String, Object> prepareSingleInsertData(String tableName,
                                                          Map<String, Object> afterData,
                                                          List<ColumnInfo> allColumns) {
        Map<String, Object> filteredData = super.prepareSingleInsertData(tableName, afterData, allColumns);

        if (filteredData.isEmpty() || allColumns == null || allColumns.isEmpty()) {
            return filteredData;
        }

        return normalizeDataByTargetColumns(filteredData, allColumns);
    }


    // 处理 Doris custom_column_mapping
    // Doris 且当前表建表属性未声明 cdc_rowid 时，插入前移除该字段
    protected void preprocessInsertData(String tableName, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        Map<String, String> customMapping = DorisCustomColumnMappingSupport
                .resolveNormalizedCustomMapping(this.dataSourceHolder.getTargetDataSourceConfig(), tableName);
        DorisCustomColumnMappingSupport.applyCustomMapping(data, customMapping);

        // Doris 且当前表建表属性未声明 cdc_rowid 时，插入前移除该字段
        if (!shouldKeepCdcRowIdForCurrentTable(tableName)) {
            data.remove(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
            data.remove(CdcColumnConstants.CDC_ROWID_COLUMN_UPPER);
        }
    }
}