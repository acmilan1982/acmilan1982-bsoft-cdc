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

    @Override
    public void setParameters(java.sql.PreparedStatement ps,
                              Map<String, Object> data,
                              List<String> keyList,
                              Set<String> dateFieldSet) throws java.sql.SQLException {
        if (keyList == null || keyList.isEmpty()) {
            throw new IllegalArgumentException("columns(keys) is empty");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is null");
        }

        int index = 1;
        for (String key : keyList) {
            Object value = data.get(key);
            this.adapter.setParameterValue(ps, index++, value, key, dateFieldSet);
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

    private StreamLoadResult executeStreamLoad(List<BatchInsertItem> batchItems, String tableName,List<ColumnInfo> allColumns) throws Exception {
        StringBuilder jsonArrayData = new StringBuilder();
        jsonArrayData.append("[");

        // map数据 转换为json字符串
        for (int i = 0; i < batchItems.size(); i++) {
            if (i > 0) {
                jsonArrayData.append(",");
            }

            BatchInsertItem item = batchItems.get(i);
            Map<String, Object> originalData = item.getData();
            Set<String> dateFieldSet = item.getPayload().getDateFieldSet();
            appendCdcTimeFields(dateFieldSet);

            // 使用 DorisInsertStrategy 处理数据格式转换，主要是时间格式
            Map<String, Object> processedData = streamLoadHelper.processDataForStreamLoad(originalData, dateFieldSet);

            // 处理 Doris custom_column_mapping
            // Doris 且当前表建表属性未声明 cdc_rowid 时，插入前移除该字段
            preprocessInsertData(tableName, processedData);

            // 再按目标表字段做最终归一化，1.processedData中去掉表中不存在的字段，2.processedData中key的大小写与表字段实际大小写保持一致
            Map<String, Object> normalizedData = normalizeDataByTargetColumns(processedData, allColumns);


            // 直接使用 JSON 序列化，避免手动构建
            String jsonObject = streamLoadHelper.toJson(normalizedData);
            jsonArrayData.append(jsonObject);
        }
        jsonArrayData.append("]");

        return streamLoadHelper.executeStreamLoad(tableName, jsonArrayData.toString(), Collections.emptyMap());
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