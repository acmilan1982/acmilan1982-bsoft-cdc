package com.bsoft.server.sql.insert;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.SyncLogEntry;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.log.LogCollector;
import com.bsoft.server.batch.BatchInsertItem;
import com.bsoft.server.batch.BatchSizeManager;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import com.bsoft.server.sql.insert.http.DorisConnectionInfo;
import com.bsoft.server.sql.insert.http.StreamLoadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Doris 单条插入策略实现
 * - 仅构建单条 INSERT INTO ... VALUES(...) 语句
 * - 不实现 "存在则不插入"；对 UNIQUE/PRIMARY KEY 表，Doris 将表现为 UPSERT（存在即覆盖）
 */
public class DorisInsertStrategy extends AbstractInsertStrategy {

    private static final Logger logger = LogManager.getLogger(DorisInsertStrategy.class);

    // 负责doris使用streamLoad写入时，把数据转换成json
    private ObjectMapper mapper;

    // Doris连接信息
    private DorisConnectionInfo dorisInfo;

    public DorisInsertStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        super(targetDataSourceConfig);
        // 使用工厂创建适配器
        this.adapter = DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.mapper = new ObjectMapper();
        // 解析Doris连接信息
        this.dorisInfo = new DorisConnectionInfo(targetDataSourceConfig.getDataSourceHost(),
                                        8030,  // 该端口目前写死
                                                targetDataSourceConfig.getDataSourceServiceName());


    }



    @Override
    public String buildInsertSql(String tableName, List<String> keys, Set<String> primaryKeys, Set<String> dateFieldSet) {
        String columns = keys.stream().map(String::toLowerCase).collect(Collectors.joining(","));

        String placeholders = keys.stream().map(k -> "?").collect(Collectors.joining(","));

        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName.toLowerCase(), columns, placeholders);
    }

    @Override
    public void setParameters(PreparedStatement ps, Map<String, Object> data, List<String> keyList, Set<String> dateFieldSet) throws SQLException {
        if (keyList == null || keyList.isEmpty()) {
            throw new IllegalArgumentException("columns(keys) is empty");
        }
        if (data == null) {
            throw new IllegalArgumentException("data is null");
        }


        int index = 1;
        // 按照keys的顺序设置参数值
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
        Map<Long, String> statusMap = new HashMap<>(); // offset -> status/error



        // Stream Load执行结果
        // 初始化默认失败结果，避免空指针
        StreamLoadResult streamLoadResult = new StreamLoadResult(false, 0, 0, "执行前异常");

        try {
            // ✅ 如果需要字段补齐,先执行补齐逻辑
            if (needPadding && allColumns != null && !allColumns.isEmpty()) {
                logger.info("🔧 开始字段补齐: 目标表 {} 共 {} 个字段", tableName, allColumns.size());
                padFieldsForBatch(batchItems, allColumns);
            }

            // 执行 Stream Load
            streamLoadResult = executeStreamLoad(batchItems,tableName);

            // 根据执行成功与否输出日志
            if (streamLoadResult.isSuccess()) {
                int loadedRows = streamLoadResult.getNumberLoadedRows();
                int filteredRows = streamLoadResult.getNumberFilteredRows();
                logger.info("Doris Stream Load 成功: 本组记录供 {} 条，实际成功 {} 条, 过滤 {} 条,执行耗时 {} 秒", batchItems.size(), loadedRows, filteredRows,(System.currentTimeMillis() - startTime)/ 1000.0);

            } else {
                logger.info("Doris Stream Load 失败，错误信息: {}",streamLoadResult.getResponseJson());
            }

        } catch (Exception e) {
            e.printStackTrace();
            // 更新失败结果
            streamLoadResult = new StreamLoadResult(
                    false,
                    0,
                    0,
                    "执行异常: " + LogCollector.getSafeStackTraceHead(e, 40, 4000));

        }finally {
            // 根据执行性能和数据大小综合调整批次大小
            batchSizeManager.adjustBatchSizeByPerformance(System.currentTimeMillis() - startTime, batchItems);

            boolean isSuccess = streamLoadResult.isSuccess();
            StreamLoadResult finalStreamLoadResult = streamLoadResult;
            batchItems.stream().forEach(item -> {;
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




    // Stream Load批量插入
    private StreamLoadResult executeStreamLoad(List<BatchInsertItem> batchItems,String tableName) throws Exception {
        // Stream Load的URL
        String streamLoadUrl = String.format("http://%s:%d/api/%s/%s/_stream_load", this.dorisInfo.getHost(),
                                                                                    this.dorisInfo.getHttpPort(),
                                                                                    this.dorisInfo.getDatabase(),
                                                                                    tableName);

        // 构建 JSON 数组数据
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
            Map<String, Object> processedData = processDataForStreamLoad(originalData, dateFieldSet);


            // 直接使用 JSON 序列化，避免手动构建
            String jsonObject = convertMapToJsonString(processedData);
            jsonArrayData.append(jsonObject);
        }
        jsonArrayData.append("]");

        // 使用 Apache HttpClient（与DorisStreamLoader完全一致）
        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(streamLoadUrl);

            // 设置请求头（完全参考DorisStreamLoader的方式）
            put.setHeader(HttpHeaders.EXPECT, "100-continue");

            // 认证头设置
            String basicAuth = basicAuthHeader();
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuth);

            put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");

            // JSON 格式相关头部
            put.setHeader("format", "json");
            put.setHeader("format_version", "2");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("strict_mode", "false");        // 改为 false，更宽松
            put.setHeader("max_filter_ratio", "0.1");     // 允许 10% 的过滤率
            put.setHeader("timeout", "300");

            String label = "json_array_" + UUID.randomUUID();
            put.setHeader("label", label);

            // 设置请求体
            put.setEntity(new StringEntity(jsonArrayData.toString(), StandardCharsets.UTF_8));


            // 调试输出
//            logger.info("🔐 Stream Load URL: {}", streamLoadUrl);
//            logger.info("🔐 标签: {}", label);

//            // 输出JSON数据的前500字符用于调试
//            String jsonPreview = jsonArrayData.length() > 500 ?
//                    jsonArrayData.substring(0, 500) + "..." : jsonArrayData.toString();
//            logger.info("🔍 JSON数据预览: {}", jsonPreview);

            // 执行请求
            try (CloseableHttpResponse response = client.execute(put)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";

//                logger.info("🔍 HTTP响应码: {}", statusCode);
//                logger.info("🔍 响应内容: {}", responseBody);

                return parseStreamLoadResponse(statusCode, responseBody);
            }
        }
    }
    // 辅助方法：将 Map 转换为 JSON 字符串
    private String convertMapToJsonString(Map<String, Object> data) {
        try {
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            // 如果 Jackson 不可用，降级到手动构建（但已经简化）
            logger.warn("⚠️ Jackson 序列化失败，使用手动构建: {}", e.getMessage());
            return null;
        }

    }
    // 认证头设置
    private String basicAuthHeader() {
        String username = this.dataSourceHolder.getTargetDataSourceConfig().getDataSourceUserName();
        String password = this.dataSourceHolder.getTargetDataSourceConfig().getDataSourcePassword();
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        String basicAuth = "Basic " + new String(encoded, StandardCharsets.UTF_8);
        return basicAuth;
    }

    // 解析Stream Load响应
    private StreamLoadResult parseStreamLoadResponse(int responseCode, String responseBody) {
        try {
            if (responseCode != 200) {
                return new StreamLoadResult(false,  0, 0,responseBody);
            }

            // 使用 Jackson 解析 响应JSON
            Map<String, Object> jsonMap = mapper.readValue(responseBody, Map.class);
            // Success/Fail
            String status = (String) jsonMap.get("Status");

            if ("Success".equals(status)) {
                int loadedRows = ((Number) jsonMap.getOrDefault("NumberLoadedRows", 0)).intValue();
                int filteredRows = ((Number) jsonMap.getOrDefault("NumberFilteredRows", 0)).intValue();
                return new StreamLoadResult(true, loadedRows, filteredRows,responseBody);
            } else {
                return new StreamLoadResult(false, 0, 0,responseBody);
            }
        } catch (Exception e) {
            logger.error("❌ 解析 Stream Load 响应失败: {}", e.getMessage());
            return new StreamLoadResult(false, 0, 0,responseBody);
        }
    }


    @Override
    public boolean supportsBatch() {
        return false; // 仅单条插入
    }

    // ---- helpers ----

    private String quote(String ident) {
        if (ident == null) return "``";
        return "`" + ident.replace("`", "") + "`";
    }

    private void bind(PreparedStatement ps, int index, Object v) throws SQLException {
        if (v == null) {
            ps.setObject(index, null);
            return;
        }



        // 将常见时间类型规范为 Timestamp，方便映射 Doris 的 DATETIME
        if (v instanceof java.util.Date && !(v instanceof java.sql.Date) && !(v instanceof java.sql.Timestamp)) {
            ps.setTimestamp(index, new Timestamp(((java.util.Date) v).getTime()));
        } else if (v instanceof Instant) {
            ps.setTimestamp(index, Timestamp.from((Instant) v));
        } else {
            ps.setObject(index, v);
        }
    }




    /**
     * 处理数据格式转换，特别是时间戳字段
     * @param originalData 原始数据
     * @param dateFieldSet 日期字段集合
     * @return 转换后的数据
     */
    public Map<String, Object>
    processDataForStreamLoad(Map<String, Object> originalData, Set<String> dateFieldSet) {
        if (dateFieldSet == null || dateFieldSet.isEmpty()) {
            return originalData;
        }

        Map<String, Object> processedData = new HashMap<>(originalData);

        for (String dateField : dateFieldSet) {
            // 时间字段的原始值
            Object value = processedData.get(dateField);
            if (value != null) {

                processedData.put(dateField, this.adapter.fastParseDateForKafka(value));
            }
        }

        return processedData;
    }

//    /**
//     * 将时间戳转换为 Doris 支持的格式
//     */
//    private String formatTimestampForDoris(Object value) {
//        if (value == null) return null;
//
//        try {
//            long timestamp;
//            if (value instanceof Number) {
//                timestamp = ((Number) value).longValue();
//            } else if (value instanceof String) {
//                timestamp = Long.parseLong((String) value);
//            } else {
//                return value.toString();
//            }
//
//            LocalDateTime dateTime = LocalDateTime.ofInstant(
//                    Instant.ofEpochMilli(timestamp),
//                    ZoneId.systemDefault()
//            );
//            return dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//        } catch (Exception e) {
//            e.printStackTrace();
//            return value.toString();
//        }
//    }

    /**
     * 字段补齐逻辑(Doris专用)
     * @param batchItems 待补齐的数据
     * @param allColumns 目标表所有字段
     */
    private void padFieldsForBatch(List<BatchInsertItem> batchItems, List<ColumnInfo> allColumns) {
        // 提取所有字段名
        Set<String> allColumnNames = allColumns.stream()
                .map(ColumnInfo::getColumnName)
                .collect(Collectors.toSet());

        // 对每条记录进行字段补齐
        for (BatchInsertItem item : batchItems) {
            Map<String, Object> data = item.getData();

            // 补齐缺失字段为 null
            for (String columnName : allColumnNames) {
                data.putIfAbsent(columnName, null);
            }
        }

        logger.debug("✅ 字段补齐完成: {} 条记录, {} 个字段",
                batchItems.size(), allColumnNames.size());
    }


}
