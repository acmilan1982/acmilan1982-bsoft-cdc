package com.bsoft.server.sql.doris.http;

import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import com.bsoft.server.sql.doris.DorisCustomColumnMappingSupport;
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
import java.util.*;

public class DorisStreamLoadHelper {

    private static final Logger logger = LogManager.getLogger(DorisStreamLoadHelper.class);

    private final DorisConnectionInfo dorisInfo;
    private final TargetDataSourceConfig targetDataSourceConfig;
    private final DatabaseAdapter adapter;
    private final ObjectMapper mapper;

    public DorisStreamLoadHelper(TargetDataSourceConfig targetDataSourceConfig, DatabaseAdapter adapter) {
        this.targetDataSourceConfig = targetDataSourceConfig;
        this.adapter = adapter;
        this.mapper = new ObjectMapper();
        this.dorisInfo = new DorisConnectionInfo(
                targetDataSourceConfig.getDataSourceHost(),
                8030,
                targetDataSourceConfig.getDataSourceServiceName()
        );
    }

    public StreamLoadResult executeStreamLoad(String tableName,
                                              String jsonArrayData,
                                              Map<String, String> extraHeaders) throws Exception {
        String streamLoadUrl = String.format(
                "http://%s:%d/api/%s/%s/_stream_load",
                dorisInfo.getHost(),
                dorisInfo.getHttpPort(),
                dorisInfo.getDatabase(),
                tableName
        );

        // 使用 Apache HttpClient（与DorisStreamLoader完全一致）
        HttpClientBuilder httpClientBuilder = HttpClients.custom()
                .setRedirectStrategy(new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                });

        String label = "doris_http_" + UUID.randomUUID();

        // 统一打印请求详情
        logger.info("🔍 Stream Load URL: {}", streamLoadUrl);
        logger.info("🔍 Label: {}", label);

        String jsonPreview = jsonArrayData.length() > 500
                ? jsonArrayData.substring(0, 500) + "..."
                : jsonArrayData;
        logger.info("🔍 JSON Preview: {}", jsonPreview);

        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = new HttpPut(streamLoadUrl);

            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader());
            put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");

            put.setHeader("format", "json");
            put.setHeader("format_version", "2");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("strict_mode", "false");
            put.setHeader("max_filter_ratio", "0.1");
            put.setHeader("timeout", "300");
            put.setHeader("label", label);

            if (extraHeaders != null) {
                for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
                    put.setHeader(entry.getKey(), entry.getValue());
                }
            }

            // 设置请求体
            put.setEntity(new StringEntity(jsonArrayData, StandardCharsets.UTF_8));

            // 执行请求
            try (CloseableHttpResponse response = client.execute(put)) {
                int statusCode = response.getStatusLine().getStatusCode();
                String responseBody = response.getEntity() != null
                        ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)
                        : "";

                // 统一打印响应详情
                logger.info("🔍 HTTP 响应码: {}", statusCode);
                logger.info("🔍 响应内容: {}", responseBody);

                return parseStreamLoadResponse(statusCode, responseBody);
            }
        }
    }

    public StreamLoadResult executePartialUpdate(String tableName,
                                                 String jsonArrayData,
                                                 List<String> columns) throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("partial_columns", "true");
        headers.put("columns", String.join(",", columns));
        return executeStreamLoad(tableName, jsonArrayData, headers);
    }

    public StreamLoadResult executeDelete(String tableName,
                                          String jsonArrayData,
                                          List<String> primaryKeys) throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("merge_type", "DELETE");
        headers.put("columns", String.join(",", primaryKeys));
        return executeStreamLoad(tableName, jsonArrayData, headers);
    }

    public String toJson(Object data) throws Exception {
        return mapper.writeValueAsString(data);
    }

    public Map<String, Object> processDataForStreamLoad(Map<String, Object> originalData, Set<String> dateFieldSet) {
        if (dateFieldSet == null || dateFieldSet.isEmpty()) {
            return originalData;
        }
        Map<String, Object> processedData = new HashMap<>(originalData);



        for (String dateField : dateFieldSet) {
            Object value = processedData.get(dateField);
            if (value != null) {
                processedData.put(dateField, adapter.fastParseDateForKafka(value));
            }
        }



        return processedData;
    }

    private String basicAuthHeader() {
        String username = targetDataSourceConfig.getDataSourceUserName();
        String password = targetDataSourceConfig.getDataSourcePassword();
        String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded, StandardCharsets.UTF_8);
    }

    private StreamLoadResult parseStreamLoadResponse(int responseCode, String responseBody) {
        try {
            if (responseCode != 200) {
                return new StreamLoadResult(false, 0, 0, responseBody);
            }

            // 使用 Jackson 解析 响应JSON
            Map<String, Object> jsonMap = mapper.readValue(responseBody, Map.class);
            // Success/Fail
            String status = (String) jsonMap.get("Status");

            if ("Success".equals(status)) {
                int loadedRows = ((Number) jsonMap.getOrDefault("NumberLoadedRows", 0)).intValue();
                int filteredRows = ((Number) jsonMap.getOrDefault("NumberFilteredRows", 0)).intValue();
                return new StreamLoadResult(true, loadedRows, filteredRows, responseBody);
            }

            return new StreamLoadResult(false, 0, 0, responseBody);
        } catch (Exception e) {
            logger.error("parse stream load response failed: {}", e.getMessage());
            return new StreamLoadResult(false, 0, 0, responseBody);
        }
    }


    // 提取主键字段名
    public Set<String> resolvePrimaryKeySet(Set<String> primaryKeySetFromPayload) {
        Set<String> primaryKeySet = new LinkedHashSet<>();

        if (primaryKeySetFromPayload != null && !primaryKeySetFromPayload.isEmpty()) {
            primaryKeySet.addAll(primaryKeySetFromPayload);
        }

        if (primaryKeySet.isEmpty()) {
            primaryKeySet.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);
        }

        return primaryKeySet;
    }
}