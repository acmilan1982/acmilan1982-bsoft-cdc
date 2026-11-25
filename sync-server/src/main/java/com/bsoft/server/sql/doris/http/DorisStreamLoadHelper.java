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

// 封装 Doris Stream Load 请求、数据序列化及日期字段转换。
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

//    public StreamLoadResult executePartialUpdate(String tableName,
//                                                 String jsonArrayData,
//                                                 List<String> columns) throws Exception {
//        Map<String, String> headers = new HashMap<>();
//        headers.put("partial_columns", "true");
//        headers.put("columns", String.join(",", columns));
//        return executeStreamLoad(tableName, jsonArrayData, headers);
//    }
// 对当前类 DorisStreamLoadHelper 的局部更新请求启用仅更新已有记录的约束。
    // 一批数据中存在新 Key 或不合法数据时，请求失败，由调用方记录失败状态。
    public StreamLoadResult executePartialUpdate(String tableName,
                                                 String jsonArrayData,
                                                 List<String> columns) throws Exception {
        // 校验目标表名，避免发送无效请求。
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("局部更新的目标表名不能为空");
        }

        // 校验请求数据，实际 JSON 内容由上游序列化生成。
        if (jsonArrayData == null || jsonArrayData.trim().isEmpty()) {
            throw new IllegalArgumentException("局部更新的 JSON 数据不能为空");
        }

        // 局部更新必须显式提供本次写入的列。
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("局部更新的字段列表不能为空");
        }

        // 校验字段并保持调用方确定的顺序，不修改原字段列表。
        List<String> requestColumns = new ArrayList<>(columns.size());
        Set<String> normalizedColumns = new HashSet<>();
        for (String column : columns) {
            // 拒绝空字段名，避免生成不完整的 columns 请求头。
            if (column == null || column.trim().isEmpty()) {
                throw new IllegalArgumentException("局部更新的字段名不能为空");
            }

            // 同一字段不允许因大小写差异而重复出现。
            String columnName = column.trim();
            String normalizedColumn = columnName.toLowerCase(Locale.ROOT);
            if (!normalizedColumns.add(normalizedColumn)) {
                throw new IllegalArgumentException(
                        "局部更新的字段重复: " + columnName
                );
            }

            // 保留目标表字段的实际大小写。
            requestColumns.add(columnName);
        }

        // 本项目统一使用 cdc_rowid 作为目标 UNIQUE KEY。
        if (!normalizedColumns.contains("cdc_rowid")) {
            throw new IllegalArgumentException(
                    "局部更新的字段列表必须包含 cdc_rowid"
            );
        }

        // 仅覆盖明确提供的列，保留未列出的目标字段。
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("partial_columns", "true");
        headers.put("columns", String.join(",", requestColumns));

        // Doris 4 遇到目标不存在的 Key 时直接失败，禁止转为插入。
        headers.put("partial_update_new_key_behavior", "ERROR");

        // 拒绝非法类型转换，并禁止过滤部分数据后提交剩余记录。
        headers.put("strict_mode", "true");
        headers.put("max_filter_ratio", "0");

        // 复用现有 HTTP 实现，额外请求头会覆盖其默认设置。
        StreamLoadResult result = executeStreamLoad(
                tableName,
                jsonArrayData,
                headers
        );

        // 将失败结果转成异常，避免上游因方法正常返回而记录更新成功。
        if (result == null || !result.isSuccess()) {
            String detail = result == null
                    ? "未返回 Stream Load 结果"
                    : result.getResponseJson();

            throw new IllegalStateException(
                    "Doris 局部更新失败，table=" + tableName
                            + "，detail=" + detail
            );
        }

        // 成功响应也必须没有过滤记录，否则不能按整批成功处理。
        if (result.getNumberFilteredRows() != 0) {
            throw new IllegalStateException(
                    "Doris 局部更新存在过滤记录，table=" + tableName
                            + "，filteredRows="
                            + result.getNumberFilteredRows()
            );
        }

        // 返回确认成功且没有过滤记录的结果。
        return result;
    }

    // 使用 Stream Load 按 cdc_rowid 批量物理删除。
// 请求禁止过滤非法记录，失败或结果无法确认时抛出异常。
    public StreamLoadResult executeDelete(String tableName,
                                          String jsonArrayData,
                                          List<String> primaryKeys) throws Exception {
        // 校验目标表名。
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("物理删除的目标表名不能为空");
        }

        // 校验请求数据，实际 JSON 由上层统一序列化生成。
        if (jsonArrayData == null || jsonArrayData.trim().isEmpty()) {
            throw new IllegalArgumentException("物理删除的 JSON 数据不能为空");
        }

        // 本项目目标 UNIQUE KEY 固定为 cdc_rowid。
        if (primaryKeys == null || primaryKeys.size() != 1) {
            throw new IllegalArgumentException(
                    "物理删除必须且只能使用 cdc_rowid 作为主键");
        }

        // 校验字段名，保留调用方提供的实际大小写。
        String primaryKey = primaryKeys.get(0);
        if (primaryKey == null
                || !"cdc_rowid".equalsIgnoreCase(primaryKey.trim())) {
            throw new IllegalArgumentException(
                    "物理删除必须使用 cdc_rowid 作为主键");
        }
        String requestColumn = primaryKey.trim();

        // 指定物理删除及输入列，仅提交行身份。
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("merge_type", "DELETE");
        headers.put("columns", requestColumn);

        // 覆盖公共 HTTP 方法的默认配置，禁止容忍过滤记录。
        headers.put("strict_mode", "true");
        headers.put("max_filter_ratio", "0");

        // 发送一次批量删除请求。
        StreamLoadResult result = executeStreamLoad(
                tableName, jsonArrayData, headers);

        // 将失败响应转为异常，避免调用方误记为成功。
        if (result == null || !result.isSuccess()) {
            String detail = result == null
                    ? "未返回 Stream Load 结果"
                    : result.getResponseJson();
            throw new IllegalStateException(
                    "Doris 物理删除失败或结果未确认，table="
                            + tableName + "，detail=" + detail);
        }

        // 即使响应成功，也不接受存在过滤记录的结果。
        if (result.getNumberFilteredRows() != 0) {
            throw new IllegalStateException(
                    "Doris 物理删除存在过滤记录，table=" + tableName
                            + "，filteredRows=" + result.getNumberFilteredRows());
        }

        // 上层策略继续检查加载记录数是否与提交记录数一致。
        return result;
    }

    public String toJson(Object data) throws Exception {
        return mapper.writeValueAsString(data);
    }

//    public Map<String, Object> processDataForStreamLoad(Map<String, Object> originalData, Set<String> dateFieldSet) {
//        if (dateFieldSet == null || dateFieldSet.isEmpty()) {
//            return originalData;
//        }
//        Map<String, Object> processedData = new HashMap<>(originalData);
//
//
//
//        for (String dateField : dateFieldSet) {
//            Object value = processedData.get(dateField);
//            if (value != null) {
//                processedData.put(dateField, adapter.fastParseDateForKafka(value));
//            }
//        }
//
//
//
//        return processedData;
//    }

    // 根据每条消息的日期单位转换数据，返回副本，日期统一输出到秒。
    public Map<String, Object> processDataForStreamLoad(
            Map<String, Object> originalData,
            Set<String> dateFieldSet,
            Map<String, String> dateFieldUnits) {
        // 日期元数据必须非 null，没有源日期字段时允许为空集合。
        Objects.requireNonNull(originalData, "originalData 不能为 null");
        Objects.requireNonNull(dateFieldSet, "dateFieldSet 不能为 null");
        Objects.requireNonNull(dateFieldUnits, "dateFieldUnits 不能为 null");

        // 复制数据，避免日期转换修改原始消息或批量插入数据。
        Map<String, Object> processedData = new LinkedHashMap<>(originalData);

        // 统一日期字段名大小写，支持现有 cdc_dml_Time 等字段。
        Set<String> normalizedDateFields = new HashSet<>();
        for (String fieldName : dateFieldSet) {
            normalizedDateFields.add(fieldName.toLowerCase(Locale.ROOT));
        }

        // 遍历实际数据，不为当前记录中缺失的日期字段补充值。
        for (Map.Entry<String, Object> entry : processedData.entrySet()) {
            String fieldName = entry.getKey();
            String normalizedFieldName = fieldName.toLowerCase(Locale.ROOT);

            // 非日期字段保持原值。
            if (!normalizedDateFields.contains(normalizedFieldName)) {
                continue;
            }

            // 空日期保持 null，不查单位、不进行转换。
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }

            // CDC 时间由服务端生成，不要求源表单位映射。
            boolean cdcTimeField = isCdcTimeField(normalizedFieldName);
            String unit;
            if (cdcTimeField) {
                // CDC 时间必须使用日期对象或日期字符串，拒绝数字时间戳。
                if (value instanceof Number
                        || (value instanceof String
                        && ((String) value).trim().matches("[+-]?[0-9]+"))) {
                    throw new IllegalArgumentException(
                            "CDC 时间字段必须使用日期对象或日期字符串，字段: "
                                    + fieldName
                    );
                }

                // ms 仅用于进入合法的日期转换入口，不对日期对象或日期字符串缩放。
                unit = "ms";
            } else {
                // 非空源日期字段必须提供明确单位。
                unit = dateFieldUnits.get(normalizedFieldName);
                if (!"ms".equals(unit)
                        && !"us".equals(unit)
                        && !"ns".equals(unit)) {
                    throw new IllegalArgumentException(
                            "日期字段缺少有效时间单位，字段: " + fieldName
                                    + "，单位: " + unit
                                    + "，仅支持 ms、us、ns"
                    );
                }
            }

            // 通过 DorisAdapter 解析单位并输出秒级字符串。
            try {
                entry.setValue(adapter.fastParseDateForKafka(value, unit));
            } catch (IllegalArgumentException exception) {
                // 转换失败时补充字段上下文并向外抛出，禁止静默继续加载。
                throw new IllegalArgumentException(
                        "Stream Load 日期字段转换失败，字段: " + fieldName
                                + "，单位: " + unit,
                        exception
                );
            }
        }

        // 返回转换后的数据，字段名称保持原样供后续映射和归一化使用。
        return processedData;
    }

    // 识别服务端生成的 CDC 时间字段，参数使用小写字段名。
    private boolean isCdcTimeField(String fieldName) {
        // 保持与 DorisAdapter 相同的 CDC 时间字段范围。
        switch (fieldName) {
            case "cdc_dml_time":
            case "cdc_kafka_enqueue_time":
            case "cdc_insert_time":
            case "cdc_update_time":
            case "cdc_delete_time":
                // CDC 时间不依赖源表单位。
                return true;
            default:
                // 其他日期字段必须使用消息中的单位。
                return false;
        }
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