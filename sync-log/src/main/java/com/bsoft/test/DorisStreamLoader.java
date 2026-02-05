package com.bsoft.test;

/**
 * Doris 3.0 Stream Load (JSON) 示例
 * - 支持：单条 JSON、JSON Lines（逐行）、JSON 数组（外层中括号）
 * - 显式使用 format_version = 2
 * - 演示 columns 与 jsonpaths 两种映射方式
 */

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class DorisStreamLoader {

    // ===== 基本配置 =====
    private static final String HOST = "192.168.1.163"; // FE IP
    private static final int    PORT = 8030;            // FE HTTP 端口
    private static final String DATABASE = "spt_his_2025";
    private static final String TABLE    = "doris_test_sink";
    private static final String USER     = "root";
    private static final String PASSWD   = "bsoft";

    // Stream Load URL: http://fe_host:fe_http_port/api/{db}/{table}/_stream_load
    private static final String LOAD_URL = String.format(
            "http://%s:%d/api/%s/%s/_stream_load", HOST, PORT, DATABASE, TABLE);

    // 允许 307 重定向（FE -> BE）
    private static final HttpClientBuilder httpClientBuilder = HttpClients.custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    // ========== 公共方法 ==========

    private static String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded, StandardCharsets.UTF_8);
    }

    private static void printResponse(CloseableHttpResponse response) throws IOException {
        System.out.println("=== Response Status === " + response.getStatusLine());
        System.out.println("=== Response Headers ===");
        for (Header h : response.getAllHeaders()) {
            System.out.println(h.getName() + ": " + h.getValue());
        }
        String body = response.getEntity() != null ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8) : "";
        System.out.println("=== Response Body ===");
        System.out.println(body);
        // Doris 成功通常 statusCode=200，且 body 中 "Status":"Success"
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            throw new IOException("Stream load failed, httpStatus=" + statusCode + ", body=" + body);
        }
    }

    private static HttpPut basePut(String label) {
        HttpPut put = new HttpPut(LOAD_URL);
        // Doris 建议 Expect:100-continue，可减少大体量请求失败时的浪费
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        System.out.println("=============================");
        System.out.println(basicAuthHeader(USER, PASSWD));
        System.out.println("=============================");

        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(USER, PASSWD));
        put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json; charset=UTF-8");

        // 关键 JSON 相关 header
        put.setHeader("format", "json");
        put.setHeader("format_version", "2");     // Doris 3.x 建议使用 JSON v2 解析器
        put.setHeader("strict_mode", "true");     // 遇到类型不匹配即报错；可按需改为 false
        put.setHeader("max_filter_ratio", "0");   // 过滤比率，0 表示不允许过滤
        put.setHeader("label", label);            // 幂等标签，可用 UUID
        return put;
    }

    // ========== 载入形态 1：单条 JSON 对象 ==========
    // 例：{"id":556393582,"number":"123344","price":"23.5","skuname":"test","skudesc":"zhangfeng_test,test"}
    public void loadSingleJsonObject(String oneJsonObject) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = basePut("json_one_" + UUID.randomUUID());
            // 如果 JSON 字段名与表列名一一对应，最简单：直接 columns 指定列顺序（可选）
            put.setHeader("columns", "id,number,price,skuname,skudesc");

            put.setEntity(new StringEntity(oneJsonObject, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(put)) {
                printResponse(response);
            }
        }
    }

    // ========== 载入形态 2：JSON Lines（每行一个 JSON 对象） ==========
    // 例：
    // {"id":1,"number":"A","price":10.2,"skuname":"x","skudesc":"d1"}
    // {"id":2,"number":"B","price":20.5,"skuname":"y","skudesc":"d2"}
    public void loadJsonLines(String jsonLines) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = basePut("json_lines_" + UUID.randomUUID());
            // JSON Lines 关键：read_json_by_line = true
            put.setHeader("read_json_by_line", "true");
            put.setHeader("columns", "id,number,price,skuname,skudesc");

            put.setEntity(new StringEntity(jsonLines, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(put)) {
                printResponse(response);
            }
        }
    }

    // ========== 载入形态 3：JSON 数组（外层中括号） ==========
    // 例：[{"id":1,...},{"id":2,...}]
    public void loadJsonArray(String jsonArray) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = basePut("json_array_" + UUID.randomUUID());
            // JSON 数组关键：strip_outer_array = true
            put.setHeader("strip_outer_array", "true");
//            put.setHeader("columns", "id,number,price,skuname,skudesc");

            put.setEntity(new StringEntity(jsonArray, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(put)) {
                printResponse(response);
            }
        }
    }

    // ========== 可选：使用 jsonpaths 的映射方式 ==========
    // 当 JSON 字段名与表列名不一致，或存在嵌套字段时，用 jsonpaths + columns 明确映射。
    // 注意：jsonpaths 是一个 JSON 数组字符串，每个路径与 columns 中同位置列对应。
    public void loadWithJsonPaths(String jsonLinesOrArray, boolean isLines, boolean isArray) throws Exception {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            HttpPut put = basePut("json_paths_" + UUID.randomUUID());

            // 映射：columns 的顺序与 jsonpaths 下标一一对应
            put.setHeader("columns", "id,number,price,skuname,skudesc");
            // 假设源 JSON 字段与表名不一致或有嵌套，示例（请按你的实际 JSON 路径填写）：
            // 这里演示：id->$.id, number->$.num, price->$.detail.price, skuname->$.sku.name, skudesc->$.sku.desc
            String jsonpaths = "[\"$.id\",\"$.num\",\"$.detail.price\",\"$.sku.name\",\"$.sku.desc\"]";
            put.setHeader("jsonpaths", jsonpaths);

            if (isLines) {
                put.setHeader("read_json_by_line", "true");
            } else if (isArray) {
                put.setHeader("strip_outer_array", "true");
            }
            // 其余同前
            put.setEntity(new StringEntity(jsonLinesOrArray, StandardCharsets.UTF_8));

            try (CloseableHttpResponse response = client.execute(put)) {
                printResponse(response);
            }
        }
    }

    // ================== DEMO ==================
    public static void main(String[] args) throws Exception {
        DorisStreamLoader loader = new DorisStreamLoader();

//        // 1) 单个 JSON 对象
//        String one = "{\"id\":556393582,\"number\":\"123344\",\"price\":\"23.5\",\"skuname\":\"test\",\"skudesc\":\"zhangfeng_test,test\"}";
//        loader.loadSingleJsonObject(one);
//
//        // 2) JSON Lines（多行，每行一个对象）
//        String lines =
//                "{\"id\":1,\"number\":\"A001\",\"price\":10.20,\"skuname\":\"sku-1\",\"skudesc\":\"desc-1\"}\n" +
//                        "{\"id\":2,\"number\":\"A002\",\"price\":20.50,\"skuname\":\"sku-2\",\"skudesc\":\"desc-2\"}\n";
//        // loader.loadJsonLines(lines);
//
//        // 3) JSON 数组（外层中括号）
        String array =
                "[" +
                        "{\"id\":3,\"number\":\"003\",\"price\":30.00,\"skuname\":\"sku-3\",\"skudesc\":\"desc-3\"}," +
                        "{\"id\":4,\"number\":\"004\",\"price\":40.99,\"skuname\":\"sku-4\",\"skudesc\":\"desc-4\"}" +
                        "]";
         loader.loadJsonArray(array);

        // 4) 使用 jsonpaths（当字段名/结构不一致时）
        // String linesWithDifferentFields =
        //         "{\"id\":5,\"num\":\"B001\",\"detail\":{\"price\":12.34},\"sku\":{\"name\":\"n1\",\"desc\":\"d1\"}}\n" +
        //         "{\"id\":6,\"num\":\"B002\",\"detail\":{\"price\":56.78},\"sku\":{\"name\":\"n2\",\"desc\":\"d2\"}}\n";
        // loader.loadWithJsonPaths(linesWithDifferentFields, true, false);
    }
}
