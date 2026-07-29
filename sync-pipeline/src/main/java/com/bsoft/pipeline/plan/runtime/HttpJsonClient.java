package com.bsoft.pipeline.plan.runtime;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;


public final class HttpJsonClient {
    private final CloseableHttpClient client;

    public HttpJsonClient(int maxTotal,
                          int maxPerRoute,
                          int connectTimeoutMs,
                          int socketTimeoutMs,
                          int connectionRequestTimeoutMs) {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(maxTotal);
        connManager.setDefaultMaxPerRoute(maxPerRoute);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeoutMs)
                .setSocketTimeout(socketTimeoutMs)
                .setConnectionRequestTimeout(connectionRequestTimeoutMs)
                .build();

        this.client = HttpClients.custom()
                .setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .disableAutomaticRetries()
                .evictExpiredConnections()
                .evictIdleConnections(30, TimeUnit.SECONDS)
                .build();
    }

    public String postJson(String url, String body) {
        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json; charset=UTF-8");
        post.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));

        try (CloseableHttpResponse resp = client.execute(post)) {
            int code = resp.getStatusLine().getStatusCode();
            HttpEntity entity = resp.getEntity();
            String respBody = entity == null ? "" : EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (code < 200 || code >= 300) {
                throw new RuntimeException(
                        "HTTP status=" + code + ", url=" + url + ", body=" + truncate(respBody, 2000));
            }
            return respBody;
        } catch (IOException e) {
            throw new RuntimeException("HTTP request failed, url=" + url + ", cause=" + e.getMessage(), e);
        }
    }

    public void close() throws IOException {
        client.close();
    }

    private static String truncate(String value, int maxLen) {
        if (value == null || value.length() <= maxLen) {
            return value;
        }
        return value.substring(0, maxLen) + "...";
    }
}

