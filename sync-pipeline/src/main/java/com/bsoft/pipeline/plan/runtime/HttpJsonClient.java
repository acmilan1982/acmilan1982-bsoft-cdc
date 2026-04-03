package com.bsoft.pipeline.plan.runtime;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;

public final class HttpJsonClient {
    private HttpJsonClient() {
    }

    public static String postJson(String url, String body) {
        HttpPost post = new HttpPost(url);
        post.setHeader("Content-Type", "application/json; charset=UTF-8");
        post.setEntity(new StringEntity(body, StandardCharsets.UTF_8));

        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse resp = client.execute(post)) {
            int code = resp.getStatusLine().getStatusCode();
            String respBody = resp.getEntity() == null ? "" : EntityUtils.toString(resp.getEntity(), StandardCharsets.UTF_8);

            if (code < 200 || code >= 300) {
                throw new RuntimeException("HTTP status=" + code + ", body=" + respBody);
            }
            return respBody;
        } catch (Exception e) {
            throw new RuntimeException("HTTP request failed", e);
        }
    }
}

