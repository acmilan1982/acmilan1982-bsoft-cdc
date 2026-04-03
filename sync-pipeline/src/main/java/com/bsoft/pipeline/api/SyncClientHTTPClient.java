package com.bsoft.pipeline.api;

import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.dbmeta.OracleColumnsQueryRequest;
import com.bsoft.common.dbmeta.OracleColumnsQueryResult;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.http.AbstractHTTPClient;
import com.bsoft.common.http.ApiResponse;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

import javax.annotation.PostConstruct;
import java.util.UUID;

/**
 * Query metadata from sync-client for pipeline runtime.
 */
public class SyncClientHTTPClient extends AbstractHTTPClient {

    private static final String TABLE_COLUMNS_QUERY_PATH = "/sync-client/api/tables/columns/query";
    private static final String SQL_COLUMNS_QUERY_PATH = "/sync-client/api/tables/sql-columns/query";

    private PipeLineInfo pipeLineInfo;

    public SyncClientHTTPClient() {
    }

    public SyncClientHTTPClient(PipeLineInfo pipeLineInfo) {
        this.pipeLineInfo = pipeLineInfo;
    }

    @PostConstruct
    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(pipeLineInfo);
    }

    /**
     * Query table columns metadata only (no landing).
     */
    public OracleColumnsQueryResult getTableSchema(String clientId, OracleColumnsQueryRequest request) throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }

        String normalizedClientId = normalizeClientId(clientId);
        String clientIP = resolveClientIP(normalizedClientId);
        HttpEntity<OracleColumnsQueryRequest> httpRequest = new HttpEntity<>(request, buildJsonHeaders());

        String queryUrl = buildUrl(clientIP, TABLE_COLUMNS_QUERY_PATH);
        try {
            ResponseEntity<OracleColumnsQueryResult> response =
                    restTemplate.postForEntity(queryUrl, httpRequest, OracleColumnsQueryResult.class);
            return response.getBody();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("call to table columns query endpoint failed: " + e.getMessage(), e);
        }
    }

    /**
     * Query SQL result-set columns metadata only (no landing).
     */
    public OracleColumnsQueryResult getSqlColumnsSchema(String clientId, OracleSqlColumnsQueryRequest request) throws Exception {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }

        String normalizedClientId = normalizeClientId(clientId);
        String clientIP = resolveClientIP(normalizedClientId);
        String traceId = UUID.randomUUID().toString();

        HttpHeaders headers = buildJsonHeaders();
        headers.set("X-Trace-Id", traceId);
        HttpEntity<OracleSqlColumnsQueryRequest> entity = new HttpEntity<>(request, headers);

        String url = buildUrl(clientIP, SQL_COLUMNS_QUERY_PATH);
        try {
            ResponseEntity<ApiResponse<OracleColumnsQueryResult>> resp = restTemplate.exchange(
                    url,
                    HttpMethod.POST,
                    entity,
                    new ParameterizedTypeReference<ApiResponse<OracleColumnsQueryResult>>() {}
            );
            return unwrapApiResponse(resp.getBody(), traceId);
        } catch (HttpStatusCodeException e) {
            throw new IllegalStateException(
                    "http call failed: status=" + e.getStatusCode() + ", body=" + e.getResponseBodyAsString(), e
            );
        }
    }

    private String resolveClientIP(String clientId) throws Exception {
        String normalizedClientId = normalizeClientId(clientId);
        waitUntilClientAlive(normalizedClientId);
        return getClientIP(normalizedClientId);
    }

    private String normalizeClientId(String clientId) {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId must not be blank");
        }
        return clientId.trim();
    }

    private String buildUrl(String clientIP, String path) {
        return "http://" + clientIP + path;
    }

    private OracleColumnsQueryResult unwrapApiResponse(ApiResponse<OracleColumnsQueryResult> body, String traceId) {
        if (body == null) {
            throw new IllegalStateException("response body is null, traceId=" + traceId);
        }
        if (!body.isSuccess()) {
            throw new IllegalStateException(
                    "call failed: code=" + body.getCode() + ", message=" + body.getMessage() + ", traceId=" + body.getTraceId()
            );
        }
        if (body.getData() == null) {
            throw new IllegalStateException("response data is null, traceId=" + body.getTraceId());
        }
        return body.getData();
    }
}
