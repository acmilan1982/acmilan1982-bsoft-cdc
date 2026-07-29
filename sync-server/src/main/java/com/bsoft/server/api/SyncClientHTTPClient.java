package com.bsoft.server.api;

import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.dbmeta.OracleColumnsQueryResult;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.zk.ZkClientRegistry;
import javax.annotation.PreDestroy;
import com.bsoft.common.http.AbstractHTTPClient;
import com.bsoft.common.http.ApiResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestClientException;

import javax.annotation.PostConstruct;
import java.util.Objects;
import java.util.UUID;

/**
 * ClassName: ServerClient
 * Package:   com.bsoft.client.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:38
 */


public class SyncClientHTTPClient extends AbstractHTTPClient {

    private static final Logger logger = LogManager.getLogger(SyncClientHTTPClient.class);


    private static final ParameterizedTypeReference<ApiResponse<Void>> VOID_API_RESPONSE =
            new ParameterizedTypeReference<ApiResponse<Void>>() {};


    // 客户端信息
    private ServerInfo serverInfo;


    public SyncClientHTTPClient() {
    }

    public SyncClientHTTPClient(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @PostConstruct
    public void init() throws Exception {
        this.zkClient = ZkClientRegistry.acquire(serverInfo.getZooKeeperConfig());
    }

    // 向探针端请求表结构信息，用于建表或扩展字段，异步投递
    public void requestTableDdlExport(TableDDLRequest request) throws Exception {
        Objects.requireNonNull(request, "request cannot be null");
        String clientId = resolveClientId(request);

        postAsyncExport(
                clientId,
                "/sync-client/api/tables/ddl/export",
                request,
                "table-ddl-export"
        );
    }

    // 向探针端请求 sql 语句执行结果信息，用于建表或扩展字段，异步投递
    public void requestSqlColumnsExport(OracleSqlColumnsQueryRequest request) throws Exception {
        Objects.requireNonNull(request, "request cannot be null");
        String clientId = resolveClientId(request);

        postAsyncExport(
                clientId,
                "/sync-client/api/tables/sql-columns/export",
                request,
                "sql-columns-export"
        );
    }


    private <T> void postAsyncExport(String clientId, String path, T requestBody, String operation) {
        // 等待客户端上线
        waitUntilClientAlive(clientId);

        // 重新获取客户端IP，防止IP变更后无法连接
        String clientIP = getClientIP(clientId);
        String url = "http://" + clientIP + path;
        String traceId = UUID.randomUUID().toString();

        HttpHeaders headers = buildJsonHeaders();
        headers.set("X-Trace-Id", traceId);

        HttpEntity<T> entity = new HttpEntity<>(requestBody, headers);

        ResponseEntity<ApiResponse<Void>> response;
        try {
            response = restTemplate.exchange(url, HttpMethod.POST, entity, VOID_API_RESPONSE);
        } catch (RestClientException e) {
            throw new IllegalStateException(buildTransportErrorMessage(operation, clientId, url, traceId), e);
        }

        validateAsyncAck(operation, clientId, url, traceId, response);
    }

    private void validateAsyncAck(
            String operation,
            String clientId,
            String url,
            String traceId,
            ResponseEntity<ApiResponse<Void>> response
    ) {
        HttpStatus status = response.getStatusCode();
        ApiResponse<Void> body = response.getBody();

        if (status != HttpStatus.ACCEPTED || body == null || !body.isSuccess()) {
            throw new IllegalStateException(buildAckErrorMessage(operation, clientId, url, traceId, status, body));
        }

        logger.info(
                "async export accepted: operation={}, clientId={}, status={}, localTraceId={}, remoteTraceId={}",
                operation,
                clientId,
                status.value(),
                traceId,
                body.getTraceId()
        );
    }

    private String buildTransportErrorMessage(String operation, String clientId, String url, String traceId) {
        return "async export request failed"
                + ", operation=" + operation
                + ", clientId=" + clientId
                + ", url=" + url
                + ", traceId=" + traceId;
    }

    private String buildAckErrorMessage(
            String operation,
            String clientId,
            String url,
            String traceId,
            HttpStatus status,
            ApiResponse<Void> body
    ) {
        return "async export ack invalid"
                + ", operation=" + operation
                + ", clientId=" + clientId
                + ", url=" + url
                + ", status=" + status.value()
                + ", code=" + (body == null ? "null" : body.getCode())
                + ", message=" + (body == null ? "null" : body.getMessage())
                + ", localTraceId=" + traceId
                + ", remoteTraceId=" + (body == null ? "null" : body.getTraceId());
    }


//
//    // 向探针端请求表结构信息，用于建表或扩展字段，异步投递
//    public void requestTableDdlExport(TableDDLRequest tableDDLRequest) throws Exception {
//        // 等待客户端上线
//        waitUntilClientAlive(tableDDLRequest);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.APPLICATION_JSON);
//
//
//        String clientIP = getClientIP(tableDDLRequest);
//        String getTableDDLURL = "http://" + clientIP + "/sync-client/api/tables/ddl/export";
//
//        // 向客户端发送请求
//        HttpEntity request = new HttpEntity<>(tableDDLRequest, headers);
//        ResponseEntity<String> response = restTemplate.postForEntity(getTableDDLURL, request, String.class);
//        System.out.println(response.getBody());
//        logger.info("客户端的响应: \n{}", response.getBody());
//
//    }
//
//
//    // 向探针端请求 sql 语句执行结果信息，用于建表或扩展字段，异步投递
//    public void requestSqlColumnsExport(OracleSqlColumnsQueryRequest request) throws Exception {
//        String clientId = request.getClientId().trim();
//        waitUntilClientAlive(clientId);
//
//        String clientIP = getClientIP(clientId);
//        String url = "http://" + clientIP + "/sync-client/api/tables/sql-columns/export";
//        String traceId = java.util.UUID.randomUUID().toString();
//
//        HttpHeaders headers = buildJsonHeaders();
//        headers.set("X-Trace-Id", traceId);
//        HttpEntity<OracleSqlColumnsQueryRequest> entity = new HttpEntity<>(request, headers);
//
//        ResponseEntity<ApiResponse<Void>> resp = restTemplate.exchange(
//                url, HttpMethod.POST, entity,
//                new ParameterizedTypeReference<ApiResponse<Void>>() {}
//        );
//
//        ApiResponse<Void> body = resp.getBody();
//        if (!resp.getStatusCode().is2xxSuccessful() || body == null || !body.isSuccess()) {
//            throw new IllegalStateException("异步投递失败: status=" + resp.getStatusCode()
//                    + ", code=" + (body == null ? "null" : body.getCode())
//                    + ", message=" + (body == null ? "null" : body.getMessage())
//                    + ", traceId=" + (body == null ? traceId : body.getTraceId()));
//        }
//    }

    @PreDestroy
    public void shutdown() {
        try {
            if (zkClient != null) {
                ZkClientRegistry.release(serverInfo.getZooKeeperConfig());
                zkClient = null;
            }
        } catch (Exception e) {
            logger.warn("Failed to release shared ZkClient in SyncClientHTTPClient", e);
        }
    }
}
