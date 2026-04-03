package com.bsoft.common.http;

import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.util.JsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * 客户端 HTTP 通信公共父类：
 * 负责 client IP 查询、在线探测、基础 Header 构建等通用逻辑。
 */
public abstract class AbstractHTTPClient {

    private static final Logger logger = LogManager.getLogger(AbstractHTTPClient.class);

    // 全局 ZooKeeper 节点管理器
    protected transient GlobalZooKeeperManager globalZooKeeperManager;

    // 客户端存活检测间隔
    protected static final long CLIENT_ALIVE_CHECK_INTERVAL_MS = 10 * 1000L;

    protected final RestTemplate restTemplate = new RestTemplate();

    protected AbstractHTTPClient() {
    }


    protected HttpHeaders buildJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }






    protected String resolveClientId(TableDDLRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("TableDDLRequest cannot be null");
        }
        if (request.getClientInfo() == null) {
            throw new IllegalArgumentException("TableDDLRequest.clientInfo cannot be null");
        }
        return normalizeClientId(request.getClientInfo().getClientId(), "TableDDLRequest.clientInfo.clientId");
    }

    protected String resolveClientId(OracleSqlColumnsQueryRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("OracleSqlColumnsQueryRequest cannot be null");
        }
        return normalizeClientId(request.getClientId(), "OracleSqlColumnsQueryRequest.clientId");
    }

    private String normalizeClientId(String clientId, String fieldName) {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return clientId.trim();
    }



    // 判断客户端是否在线，如不在线则等待
    protected void waitUntilClientAlive(String clientId) {
        while (true) {
            boolean alive = isClientAlive(clientId);
            if (alive) {
                logger.info("成功连接客户端: clientId={}", clientId);
                return;
            }

            logger.info("客户端不可达，{} 秒后重试: clientId={}",
                    CLIENT_ALIVE_CHECK_INTERVAL_MS / 1000, clientId);

            try {
                Thread.sleep(CLIENT_ALIVE_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("等待客户端在线被中断, clientId=" + clientId, e);
            }
        }
    }


    protected boolean isClientAlive(String clientId) {
        try {
            String clientIP = getClientIP(clientId);
            String clientAliveURL = "http://" + clientIP + "/sync-client/api/ping";
            ResponseEntity<String> response = restTemplate.getForEntity(clientAliveURL, String.class);
            return response.getStatusCode().is2xxSuccessful()
                    && "pong".equalsIgnoreCase(response.getBody());
        } catch (Exception e) {
            return false;
        }
    }


    protected String getClientIP(String clientId) {
        try {
            String clientIPPath = "/clients/" + clientId + "/ip";
            String clientIPJson = globalZooKeeperManager.getNodeData(clientIPPath);
            return JsonUtil.jsonToIp(clientIPJson);
        } catch (Exception e) {
            throw new RuntimeException("获取客户端 IP 失败, clientId=" + clientId, e);
        }
    }
}