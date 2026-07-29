package com.bsoft.common.http;

import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * client HTTP 通信公共父类。
 *
 * 职责：
 * 1. client IP 查询
 * 2. client 在线探测
 * 3. 基础 Header 构建
 *
 * 说明：
 * 1. 当前类不再依赖 GlobalZooKeeperManager
 * 2. 通过外部注入的 ZkClient 读取 client IP 节点
 */
public abstract class AbstractHTTPClient {

    private static final Logger logger = LogManager.getLogger(AbstractHTTPClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    /**
     * 由子类注入的共享 ZK 客户端。
     */
    protected transient ZkClient zkClient;

    /**
     * 客户端存活检测间隔。
     */
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

    protected void waitUntilClientAlive(String clientId) {
        while (true) {
            boolean alive = isClientAlive(clientId);
            if (alive) {
                logger.info("客户端id: {} 可达,即将连接",clientId);
                return;
            }

            logger.info("客户端id: {} 不可达, 将在  {}秒后重试",clientId,CLIENT_ALIVE_CHECK_INTERVAL_MS / 1000);

            try {
                Thread.sleep(CLIENT_ALIVE_CHECK_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("waitUntilClientAlive interrupted, clientId=" + clientId, e);
            }
        }
    }

    protected boolean isClientAlive(String clientId) {
        try {
            String clientIP = getClientIP(clientId);
            String clientAliveURL = "http://" + clientIP + "/sync-client/api/ping";
            logger.info("客户端id: {}, url: {}",clientId, clientAliveURL);
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
            String clientIPJson = zkClient.getString(clientIPPath);

            return JsonUtil.jsonToIp(unbox(clientIPJson));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get client IP, clientId=" + clientId, e);
        }
    }


    /**
     * 将带转义、带首尾引号的 JSON 字符串还原为原始 JSON 字符串
     * @param input 可能带转义的字符串: "\"{\\\"ip\\\":\\\"127.0.0.1\\\"}\""
     * @return 原始 JSON 字符串: {"ip":"127.0.0.1"}
     */
    public static String unbox(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        try {
            // 尝试解析一次
            JsonNode node = mapper.readTree(input);

            // 如果解析结果是一个文本节点，说明它是一个“包装”出来的字符串
            while (node.isTextual()) {
                String content = node.asText();

                // 检查剥开后的内容是否还像 JSON（以 { 或 [ 开头）
                // 如果不像了，说明已经剥到底了，停止剥壳
                if (content != null && (content.trim().startsWith("{") || content.trim().startsWith("["))) {
                    node = mapper.readTree(content);
                } else {
                    // 已经是普通字符串了，直接返回
                    return content;
                }
            }

            // 将最终的 JsonNode 还原回不带转义的 String
            return node.toString();

        } catch (Exception e) {
            // 如果解析失败，说明原本就不是 JSON 格式，直接返回原字符串
            return input;
        }
    }
}