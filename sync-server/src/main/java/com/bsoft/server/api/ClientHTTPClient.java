package com.bsoft.server.api;

import com.bsoft.common.bean.ServerInfo;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.dbmeta.OracleTableMetadataReader;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.util.JsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * ClassName: ServerClient
 * Package:   com.bsoft.client.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:38
 */


public class ClientHTTPClient {

    private static final Logger logger = LogManager.getLogger(ClientHTTPClient.class);

    private final RestTemplate restTemplate = new RestTemplate();

    // 客户端信息
    private ServerInfo serverInfo;

    // 全局 ZooKeeper 节点管理器
    protected transient GlobalZooKeeperManager globalZooKeeperManager;

    // 客户端存活检测间隔（毫秒）
    private static final long CLIENT_ALIVE_CHECK_INTERVAL_MS = 10 * 1000L;


    public ClientHTTPClient() {
    }

    public ClientHTTPClient(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    @PostConstruct
    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(serverInfo);

        System.out.println();
    }

    public void getTableMetadata(TableDDLRequest tableDDLRequest) throws Exception {
        // 等待客户端上线
        waitUntilClientAlive(tableDDLRequest);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // 重新获取客户端IP，防止IP变更后无法连接
        String clientIP = getClientIP(tableDDLRequest);
        String getTableDDLURL = "http://" + clientIP + "/sync-client/api/tables/ddl/export";

        // 向客户端发送请求
        HttpEntity request = new HttpEntity<>(tableDDLRequest, headers);
        ResponseEntity<String> response = restTemplate.postForEntity(getTableDDLURL, request, String.class);
        System.out.println(response.getBody());
        logger.info("客户端的响应: \n{}", response.getBody());

    }

    public void waitUntilClientAlive(TableDDLRequest tableDDLRequest) {
        while (true) {
            boolean alive = isClientAlive(tableDDLRequest);
            if (alive) {
                logger.info("成功连接到客户端！！！！！！！");
                break;
            } else {
                logger.info("无法连接到客户端，地址：{} ，将在{}秒后重试...",
                            getClientIP(tableDDLRequest),
                        CLIENT_ALIVE_CHECK_INTERVAL_MS / 1000);
                try {
                    Thread.sleep(CLIENT_ALIVE_CHECK_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("检测线程被中断，退出检测。");
                    break;
                }
            }
        }
    }

    // 检查客户端是否存活
    private boolean isClientAlive(TableDDLRequest tableDDLRequest) {
        try {
            // 重新获取客户端IP，防止IP变更后无法连接
            String clientIP = getClientIP(tableDDLRequest);
            String clientAliveURL = "http://" + clientIP + "/sync-client/api/ping";
            ResponseEntity<String> response = restTemplate.getForEntity(clientAliveURL, String.class);
            return response.getStatusCode().is2xxSuccessful() && "pong".equalsIgnoreCase(response.getBody());
        } catch (Exception e) {
            return false;
        }
    }

    private String getClientIP(TableDDLRequest tableDDLRequest) {
        try {
            // 重新获取客户端IP，防止IP变更后无法连接
            String clientIPPath = "/clients/" + tableDDLRequest.getClientId() + "/ip";
            String clientIPJsonFormat = globalZooKeeperManager.getNodeData(clientIPPath);
            return JsonUtil.jsonToIp(clientIPJsonFormat);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }


}
