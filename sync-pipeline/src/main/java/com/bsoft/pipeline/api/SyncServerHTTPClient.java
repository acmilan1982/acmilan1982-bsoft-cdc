package com.bsoft.pipeline.api;

/**
 * ClassName: SyncServerHTTPClient
 * Package:   com.bsoft.pipeline.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 21:02
 */
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.dbmeta.SqlResultCreateTableRequest;
import com.bsoft.common.dbmeta.TableExistsCheckRequest;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.http.AbstractHTTPClient;
import com.bsoft.common.util.JsonUtil;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;

import javax.annotation.PostConstruct;

public class SyncServerHTTPClient extends AbstractHTTPClient {

    private final PipeLineInfo pipeLineInfo;

    public SyncServerHTTPClient(PipeLineInfo pipeLineInfo) {
        this.pipeLineInfo = pipeLineInfo;
    }

    @PostConstruct
    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(pipeLineInfo);
    }

    /**
     * 调用 sync-server 建表接口：
     * POST /sync-server/api/tables/sql-result/import
     */
    public String createSqlResultTable(String serverId, SqlResultCreateTableRequest request) throws Exception {
        if (serverId == null || serverId.trim().isEmpty()) {
            throw new IllegalArgumentException("serverId 不能为空");
        }
        if (request == null) {
            throw new IllegalArgumentException("request 不能为空");
        }

        String serverIp = getServerIP(serverId);
        String url = "http://" + serverIp + "/sync-server/api/tables/sql-result/import";

        HttpEntity<SqlResultCreateTableRequest> httpRequest =
                new HttpEntity<>(request, buildJsonHeaders());

        ResponseEntity<String> response =
                restTemplate.postForEntity(url, httpRequest, String.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("createSqlResultTable failed, status=" + response.getStatusCode());
        }

        return response.getBody();
    }

    private String getServerIP(String serverId) {
        try {
            String serverIPPath = "/servers/" + serverId + "/ip";
            String serverIPJson = globalZooKeeperManager.getNodeData(serverIPPath);
            return JsonUtil.jsonToIp(serverIPJson);
        } catch (Exception e) {
            throw new RuntimeException("获取服务端 IP 失败, serverId=" + serverId, e);
        }
    }


    /**
     * 调用 sync-server 表存在性检查接口
     * POST /sync-server/api/tables/exists
     */
    public boolean checkTableExists(String serverId, TableExistsCheckRequest request) throws Exception {
        if (serverId == null || serverId.trim().isEmpty()) {
            throw new IllegalArgumentException("serverId 不能为空");
        }
        if (request == null) {
            throw new IllegalArgumentException("request 不能为空");
        }
        if (request.getDataSourceId() == null || request.getDataSourceId().trim().isEmpty()) {
            throw new IllegalArgumentException("request.dataSourceId 不能为空");
        }
        if (request.getTableName() == null || request.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("request.tableName 不能为空");
        }

        String serverIp = getServerIP(serverId);
        String url = "http://" + serverIp + "/sync-server/api/tables/exists";

        HttpEntity<TableExistsCheckRequest> httpRequest =
                new HttpEntity<>(request, buildJsonHeaders());

        ResponseEntity<Boolean> response =
                restTemplate.postForEntity(url, httpRequest, Boolean.class);

        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("checkTableExists failed, status=" + response.getStatusCode());
        }

        Boolean body = response.getBody();
        return body != null && body;
    }

}