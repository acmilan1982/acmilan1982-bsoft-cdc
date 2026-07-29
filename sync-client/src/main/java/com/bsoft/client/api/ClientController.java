package com.bsoft.client.api;

import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.dbmeta.*;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.http.ApiResponse;
import com.bsoft.common.http.ErrorCode;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.bsoft.common.zk.ZkClientRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ClassName: ServerClient
 * Package:   com.bsoft.client.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:38
 */

@RestController
@RequestMapping("/api")
public class ClientController {
    private static final Logger logger = LogManager.getLogger(ClientController.class);

    private final RestTemplate restTemplate = new RestTemplate();
//    OracleTableMetadataReader oracleTableMetadataReader;
    private OracleTableMetadataReaderRegistry oracleTableMetadataReaderRegistry;

    // server 的基础地址
    private String syncServerBaseUrl;
    // 用于把单表 DDL/元数据回调给 sync-server
    private  String tableDdlImportUrl;
    // 服务端健康检查/存活探测地址
    private  String serverPingUrl;
    // 把 SQL 查询结果对应的列元数据回调给 sync-server 建表
    private  String sqlResultTableImportUrl;


    private final String  serverIPPath = "/servers/Server001/ip";

    // 服务器IP地址+端口
    private String serverIP;
    // 客户端信息
    @Autowired
    private ClientInfo clientInfo;

//    private transient ClientZkCoordinator clientZkCoordinator;



    // 用于存储待处理请求的队列，单表的ddl
    private final BlockingQueue<TableDDLRequest> requestQueue = new LinkedBlockingQueue<>();

    // 用于存储待处理请求的队列，SQL元数据
    private final BlockingQueue<OracleSqlColumnsQueryRequest> sqlColumnsQueue = new LinkedBlockingQueue<>();

    // 两个常驻消费者线程
    private Thread ddlWorkerThread;
    private Thread sqlColumnsWorkerThread;


    // 关闭标记
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 当前 ClientController 使用的共享 ZK 客户端。
     *
     * 说明：
     * 1. 这里只用于读取非 client-process / non-job 的通用 ZK 路径
     * 2. 例如当前的 server IP 路径：/servers/Server001/ip
     */
    private transient ZkClient zkClient;


    @PostConstruct
    public void init() throws Exception {
//        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(clientInfo);
//        String serverIPJsonFormat = globalZooKeeperManager.getNodeData(serverIPPath);



        // 通过共享注册表获取当前 JVM 内可复用的 ZkClient。
        this.zkClient = ZkClientRegistry.acquire(clientInfo.getZooKeeperConfig());


        // 直接读取 server IP 节点。
        // 当前路径不属于 client process / client job 语义，因此不走 ClientProcessZkService / ClientJobZkService。
        String serverIPJsonFormat = zkClient.getString(serverIPPath);
        if (serverIPJsonFormat == null || serverIPJsonFormat.trim().isEmpty()) {
            logger.info("无法从 {} 获取中心端的ip值,中心端可能未上线，但不影响当前探针", serverIPPath);
        }else{
            serverIP = JsonUtil.jsonToIp(serverIPJsonFormat);
            syncServerBaseUrl = "http://" + serverIP + "/sync-server/api";
            serverPingUrl = syncServerBaseUrl + "/ping";
            tableDdlImportUrl = syncServerBaseUrl + "/tables/import";
            sqlResultTableImportUrl = syncServerBaseUrl + "/tables/sql-result/import";
        }



        // 启动处理线程
        startWorkers();
        System.out.println();
    }

    private void startWorkers() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        ddlWorkerThread = new Thread(this::runDdlWorker, "client-ddl-worker");
        ddlWorkerThread.setDaemon(true);

        sqlColumnsWorkerThread = new Thread(this::runSqlColumnsWorker, "client-sql-columns-worker");
        sqlColumnsWorkerThread.setDaemon(true);

        ddlWorkerThread.start();
        sqlColumnsWorkerThread.start();

        logger.info("ClientController workers started.");
    }

    @PreDestroy
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        if (ddlWorkerThread != null) {
            ddlWorkerThread.interrupt();
        }
        if (sqlColumnsWorkerThread != null) {
            sqlColumnsWorkerThread.interrupt();
        }

        joinQuietly(ddlWorkerThread, 5000);
        joinQuietly(sqlColumnsWorkerThread, 5000);

        logger.info("ClientController workers stopped.");

        try {
            if (zkClient != null) {
                ZkClientRegistry.release(clientInfo.getZooKeeperConfig());
                zkClient = null;
            }
        } catch (Exception e) {
            logger.warn("Failed to release shared ZkClient in ClientController shutdown", e);
        }
    }

    private void joinQuietly(Thread t, long millis) {
        if (t == null) return;
        try {
            t.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting thread {} to stop.", t.getName());
        }
    }



    public void initializeMetadataReader(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry) {
        this.oracleTableMetadataReaderRegistry = new OracleTableMetadataReaderRegistry(businessDataSourceConfigRegistry);
    }


    private void runDdlWorker() {
        while (running.get()) {
            try {
                TableDDLRequest req = requestQueue.take(); // 阻塞等待
                handleDdlRequest(req);
            } catch (InterruptedException e) {
                logger.info("DDL worker interrupted, exiting. running={}", running.get());
                break;
            } catch (Throwable t) {
                logger.error("DDL worker process failed", t);
            }
        }
        logger.info("DDL worker exited.");
    }

    private void runSqlColumnsWorker() {
        while (running.get()) {
            try {
                OracleSqlColumnsQueryRequest req = sqlColumnsQueue.take(); // 阻塞等待
                handleSqlColumnsRequest(req);
            } catch (InterruptedException e) {
                logger.info("SQL-columns worker interrupted, exiting. running={}", running.get());
                break;
            } catch (Throwable t) {
                logger.error("SQL-columns worker process failed", t);
            }
        }
        logger.info("SQL-columns worker exited.");
    }


    // 异步处理 单表 的ddl请求
    @PostMapping("/tables/ddl/export")
    private ResponseEntity<ApiResponse<Void>> exportTableDDL(
            @RequestBody TableDDLRequest tableDDLRequest,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceIdHeader) {

        String traceId = (traceIdHeader == null || traceIdHeader.trim().isEmpty())
                ? UUID.randomUUID().toString()
                : traceIdHeader.trim();

        try {
            boolean accepted = requestQueue.offer(tableDDLRequest);
            if (!accepted) {
                logger.warn("DDL request queue is full, traceId={}", traceId);
                return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(ApiResponse.fail(
                                ErrorCode.INTERNAL_ERROR,
                                "DDL request queue is full",
                                traceId
                        ));
            }

            logger.info("received ddl export request, queued. queueSize={}, traceId={}", requestQueue.size(), traceId);
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(ApiResponse.ok(null, traceId));
        } catch (Exception e) {
            logger.error("exportTableDDL failed, traceId={}", traceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.fail(
                            ErrorCode.INTERNAL_ERROR,
                            safeMessage(e),
                            traceId
                    ));
        }
    }


    // 获取指定schema，指定表的所有列信息
    @PostMapping("/tables/columns/query")
    private ResponseEntity<OracleColumnsQueryResult> queryTableColumns(@RequestBody OracleColumnsQueryRequest oracleColumnsQueryRequest) {
        try {

            OracleTableMetadataReader oracleTableMetadataReader = this.oracleTableMetadataReaderRegistry.getOracleTableMetadataReader(oracleColumnsQueryRequest.getBusinessDataSourceId());
            OracleColumnsQueryResult oracleColumnsQueryResult = oracleTableMetadataReader.queryColumnsByOwnerAndTables(oracleColumnsQueryRequest);
            return ResponseEntity.ok(oracleColumnsQueryResult);

        } catch (Exception e) {
            logger.error("exportTableColumns failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

    }



    // 异步：入队，稍后处理（给 sync-server 用）
    @PostMapping("/tables/sql-columns/export")
    private ResponseEntity<ApiResponse<Void>> exportColumnsBySqlAsync(
            @RequestBody OracleSqlColumnsQueryRequest request) {
        String traceId = UUID.randomUUID().toString();
        try {
            sqlColumnsQueue.offer(request);
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(ApiResponse.ok(null, traceId));
        } catch (Exception e) {
            logger.error("exportColumnsBySqlAsync failed, traceId={}", traceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.fail(
                            ErrorCode.INTERNAL_ERROR,
                            safeMessage(e),
                            traceId
                    ));
        }
    }

    // 同步：立即查询并返回结果（给 sync-pipeline 用）
    @PostMapping("/tables/sql-columns/query")
    private ResponseEntity<ApiResponse<OracleColumnsQueryResult>> queryColumnsBySqlSync(@RequestBody OracleSqlColumnsQueryRequest request) {
        String traceId = UUID.randomUUID().toString();
        try {
            OracleTableMetadataReader oracleTableMetadataReader = this.oracleTableMetadataReaderRegistry.getOracleTableMetadataReader(request.getBusinessDataSourceId());
            OracleColumnsQueryResult result = oracleTableMetadataReader.queryColumnsBySql(request);
            return ResponseEntity.ok(ApiResponse.ok(result, traceId));
        } catch (Exception e) {
            logger.error("queryColumnsBySqlSync failed, traceId={}", traceId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.fail(
                            ErrorCode.INTERNAL_ERROR,
                            safeMessage(e),
                            traceId
                    ));
        }
    }

    private String safeMessage(Throwable e) {
        String msg = e == null ? null : e.getMessage();
        return (msg == null || msg.trim().isEmpty()) ? e.getClass().getName() : msg;
    }

/*
    // ===== 重写处理循环：DDL请求 + SQL元数据请求都串行处理 =====
    private void processRequests() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 1. 优先处理传统单表建表请求
                TableDDLRequest ddlReq = requestQueue.poll(200, TimeUnit.MILLISECONDS);
                if (ddlReq != null) {
                    handleDdlRequest(ddlReq);
                    continue;
                }

                // 2. 处理 SQL 元数据 + 建表请求
                OracleSqlColumnsQueryRequest sqlReq = sqlColumnsQueue.poll(200, TimeUnit.MILLISECONDS);
                if (sqlReq != null) {
                    handleSqlColumnsRequest(sqlReq);
                }

            } catch (Exception e) {
                logger.error("处理请求队列失败", e);
            }
        }
    }
*/




    // ===== 传统 DDL 请求处理（把你原 processRequests 里的 DDL 部分抽出来）=====
    private void handleDdlRequest(TableDDLRequest tableDDLRequest) throws Exception {
        TableMappingConfig tableMappingConfig = new TableMappingConfig();
        tableMappingConfig.setSchemaName(tableDDLRequest.getSchemaName());
        tableMappingConfig.setTableName(tableDDLRequest.getTableName());

        // 获取单表的ddl，设置到 TableMappingConfig
        oracleTableMetadataReaderRegistry.getOracleTableMetadataReader(tableDDLRequest.getBusinessDataSourceId())
                                         .getSingleTableMetadata(tableMappingConfig);

        List<TableMappingConfigDTO> dtoList =
                TableMappingConfigDTO.fromList(Collections.singletonList(tableMappingConfig));


        tableDDLRequest.setClientInfo(this.clientInfo);

        TableDDLRequestWrapper wrapper = new TableDDLRequestWrapper(tableDDLRequest, dtoList);
        HttpEntity<TableDDLRequestWrapper> httpReq = new HttpEntity<>(wrapper, buildJsonHeaders());

        ResponseEntity<String> response = restTemplate.postForEntity(tableDdlImportUrl, httpReq, String.class);
        logger.info("DDL回调响应: {}", response.getBody());
    }


    // 自定义 SQL 串行处理核心：查元数据 -> 回调 sync-server 建表 =====
    private void handleSqlColumnsRequest(OracleSqlColumnsQueryRequest req) throws Exception {
        // 1) 查询 SQL 结果字段元数据
        OracleColumnsQueryResult result = oracleTableMetadataReaderRegistry.getOracleTableMetadataReader(req.getBusinessDataSourceId())
                       .queryColumnsBySql(req);
        if (result == null || result.getColumns() == null || result.getColumns().isEmpty()) {
            throw new IllegalStateException("SQL 元数据为空, targetTable=" + req.getTargetTableName());
        }

        // 2) 组建建表请求（目标表名来自 req）
        SqlResultCreateTableRequest createReq = new SqlResultCreateTableRequest();
        createReq.setTargetTableName(req.getTargetTableName());
        createReq.setTargetDataSourceId(req.getTargetDataSourceId());
        createReq.setColumns(result.getColumns());
        createReq.setSqlNodeId(req.getSqlNodeId());
        createReq.setClientInfo(this.clientInfo);

        // 3) 回调 sync-server 建表接口（保持串行）

        HttpEntity<SqlResultCreateTableRequest> httpReq = new HttpEntity<>(createReq, buildJsonHeaders());
        ResponseEntity<String> resp = restTemplate.postForEntity(sqlResultTableImportUrl, httpReq, String.class);

        logger.info("sql-result回调响应: {}", resp.getBody());
    }

    private HttpHeaders buildJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        return headers;
    }
//
//    // 处理请求队列中,拉取ddl请求
//    private void processRequests() {
//        while (!Thread.currentThread().isInterrupted()) {
//            try {
//                // 从队列中获取请求，如果队列为空则阻塞等待
//                TableDDLRequest tableDDLRequest = requestQueue.take();
//
//                HttpHeaders headers = new HttpHeaders();
//                headers.setContentType(MediaType.APPLICATION_JSON);
//
//                TableMappingConfig tableMappingConfig = new TableMappingConfig();
//                tableMappingConfig.setSchemaName(tableDDLRequest.getSchemaName());
//                tableMappingConfig.setTableName(tableDDLRequest.getTableName());
//
//                // 获取源库 单表 的建表语句，键信息等
//                oracleTableMetadataReader.getSingleTableMetadata(tableMappingConfig);
//
//                // 转换为 DTO 避免循环引用，只传输必要信息
//                List<TableMappingConfigDTO>  tableMappingConfigDTOList = TableMappingConfigDTO.fromList(
//                        Collections.singletonList(tableMappingConfig)
//                );
//
//                // 创建包装对象，同时封装两个对象
//                TableDDLRequestWrapper wrapper = new TableDDLRequestWrapper(tableDDLRequest, tableMappingConfigDTOList);
//
//                HttpEntity<TableDDLRequestWrapper> request = new HttpEntity<>(wrapper, headers);
//                ResponseEntity<String> response = restTemplate.postForEntity(sendTableDDLURL, request, String.class);
//
//                logger.info("中心端的响应: \n{}", response.getBody());
//
//
//            }  catch (Exception e) {
//                e.printStackTrace();
//                logger.error("处理建表请求时发生异常", e);
//                // 继续处理下一个请求
//            }
//        }
//    }






    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }


//    public void fetchAndSendTableMetadata(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
//        oracleTableMetadataReader = new OracleTableMetadataReader(businessDataSourceConfig);
//
//        try {
//            // 等待中心端上线
//            waitUntilServerAlive();
//
//            HttpHeaders headers = new HttpHeaders();
//            headers.setContentType(MediaType.APPLICATION_JSON);
//
//            // 获取源库每个表的建表语句，键信息等
//            oracleTableMetadataReader.getAllTableMetadata();
//
//            // 转换为 DTO 避免循环引用，只传输必要信息
//            List<TableMappingConfigDTO>  tableMappingConfigDTOList = TableMappingConfigDTO.fromList(
//                    businessDataSourceConfig.getTableMappingConfigList()
//            );
//            HttpEntity<List<TableMappingConfigDTO>> request = new HttpEntity<>(tableMappingConfigDTOList, headers);
//            ResponseEntity<String> response = restTemplate.postForEntity(sendTableDDLURL, request, String.class);
//            System.out.println(response.getBody());
//            logger.info("客户端的响应: \n{}", response.getBody());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    public void waitUntilServerAlive() {
//        while (true) {
//            boolean alive = isServerAlive();
//            if (alive) {
//                System.out.println("连接到中心端！！！！！！！");
//                break;
//            } else {
//                System.out.println("无法连接到中心端，将在5秒后重试...");
//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                    System.out.println("检测线程被中断，退出检测。");
//                    break;
//                }
//            }
//        }
//    }

//    public boolean isServerAlive() {
//        try {
//            ResponseEntity<String> response = restTemplate.getForEntity(serverAliveURL, String.class);
//            return response.getStatusCode().is2xxSuccessful()
//                    && "pong".equalsIgnoreCase(response.getBody());
//        } catch (Exception e) {
//            // 可以加日志
//            return false;
//        }
//    }

    @GetMapping("/ping")
    public String ping() {
        return "pong";
    }



}
