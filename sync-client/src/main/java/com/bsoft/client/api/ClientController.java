package com.bsoft.client.api;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.ClientInfo;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.bean.TableDDLRequestWrapper;
import com.bsoft.common.bean.TableMappingConfig;
import com.bsoft.common.dbmeta.OracleTableMetadataReader;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.global.GlobalZooKeeperManager;
import com.bsoft.common.util.JsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

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
    OracleTableMetadataReader oracleTableMetadataReader;
    private  String  sendTableDDLURL;
    private  String  serverAliveURL;
//    private  String  getTableDDLURL;

    private final String  serverIPPath = "/servers/Server001/ip";
    // 客户端信息
    @Autowired
    private ClientInfo clientInfo;

    // 全局 ZooKeeper 节点管理器
    protected transient GlobalZooKeeperManager globalZooKeeperManager;

    // 服务器IP地址+端口
    private String serverIP;

    // 用于存储待处理请求的队列
    private final BlockingQueue<TableDDLRequest> requestQueue = new LinkedBlockingQueue<>();

    // 处理中心端请求的线程
    private final ExecutorService processingExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "table-creation-processor");
        thread.setDaemon(true);
        return thread;
    });

    @PostConstruct
    public void initProcessor() {
        // 启动处理线程
        processingExecutor.submit(this::processRequests);
    }

    @PreDestroy
    public void shutdown() {
        processingExecutor.shutdown();
        try {
            if (!processingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                processingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            processingExecutor.shutdownNow();
        }
    }


    @PostConstruct
    public void init() throws Exception {
        this.globalZooKeeperManager = GlobalZooKeeperManager.getInstance(clientInfo);
        String serverIPJsonFormat = globalZooKeeperManager.getNodeData(serverIPPath);
        serverIP = JsonUtil.jsonToIp(serverIPJsonFormat);
        serverAliveURL = "http://" + serverIP + "/sync-server/api/ping";
        sendTableDDLURL = "http://" + serverIP + "/sync-server/api/tables/import";
//        getTableDDLURL = "http://" + serverIP + "/sync-server/api/getTableDDL";


        System.out.println();
    }

    @PostMapping("/tables/ddl/export")
    private ResponseEntity<String> exportTableDDL(@RequestBody TableDDLRequest tableDDLRequest) {
        try {

            // 将请求添加到队列中
            requestQueue.offer(tableDDLRequest);
            logger.info("收到建表请求，已加入处理队列，当前队列大小: {}", requestQueue.size());
            // 立即返回成功响应

        } catch (Exception e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok("请求已接收，稍后会发送建表语句");
    }

    public void initializeMetadataReader(BusinessDataSourceConfig businessDataSourceConfig) {
        this.oracleTableMetadataReader = new OracleTableMetadataReader(businessDataSourceConfig);
    }


    // 处理请求队列中,拉取ddl请求
    private void processRequests() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 从队列中获取请求，如果队列为空则阻塞等待
                TableDDLRequest tableDDLRequest = requestQueue.take();

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);

                TableMappingConfig tableMappingConfig = new TableMappingConfig();
                tableMappingConfig.setSchemaName(tableDDLRequest.getSchemaName());
                tableMappingConfig.setTableName(tableDDLRequest.getTableName());

                // 获取源库 单表 的建表语句，键信息等
                oracleTableMetadataReader.getSingleTableMetadata(tableMappingConfig);

                // 转换为 DTO 避免循环引用，只传输必要信息
                List<TableMappingConfigDTO>  tableMappingConfigDTOList = TableMappingConfigDTO.fromList(
                        Collections.singletonList(tableMappingConfig)
                );

                // 创建包装对象，同时封装两个对象
                TableDDLRequestWrapper wrapper = new TableDDLRequestWrapper(tableDDLRequest, tableMappingConfigDTOList);

                HttpEntity<TableDDLRequestWrapper> request = new HttpEntity<>(wrapper, headers);
                ResponseEntity<String> response = restTemplate.postForEntity(sendTableDDLURL, request, String.class);

                logger.info("中心端的响应: \n{}", response.getBody());


            }  catch (Exception e) {
                e.printStackTrace();
                logger.error("处理建表请求时发生异常", e);
                // 继续处理下一个请求
            }
        }
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
