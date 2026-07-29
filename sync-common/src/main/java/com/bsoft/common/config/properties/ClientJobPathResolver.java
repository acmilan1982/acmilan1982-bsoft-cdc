package com.bsoft.common.config.properties;

/**
 * ClassName: ClientJobPathResolver
 * Package:   com.bsoft.client.signal
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/16 - 9:33
 */


/**
 * 客户端 Job 级路径解析器
 *
 * 作用：
 * 1. 为某个业务库对应的 job 生成独立 ZK 路径
 * 2. 统一路径规则，避免在多个类里手写路径字符串
 *
 * 路径结构：
 * /clients/{clientId}/jobs/{dataSourceId}/status
 * /clients/{clientId}/jobs/{dataSourceId}/signals
 * /clients/{clientId}/jobs/{dataSourceId}/scn
 */
public class ClientJobPathResolver {

    private final ClientInfo clientInfo;
    private final String dataSourceId;

    public ClientJobPathResolver(ClientInfo clientInfo, String dataSourceId) {
        this.clientInfo = clientInfo;
        this.dataSourceId = dataSourceId;
    }

    /**
     * 客户端所有 job 的根路径
     */
    public String jobsRootPath() {
        return "/clients/" + clientInfo.getClientId() + "/jobs";
    }

    /**
     * 当前业务库 job 根路径
     */
    public String jobRootPath() {
        return jobsRootPath() + "/" + dataSourceId;
    }

    /**
     * 当前业务库 job 状态路径
     */
    public String statusPath() {
        return jobRootPath() + "/status";
    }

    /**
     * 当前业务库 job 停止信号路径
     */
    public String stopSignalPath() {
        return jobRootPath() + "/signals";
    }

    /**
     * 当前业务库 job 的 SCN 路径
     */
    public String scnPath() {
        return jobRootPath() + "/scn";
    }

    public String dataSourceId() {
        return dataSourceId;
    }
}