package com.bsoft.common.zk.service;

import com.alibaba.fastjson.JSONObject;
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.enums.ClientProcessStatus;
import com.bsoft.common.util.JsonUtil;
import com.bsoft.common.zk.ZkClient;
import com.bsoft.common.zk.ZkClientRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.UUID;

/**
 * client 进程级 ZooKeeper 服务。
 */
public class ClientProcessZkService {

    private static final Logger logger = LogManager.getLogger(ClientProcessZkService.class);

    /**
     * client 配置
     */
    private final ClientInfo clientInfo;

    /**
     * 底层 ZK 客户端
     */
    private final ZkClient zkClient;

    /**
     * 当前实例是否持有共享连接引用
     */
    private final boolean sharedClient;

    /**
     * 当前 client 进程实例 ID。
     */
    private final String processInstanceId = UUID.randomUUID().toString();


    // alive 节点注册冲突后的重试间隔秒数。
    private static final long ALIVE_REGISTER_RETRY_INTERVAL_SECONDS = 10L;

    // alive 节点注册冲突后的最大等待秒数，超过后执行一次强制接管。
    private static final long ALIVE_REGISTER_MAX_WAIT_SECONDS = 60L;


    private ClientProcessZkService(ClientInfo clientInfo, ZkClient zkClient, boolean sharedClient) {
        if (clientInfo == null) {
            throw new IllegalArgumentException("ClientInfo cannot be null");
        }
        if (zkClient == null) {
            throw new IllegalArgumentException("ZkClient cannot be null");
        }

        this.clientInfo = clientInfo;
        this.zkClient = zkClient;
        this.sharedClient = sharedClient;
    }

    /**
     * 打开一个 client 进程级 ZK 服务
     *
     * 说明：
     * 1. 内部通过 ZkClientRegistry 获取 JVM 内共享 ZkClient
     * 2. 调用方拿到的是 ClientProcessZkService，而不是底层 ZkClient
     */
    public static ClientProcessZkService open(ClientInfo clientInfo) throws Exception {
        ZkClient zkClient = ZkClientRegistry.acquire(clientInfo.getZooKeeperConfig());
        return new ClientProcessZkService(clientInfo, zkClient, true);
    }

    /**
     * 使用外部提供的 ZkClient 创建 process 服务
     *
     * 适用于测试或明确由外部管理连接生命周期的场景
     */
    public static ClientProcessZkService using(ClientInfo clientInfo, ZkClient zkClient) {
        return new ClientProcessZkService(clientInfo, zkClient, false);
    }

    /**
     * 初始化 client 进程级节点结构。
     */
    public void initClientProcessNodes() throws Exception {
        // 结构节点也写 {}，保证节点内容始终是 JSON 对象文本。
        zkClient.createJsonIfAbsent(clientInfo.getClientStatusPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);
        zkClient.createJsonIfAbsent(clientInfo.getClientIpPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);
        zkClient.createJsonIfAbsent(clientInfo.getClientJobsRootPath(), JsonUtil.emptyObject(), CreateMode.PERSISTENT);

        // 记录初始化完成日志。
        logger.info("Initialized client process nodes, clientId={}", clientInfo.getClientId());
    }


    /**
     * 更新 client IP 节点。
     */
    public void updateIp() throws Exception {
        // IP 数据也统一构造成 Map，避免字符串 JSON 二次序列化。
        Map<String, String> data = JsonUtil.ipToMap(clientInfo.getIp() + ":" + clientInfo.getServerPort());

        // 只写结构化对象，由 ZkClient 序列化一次。
        zkClient.createOrSetJson(
                clientInfo.getClientIpPath(),
                data,
                CreateMode.PERSISTENT
        );
    }

    /**
     * 注册 client 存活临时节点。
     */
    public void registerAliveNode() throws Exception {
        // 构造当前 JVM 进程 ID。
        String pid = resolvePid();

        // 构造 alive 节点 JSON 对象数据。
        Map<String, String> data = JsonUtil.clientAliveToMap(
                clientInfo.getClientId(),
                clientInfo.getIp(),
                clientInfo.getServerPort(),
                pid,
                processInstanceId
        );

        // 读取当前 client 的 alive 节点路径。
        String alivePath = clientInfo.getClientAlivePath();

        // 先尝试立即创建一次，正常情况下不会进入重试流程。
        if (tryCreateAliveNode(alivePath, data, 0L)) {
            // 创建成功后直接返回。
            return;
        }

        // 初始化已经等待的秒数。
        long waitedSeconds = 0L;

        // 在最大等待时间内按固定间隔重试，等待旧 ZK session 自然超时释放 EPHEMERAL 节点。
        while (waitedSeconds < ALIVE_REGISTER_MAX_WAIT_SECONDS) {
            // 计算本轮实际睡眠秒数，避免超过最大等待时间。
            long sleepSeconds = Math.min(
                    ALIVE_REGISTER_RETRY_INTERVAL_SECONDS,
                    ALIVE_REGISTER_MAX_WAIT_SECONDS - waitedSeconds
            );

            // 当前线程等待一段时间后再重试。
            sleepBeforeAliveRetry(sleepSeconds, waitedSeconds, alivePath);

            // 累加已经等待的秒数。
            waitedSeconds += sleepSeconds;

            // 再次尝试创建 alive 节点。
            if (tryCreateAliveNode(alivePath, data, waitedSeconds)) {
                // 创建成功后直接返回。
                return;
            }
        }

        // 超过最大等待时间仍未释放时，执行一次强制接管。
        forceTakeoverAliveNode(alivePath, data, waitedSeconds);
    }

    /**
     * 解析当前 JVM 进程 ID。
     */
    private String resolvePid() {
        // RuntimeMXBean name 通常是 pid@hostname。
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();

        // 空值时返回空字符串。
        if (runtimeName == null || runtimeName.trim().isEmpty()) {
            return "";
        }

        // 提取 @ 前面的 pid。
        int atIndex = runtimeName.indexOf('@');

        // 没有 @ 时返回原始值。
        if (atIndex < 0) {
            return runtimeName;
        }

        // 返回 pid。
        return runtimeName.substring(0, atIndex);
    }


    // 尝试创建 alive 临时节点，节点已存在时只记录日志并返回 false。
    private boolean tryCreateAliveNode(String alivePath, Map<String, String> data, long waitedSeconds) throws Exception {
        try {
            // 创建 EPHEMERAL 节点，ZK session 失效后自动删除。
            zkClient.createJson(
                    alivePath,
                    data,
                    CreateMode.EPHEMERAL
            );

            // 记录 alive 节点注册成功日志。
            logger.info("注册探针端临时节点： alive 完成, clientId={}, path={}, instanceId={}, waitedSeconds={}",
                    clientInfo.getClientId(),
                    alivePath,
                    processInstanceId,
                    waitedSeconds);

            // 返回创建成功。
            return true;
        } catch (KeeperException.NodeExistsException e) {
            // 读取旧 alive 节点内容，便于判断是否为旧进程残留。
            String oldAliveData = readAliveNodeRawQuietly(alivePath);

            // 记录本次创建冲突日志。
            logger.warn("探针端临时节点： alive 已经存在, 稍后再试. clientId={}, path={}, waitedSeconds={}, retryIntervalSeconds={}, maxWaitSeconds={}, newInstanceId={}, oldAliveData={}",
                    clientInfo.getClientId(),
                    alivePath,
                    waitedSeconds,
                    ALIVE_REGISTER_RETRY_INTERVAL_SECONDS,
                    ALIVE_REGISTER_MAX_WAIT_SECONDS,
                    processInstanceId,
                    oldAliveData);

            // 返回创建失败但可重试。
            return false;
        }
    }

    // 等待下一次 alive 节点注册重试。
    private void sleepBeforeAliveRetry(long sleepSeconds, long waitedSeconds, String alivePath) {
        try {
            // 记录即将等待重试的日志。
            logger.warn("Waiting before retrying alive node registration. clientId={}, path={}, waitedSeconds={}, sleepSeconds={}, maxWaitSeconds={}, instanceId={}",
                    clientInfo.getClientId(),
                    alivePath,
                    waitedSeconds,
                    sleepSeconds,
                    ALIVE_REGISTER_MAX_WAIT_SECONDS,
                    processInstanceId);

            // 睡眠到下一次重试时间。
            Thread.sleep(sleepSeconds * 1000L);
        } catch (InterruptedException e) {
            // 恢复线程中断标记。
            Thread.currentThread().interrupt();

            // 启动阶段被中断时直接失败。
            throw new IllegalStateException("Interrupted while waiting to retry alive node registration, clientId="
                    + clientInfo.getClientId()
                    + ", path="
                    + alivePath, e);
        }
    }

    // 超过等待时间后强制删除旧 alive 节点并接管。
    private void forceTakeoverAliveNode(String alivePath, Map<String, String> data, long waitedSeconds) throws Exception {
        // 读取强制删除前的旧 alive 节点内容。
        String oldAliveData = readAliveNodeRawQuietly(alivePath);

        // 记录强制接管日志。
        logger.warn("Alive node still exists after waiting, force takeover once. clientId={}, path={}, waitedSeconds={}, maxWaitSeconds={}, newInstanceId={}, oldAliveData={}",
                clientInfo.getClientId(),
                alivePath,
                waitedSeconds,
                ALIVE_REGISTER_MAX_WAIT_SECONDS,
                processInstanceId,
                oldAliveData);

        // 删除旧 alive 节点，非递归删除，因为 alive 是叶子节点。
        zkClient.delete(alivePath, false);

        try {
            // 删除后重新创建 EPHEMERAL alive 节点。
            zkClient.createJson(
                    alivePath,
                    data,
                    CreateMode.EPHEMERAL
            );

            // 记录强制接管成功日志。
            logger.warn("Force takeover client alive node succeeded. clientId={}, path={}, instanceId={}, waitedSeconds={}",
                    clientInfo.getClientId(),
                    alivePath,
                    processInstanceId,
                    waitedSeconds);
        } catch (KeeperException.NodeExistsException e) {
            // 强制删除后仍然存在，说明可能有并发新进程创建，直接失败。
            throw new IllegalStateException(
                    "Client alive node still exists after force takeover, clientId="
                            + clientInfo.getClientId()
                            + ", path="
                            + alivePath,
                    e
            );
        }
    }

    // 安静读取 alive 节点原始 JSON 文本，读取失败时返回错误摘要。
    private String readAliveNodeRawQuietly(String alivePath) {
        try {
            // 读取 alive 节点原始 JSON 字符串。
            String raw = zkClient.getString(alivePath);

            // 节点不存在时返回占位文本。
            return raw == null ? "<null>" : raw;
        } catch (Exception e) {
            // 读取失败只用于日志，不影响注册重试流程。
            return "<read failed: " + e.getMessage() + ">";
        }
    }
    /**
     * 更新 client 进程状态节点。
     */
    public void updateStatus(ClientProcessStatus processStatus) throws Exception {
        // 只写结构化对象，由 ZkClient 序列化一次。
        zkClient.createOrSetJson(
                clientInfo.getClientStatusPath(),
                JsonUtil.clientProcessStatusToMap(processStatus),
                CreateMode.PERSISTENT
        );
    }



    /**
     * 获取 client IP 节点原始 JSON 文本
     */
    public String getClientIpNodeRaw() throws Exception {
        return zkClient.getString(clientInfo.getClientIpPath());
    }

    /**
     * 获取 client jobs 根路径
     */
    public String getJobsRootPath() {
        return clientInfo.getClientJobsRootPath();
    }

    /**
     * 释放当前 process 服务持有的共享连接引用
     */
    public void close() {
        if (sharedClient) {
            ZkClientRegistry.release(clientInfo.getZooKeeperConfig());
        }
    }
//
//    /**
//     * 将 ClientProcessStatus 序列化为 JSON
//     *
//     * JSON 结构示例：
//     * {
//     *   "code": "CONFIG_MISSING",
//     *   "description": "缺少必要配置",
//     *   "detailInfo": "订阅关系配置缺失，将在10秒后重试"
//     * }
//     *
//     * 说明：
//     * 1. 这里与 JobStatus 的 JSON 风格保持一致
//     * 2. detailInfo 字段始终输出，避免上层解析时结构不稳定
//     */
//    private String clientProcessStatusToJson(ClientProcessStatus processStatus) {
//        JSONObject json = new JSONObject();
//        json.put("code", processStatus.getCode());
//        json.put("description", processStatus.getDescription());
//        json.put("detailInfo", processStatus.getDetailInfo() == null ? "" : processStatus.getDetailInfo());
//        return json.toJSONString();
//    }
}