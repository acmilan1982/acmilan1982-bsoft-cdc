
package com.bsoft.client.signal;

import com.bsoft.common.config.properties.ClientInfo;

import com.bsoft.common.util.ZooKeeperNodeManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

/**
 * 基于 ZooKeeper 的分布式停止信号管理器
 * 用于在集群环境中传递作业停止信号
 */
public class DistributedStopSignalManager implements Serializable {

    private static final Logger logger = LogManager.getLogger(DistributedStopSignalManager.class);

    private final String stopSignalPath;

    private final ZooKeeperNodeManager zooKeeperNodeManager;

    public DistributedStopSignalManager(ClientInfo clientInfo) throws Exception {
        this.stopSignalPath = clientInfo.getStopSignalPath();
        zooKeeperNodeManager  = new ZooKeeperNodeManager(clientInfo);
    }

    /**
     * 发送停止信号
     */
    public void sendStopSignal(String reason) {
        try {

            ObjectNode signal = new ObjectMapper().createObjectNode();
            signal.put("reason", reason);
            signal.put("sender", getClass().getSimpleName());

            // 停止信号发送到指定节点
            zooKeeperNodeManager.sendJobStopSignal(signal.toString());
            logger.info("📢 已发送停止信号到 ZooKeeper: {}, 原因: {}", stopSignalPath, reason);

        } catch (Exception e) {
            logger.error("发送停止信号失败: {}", e.getMessage(), e);
            throw new RuntimeException("发送停止信号失败", e);
        }
    }

    /**
     * 检查是否存在停止信号
     */
    public boolean checkStopSignal() {
        try {


            String data = zooKeeperNodeManager.getNodeData(stopSignalPath);
            if (data != null && !data.isEmpty()) {
                ObjectNode signal = (ObjectNode) new ObjectMapper().readTree(data);
                String signalType = signal.get("signal").asText();
                return "STOP".equals(signalType);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 节点不存在或其他错误，认为没有停止信号
            logger.debug("检查停止信号时发生错误: {}", e.getMessage());
        }
        return false;
    }

    /**
     * 获取停止信号详情
     */
    public String getStopSignalDetails() {
        try {

            String data = zooKeeperNodeManager.getNodeData(stopSignalPath);
            if (data != null && !data.isEmpty()) {
                ObjectNode signal = (ObjectNode) new ObjectMapper().readTree(data);
                return String.format("信号: %s, 原因: %s, 时间: %s",
                        signal.get("signal").asText(),
                        signal.get("reason").asText(),
                        signal.get("timestamp").asLong());
            }
        } catch (Exception e) {
            logger.debug("获取停止信号详情时发生错误: {}", e.getMessage());
        }
        return "无停止信号";
    }

    /**
     * 清除停止信号
     */
    public void clearStopSignal() {
        try {


            zooKeeperNodeManager.clearNodeData(stopSignalPath);
            logger.info("🧹 已清除停止信号: {}", stopSignalPath);
        } catch (Exception e) {
            logger.error("清除停止信号失败: {}", e.getMessage());
        }
    }

    /**
     * 关闭资源
     */
    public void close() {
        if (zooKeeperNodeManager != null) {
            try {
                zooKeeperNodeManager.close();
                logger.debug("ZooKeeper 连接已关闭");
            } catch (Exception e) {
                logger.error("关闭 ZooKeeper 连接失败", e);
            }
        }
    }


    public String getStopSignalPath() {
        return stopSignalPath;
    }
}