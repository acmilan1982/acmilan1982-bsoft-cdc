package com.bsoft.common.util;

import com.alibaba.fastjson.JSONObject;
import com.bsoft.common.enums.ClientProcessStatus;
import com.bsoft.common.enums.JobStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JSON 数据构造工具类，负责构造写入 ZK 的结构化对象数据。
 */
public class JsonUtil {

    /**
     * 构造 client 存活节点数据。
     */
    public static Map<String, String> clientAliveToMap(String clientId,
                                                       String ip,
                                                       int port,
                                                       String pid,
                                                       String instanceId) {
        // 使用 LinkedHashMap 保持 ZK 工具查看时字段顺序稳定。
        Map<String, String> data = new LinkedHashMap<>();

        // 写入 clientId，便于监控端直接识别节点归属。
        data.put("clientId", clientId == null ? "" : clientId);

        // 写入进程 IP 和端口。
        data.put("ip", (ip == null ? "" : ip) + ":" + port);

        // 写入 JVM 进程 ID。
        data.put("pid", pid == null ? "" : pid);

        // 写入本次进程实例 ID，用于区分不同启动批次。
        data.put("instanceId", instanceId == null ? "" : instanceId);

        // 写入创建时间。
        data.put("startTime", formatNow());

        // 写入更新时间，临时节点通常不需要心跳更新，但字段保持统一。
        data.put("updateTime", formatNow());

        // 返回结构化对象，由 ZkClient 统一序列化一次。
        return data;
    }

    /**
     * 构造 job 状态节点数据。
     */
    public static Map<String, String> jobStatusToMap(JobStatus status) {
        // 状态不能为空，否则无法构造稳定的 ZK 节点对象。
        if (status == null) {
            throw new IllegalArgumentException("JobStatus cannot be null");
        }

        // 使用 LinkedHashMap 保持工具查看时字段顺序稳定。
        Map<String, String> data = new LinkedHashMap<>();
        data.put("code", status.getCode());
        data.put("description", status.getDescription());
        data.put("detailInfo", status.getDetailInfo() == null ? "" : status.getDetailInfo());
        data.put("updateTime", formatNow());
        return data;
    }

    /**
     * 构造 IP 节点数据。
     */
    public static Map<String, String> ipToMap(String ip) {
        // IP 节点也统一写 JSON 对象文本。
        Map<String, String> data = new LinkedHashMap<>();
        data.put("ip", ip == null ? "" : ip);
        data.put("updateTime", formatNow());
        return data;
    }

    /**
     * 从 IP 节点 JSON 对象文本中读取 IP 字段。
     */
    public static String jsonToIp(String ipNodeJson) {
        try {
            // 读取端只按 JSON 对象解析，不兼容字符串包 JSON。
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(ipNodeJson);
            JsonNode ipNode = root.get("ip");
            return ipNode == null ? "" : ipNode.asText();
        } catch (Exception e) {
            // 读取失败时返回空字符串，由调用方决定如何处理。
            return "";
        }
    }

    /**
     * 构造 SCN 节点数据。
     */
    public static Map<String, String> scnToMap(String scn) {
        // SCN 节点统一写 JSON 对象文本。
        Map<String, String> data = new LinkedHashMap<>();
        data.put("scn", scn == null ? "" : scn);
        data.put("updateTime", formatNow());
        return data;
    }

    /**
     * 构造 client 进程状态节点数据。
     */
    public static Map<String, String> clientProcessStatusToMap(ClientProcessStatus processStatus) {
        // 进程状态不能为空，否则无法构造稳定的 ZK 节点对象。
        if (processStatus == null) {
            throw new IllegalArgumentException("ClientProcessStatus cannot be null");
        }

        // 使用 LinkedHashMap 保持工具查看时字段顺序稳定。
        Map<String, String> data = new LinkedHashMap<>();
        data.put("code", processStatus.getCode());
        data.put("description", processStatus.getDescription());
        data.put("detailInfo", processStatus.getDetailInfo() == null ? "" : processStatus.getDetailInfo());
        data.put("updateTime", formatNow());
        return data;
    }

    /**
     * 构造空 JSON 对象节点数据。
     */
    public static Map<String, String> emptyObject() {
        // 空结构节点也必须是 JSON 对象文本，即 {}。
        return new LinkedHashMap<>();
    }


    /**
     * 构建 job 根节点数据。
     */
    public static Map<String, String> jobRootToMap(String dataSourceOrg) {
        // 使用 LinkedHashMap 保持 ZK 节点字段输出顺序稳定。
        Map<String, String> data = new LinkedHashMap<>();

        // 写入业务数据源机构名称。
        data.put("dataSourceOrg", dataSourceOrg == null ? "" : dataSourceOrg);

        // 写入本次节点更新时间。
        data.put("updateTime", formatNow());

        // 返回结构化节点数据。
        return data;
    }


    public static String offsetToJson(Object originalData, String topic, Long offset) {
        try {
            Map<String, String> topicsMap = new HashMap<>();

            if (originalData != null) {
                JsonNode topicsNode = new ObjectMapper()
                        .readTree(originalData.toString())
                        .get("topics");
                if (topicsNode.isArray()) {
                    topicsNode.forEach(node -> {
                        String existingTopic = node.get("topic").asText();
                        String existingOffset = node.get("offset").asText();
                        topicsMap.put(existingTopic, existingOffset);
                    });
                }
            }

            topicsMap.put(topic, String.valueOf(offset));

            List<Map<String, String>> topicsList = new ArrayList<>();
            for (Map.Entry<String, String> entry : topicsMap.entrySet()) {
                Map<String, String> topicMap = new HashMap<>();
                topicMap.put("topic", entry.getKey());
                topicMap.put("offset", entry.getValue());
                topicsList.add(topicMap);
            }

            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("topics", topicsList);
            jsonData.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            return new ObjectMapper().writeValueAsString(jsonData);
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }


    public static String unconsumedCountToJson(Object originalData, String topic, Long offset) {
        try {
            Map<String, String> topicsMap = new HashMap<>();

            if (originalData != null) {
                JsonNode topicsNode = new ObjectMapper()
                        .readTree(originalData.toString())
                        .get("topics");
                if (topicsNode.isArray()) {
                    topicsNode.forEach(node -> {
                        String existingTopic = node.get("topic").asText();
                        String existingOffset = node.get("unconsumedMessageCount").asText();
                        topicsMap.put(existingTopic, existingOffset);
                    });
                }
            }

            topicsMap.put(topic, String.valueOf(offset));

            List<Map<String, String>> topicsList = new ArrayList<>();
            for (Map.Entry<String, String> entry : topicsMap.entrySet()) {
                Map<String, String> topicMap = new HashMap<>();
                topicMap.put("topic", entry.getKey());
                topicMap.put("unconsumedMessageCount", entry.getValue());
                topicsList.add(topicMap);
            }

            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("topics", topicsList);
            jsonData.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

            return new ObjectMapper().writeValueAsString(jsonData);
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }



    public static String sourceTableNameToJson(Object originalSourceTableName, String newSourceTableName) {
        try {
            List<String> sourceTableList = new ArrayList<>();
            if (originalSourceTableName != null) {
                JsonNode sourceTableNode = new ObjectMapper()
                        .readTree(originalSourceTableName.toString())
                        .get("sourceTableName");
                if (sourceTableNode.isArray()) {
                    sourceTableNode.forEach(node -> sourceTableList.add(node.asText()));
                }
            }
            if (!sourceTableList.contains(newSourceTableName)) {
                sourceTableList.add(newSourceTableName);
            }
            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("sourceTableName", sourceTableList);
            jsonData.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            return new ObjectMapper().writeValueAsString(jsonData);
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }


//    /**
//     * 将 ClientProcessStatus 转换为 JSON
//     *
//     * JSON 结构示例：
//     * {
//     *   "code": "0001",
//     *   "description": "缺少必要配置",
//     *   "detailInfo": "订阅关系配置缺失，将在10秒后重试",
//     *   "updateTime": "2026-04-22 18:30:00"
//     * }
//     *
//     * 说明：
//     * 1. 与 ipToJson(...) 的风格保持一致
//     * 2. detailInfo 字段始终输出，避免上层解析结构不稳定
//     * 3. updateTime 用于记录当前状态最近一次更新时间
//     */
//    public static Map clientProcessStatusToJson(ClientProcessStatus processStatus) {
//        // 构造需要写入 ZK 的结构化数据。
//        Map<String, String> data = new LinkedHashMap<>();
//        data.put("code", processStatus.getCode());
//        data.put("description", processStatus.getDescription());
//        data.put("detailInfo", processStatus.getDetailInfo() == null ? "" : processStatus.getDetailInfo());
//        data.put("updateTime", formatNow());
//        return data;
//    }

    /**
     * 格式化当前时间。
     */
    public static String formatNow() {
        // ZK 展示用时间格式保持现有风格。
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
