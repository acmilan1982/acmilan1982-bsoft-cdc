package com.bsoft.common.util;

import com.bsoft.common.enums.JobStatus;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName: JsonUtil
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/3 - 3:14
 */
public class JsonUtil {


    public static String jobStatusToJson(JobStatus status) {
        try {
            Map<String, String> map = new HashMap<>();
            map.put("code", status.getCode());
            map.put("description", status.getDescription());
            map.put("detailInfo", status.getDetailInfo());
            map.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            return new ObjectMapper().writeValueAsString(map);
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }

    public static String ipToJson(String  ip) {
        try {
            Map<String, String> map = new HashMap<>();
            map.put("ip", ip);
            map.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            return new ObjectMapper().writeValueAsString(map);
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }

    public static String jsonToIp(String ip) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(ip);
            return  root.get("ip").asText();
        } catch (Exception e) {
            return "{\"code\":\"-1\",\"description\":\"未知状态\"}";
        }
    }


    public static String scnToJson(String offset) {
        try {
            Map<String, String> map = new HashMap<>();
            map.put("offset", offset);
            map.put("updateTime", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            return new ObjectMapper().writeValueAsString(map);
        } catch (Exception e) {
            return "{\"offset\":\"-1\",\"description\":\"未知状态\"}";
        }
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
}
