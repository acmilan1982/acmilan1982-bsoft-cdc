package com.bsoft.server.k2db;

import com.bsoft.common.bean.ServerTopicInfo;
import com.bsoft.common.bean.TargetDataSourceConfig;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 一个目标表写入算子的上下文，代表多个 topic 写入同一张目标表
public class TargetTableWriteContext implements Serializable {


    // 目标表分组键
    private final TargetTableKey targetTableKey;

    // 目标数据源配置
    private final TargetDataSourceConfig targetDataSourceConfig;

    // 当前目标表对应的全部 topic 信息
    private final List<ServerTopicInfo> topicInfos;

    // 按 topic 名索引 topic 信息
    private final Map<String, ServerTopicInfo> topicInfoByTopic;

    // 创建目标表写入上下文
    public TargetTableWriteContext(TargetTableKey targetTableKey,
                                   TargetDataSourceConfig targetDataSourceConfig,
                                   List<ServerTopicInfo> topicInfos) {
        // 保存目标表分组键
        this.targetTableKey = targetTableKey;

        // 保存目标数据源配置
        this.targetDataSourceConfig = targetDataSourceConfig;

        // 保存 topic 列表，调用方保证顺序即可
        this.topicInfos = topicInfos == null ? Collections.emptyList() : topicInfos;

        // 构建 topic 索引
        this.topicInfoByTopic = buildTopicInfoByTopic(this.topicInfos);
    }

    // 构建 topic 到 ServerTopicInfo 的索引
    private Map<String, ServerTopicInfo> buildTopicInfoByTopic(List<ServerTopicInfo> topicInfos) {
        // 使用 LinkedHashMap 保留原始顺序
        Map<String, ServerTopicInfo> result = new LinkedHashMap<>();

        // 遍历当前目标表下的所有 topic
        for (ServerTopicInfo topicInfo : topicInfos) {
            // 跳过空对象
            if (topicInfo == null) {
                continue;
            }

            // 跳过空 topic
            if (topicInfo.getTopic() == null || topicInfo.getTopic().trim().isEmpty()) {
                continue;
            }

            // 按 topic 名建立索引
            result.put(topicInfo.getTopic().trim(), topicInfo);
        }

        // 返回只读索引，避免运行期被误改
        return Collections.unmodifiableMap(result);
    }

    // 获取目标表分组键
    public TargetTableKey getTargetTableKey() {
        // 返回目标表分组键
        return targetTableKey;
    }

    // 获取目标数据源配置
    public TargetDataSourceConfig getTargetDataSourceConfig() {
        // 返回目标数据源配置
        return targetDataSourceConfig;
    }

    // 获取目标数据源 ID
    public String getTargetDataSourceId() {
        // 从分组键返回目标数据源 ID
        return targetTableKey.getTargetDataSourceId();
    }

    // 获取目标表名
    public String getTargetTableName() {
        // 从分组键返回目标表名
        return targetTableKey.getTargetTableName();
    }

    // 获取当前目标表对应的全部 topic 信息
    public List<ServerTopicInfo> getTopicInfos() {
        // 返回 topic 信息列表
        return topicInfos;
    }

    // 获取 topic 索引
    public Map<String, ServerTopicInfo> getTopicInfoByTopic() {
        // 返回 topic 索引
        return topicInfoByTopic;
    }

    // 获取代表 topic，用于暂时发起建表请求
    public ServerTopicInfo getRepresentativeTopicInfo() {
        // 没有 topic 时返回 null
        if (topicInfos.isEmpty()) {
            return null;
        }

        // 按用户要求暂时取第一个 topic
        return topicInfos.get(0);
    }

    // 根据 topic 名获取 topic 信息
    public ServerTopicInfo findTopicInfo(String topic) {
        // 空 topic 直接返回 null
        if (topic == null || topic.trim().isEmpty()) {
            return null;
        }

        // 从索引中查找 topic 信息
        return topicInfoByTopic.get(topic.trim());
    }
}