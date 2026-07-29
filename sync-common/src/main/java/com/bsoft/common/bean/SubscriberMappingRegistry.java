package com.bsoft.common.bean;

import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.config.properties.SimpleClientInfo;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName: SubscriberMappingRegistry
 * Package:   com.bsoft.bean
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/31 - 15:31
 */
public class SubscriberMappingRegistry implements Serializable {

    // 从表中读取的订阅关系，最终的处理结果，都是一对一的关系
    private  List<SubscriberMapping> subscriberMappingList = new ArrayList<>();

    // 以fromDataSourceId为key,对订阅关系进行分组
    private final Map<String, List<SubscriberMapping>> mappingMapByFromDataSourceId = new HashMap<>(); // 以ID为key的快速查找

    // subscriberMappingList的内容，全部转换成：0toDataSourceId + "." + table.getSchemaName() + "." + table.getTableName();
    private final Set<String> mappingSetByToDataSourceIdSchemaNameTableName = new HashSet<>();


    // 从表中读取的订阅关系，先临时放在这里，稍后处理成一对一的关系
    private  List<SubscriberMapping> tempSubscriberMappingList = new ArrayList<>();

    private ServerInfo serverInfo;

    private ClientInfo clientInfo;

    private PipeLineInfo pipeLineInfo;

    // key为：dataSourceId,表示业务库的主键
    private final Map<String, SimpleClientInfo> clientInfoRegistry = new HashMap<>();



    public SubscriberMappingRegistry() {

    }


    public void register(SubscriberMapping mapping) {
        tempSubscriberMappingList.add(mapping);
    }

    public void register(List<SimpleClientInfo> simpleClientInfoList ) {
        // 入参为空时直接返回
        if (simpleClientInfoList == null || simpleClientInfoList.isEmpty()) {
            return;
        }

        // 遍历所有客户端信息
        for (SimpleClientInfo simpleClientInfo : simpleClientInfoList) {
            // 跳过空对象
            if (simpleClientInfo == null) {
                continue;
            }

            // 遍历当前客户端负责的所有数据源 ID
            for (String dataSourceId : simpleClientInfo.getDataSourceIdList()) {
                // 同一个 DATA_SOURCE_ID 只保留第一条
                clientInfoRegistry.putIfAbsent(dataSourceId, simpleClientInfo);
            }
        }
    }
    // 这里处理表中一对一，多对一，一对多，多对多等所有关系，都转换成一对一的关系
    // 客户端，中心端都会调用当前方法
    public void afterPropertiesSet() {

        // step1: 拆分成 一对一 的关系
        List<SubscriberMapping> flatMapSubscriberMappingList= new ArrayList<>();


        // CDC_DATA_SUBSCRIBE中的所有记录打散，形成 一对一 的多个SubscriberMapping
        for (SubscriberMapping subscriberMapping : tempSubscriberMappingList) {
            // CDC_DATA_SUBSCRIBE中的单条SubscriberMapping，拆分为多个 一对一 的 SubscriberMapping
            flatMapSubscriberMappingList.addAll(map(subscriberMapping));
        }

        // step2: 多条一对一的关系，如果  FROM_SOURCE_ID 与 TO_SOURCE_ID相等，合并他们的表
        Map<String, List<SubscriberMapping>> mappingMapByFromDataSourceIdToDataSourceId = new HashMap<>();

        // 将所有的 一对一 的关系，按照 FROM_SOURCE_ID + TO_SOURCE_ID 进行分组
        // 相同FROM_SOURCE_ID + TO_SOURCE_ID同相，List<SubscriberMapping>目前为表中数据，简单的加在一起，稍后会进行去重
        for (SubscriberMapping subscriberMapping : flatMapSubscriberMappingList) {
            String key = subscriberMapping.getFromDataSourceId() + "_" + subscriberMapping.getToDataSourceId();
            mappingMapByFromDataSourceIdToDataSourceId.computeIfAbsent(key, k -> new ArrayList<>()).add(subscriberMapping);
        }

        // step3: 最终结果：相同FROM_SOURCE_ID + TO_SOURCE_ID的订阅关系，表进行合并
        List<SubscriberMapping> finalFlatMapSubscriberMappingList= new ArrayList<>();
        for (Map.Entry<String, List<SubscriberMapping>> stringListEntry : mappingMapByFromDataSourceIdToDataSourceId.entrySet()) {
            // 重复出现的表，只保留一个
            HashSet<Table> availableTableSet = new HashSet<>();
            // key用来提取 FROM_SOURCE_ID,TO_SOURCE_ID
            String key = stringListEntry.getKey();
            List<SubscriberMapping> value = stringListEntry.getValue();
            // 所有的表，都合并到一个Set中，去重
            value.forEach(subscriberMapping -> subscriberMapping.getSourceTableNameList().forEach(availableTableSet::add));

            // key为：subscriberMapping.getFromDataSourceId() + "_" + subscriberMapping.getToDataSourceId()，用 _ 分割，所以这里解析一下
            String[] FromDataSourceIdToDataSourceIArray = key.split("_");
            String fromDataSourceId = FromDataSourceIdToDataSourceIArray[0];
            String toDataSourceId = FromDataSourceIdToDataSourceIArray[1];

            // 形成最终的 一对一 的关系
            finalFlatMapSubscriberMappingList.add(new SubscriberMapping(
                    fromDataSourceId,
                    toDataSourceId,
                    new ArrayList<>(availableTableSet)


            ));

            // 客户端需要，服务端不需要
            if(this.clientInfo != null){
                finalFlatMapSubscriberMappingList.forEach(subscriberMapping -> subscriberMapping.setClientInfo(this.clientInfo));
            }

            // 服务端需要，客户端不需要
            if(this.serverInfo != null){
                finalFlatMapSubscriberMappingList.forEach(subscriberMapping -> subscriberMapping.setServerInfo(this.serverInfo));
                for (SubscriberMapping subscriberMapping : finalFlatMapSubscriberMappingList) {
                    subscriberMapping.setServerInfo(this.serverInfo);
                    subscriberMapping.setClientInfo(this.clientInfoRegistry.get(subscriberMapping.getFromDataSourceId()));
                }
            }

            // 服务端需要，客户端不需要
            if(this.pipeLineInfo != null){
                finalFlatMapSubscriberMappingList.forEach(subscriberMapping -> subscriberMapping.setPipeLineInfo(this.pipeLineInfo));
                for (SubscriberMapping subscriberMapping : finalFlatMapSubscriberMappingList) {
                    subscriberMapping.setPipeLineInfo(this.pipeLineInfo);
                    subscriberMapping.setClientInfo(this.clientInfoRegistry.get(subscriberMapping.getFromDataSourceId()));
                }
            }

        }

        // step4: 当前类的属性： subscriberMappingList，持有这些最终的 一对一 的关系
        // 这些SubscriberMapping对象，fromDataSourceId 与 toDataSourceId 皆不相同
        this.subscriberMappingList = finalFlatMapSubscriberMappingList;

        // step5: 最后，以FROM_SOURCE_ID为key保存，目前已经是 一对一 的关系了
        for (SubscriberMapping subscriberMapping : finalFlatMapSubscriberMappingList) {
            mappingMapByFromDataSourceId.computeIfAbsent(subscriberMapping.getFromDataSourceId(), k -> new ArrayList<>()).add(subscriberMapping);
        }

        // todo: 5. 为了便于处理执行计划，生成Set
        for (SubscriberMapping subscriberMapping : finalFlatMapSubscriberMappingList) {
            for (Table table : subscriberMapping.getSourceTableNameList()) {
                mappingSetByToDataSourceIdSchemaNameTableName.add(
                        subscriberMapping.getToDataSourceId() + "." + table.getSchemaName() + "." + table.getTableName()
                );
            }
        }

        

    }

    // 入参subscriberMapping，是CDC_DATA_SUBSCRIBE中的一条记录
    // 因为CDC_DATA_SUBSCRIBE中的单条记录，DATA_FROM_SOURCE_ID,DATA_TO_SOURCE_ID都可能有多个值，这里根据 一对一的关系，拆分成多个SubscriberMapping
    // 不是当前业务数据库的fromDataSourceId，直接忽略掉
    private List<SubscriberMapping> map(SubscriberMapping subscriberMapping) {
        // 订阅表中的单条SubscriberMapping，拆分为多个 一对一 的 SubscriberMapping，存在这里
        List<SubscriberMapping> flatMapSubscriberMapping= new ArrayList<>();



        for (String fromDataSourceId : subscriberMapping.getTempFromDataSourceIdList()) {

            for (String toDataSourceId : subscriberMapping.getTempToDataSourceIdList()) {

                // 因为 DATA_SOURCE_TABLE 字段保存所有需要同步的表，但不一定当前fromDataSourceId中的表，因此需要过滤出当前业务库的表
                List<Table> tableList = subscriberMapping.getTempSourceTableNameList().stream()
                                                         .filter(table -> table.getDataSourceId().trim().equals(fromDataSourceId))
                                                         .collect(Collectors.toList());

                flatMapSubscriberMapping.add(new SubscriberMapping(fromDataSourceId,
                                                                   toDataSourceId,
                                                                   tableList));
            }

        }

        return flatMapSubscriberMapping;
    }




    public List<SubscriberMapping> findSubscriberMappingByDataFromSourceId(BusinessDataSourceConfig businessDataSourceConfig) {
         return mappingMapByFromDataSourceId.get(businessDataSourceConfig.getDataSourceId());

    }


//
//    public TopicInfoRegistry generateServerTopicInfo(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
//                                                     TargetDataSourceConfigRegistry targetDataSourceConfigRegistry) {
//        TopicInfoRegistry topicInfoRegistry = new TopicInfoRegistry();
//        for (SubscriberMapping subscriberMapping : this.tempSubscriberMappingList) {
//            topicInfoRegistry.register(subscriberMapping.generateServerTopicInfo(businessDataSourceConfigRegistry, targetDataSourceConfigRegistry));
//        }
//
//        return topicInfoRegistry;
//    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }

    public void setServerInfo(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
    }

    public ClientInfo getClientInfo() {
        return clientInfo;
    }

    public void setClientInfo(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }

    public List<SubscriberMapping> getTempSubscriberMappingList() {
        return tempSubscriberMappingList;
    }

    public void setTempSubscriberMappingList(List<SubscriberMapping> tempSubscriberMappingList) {
        this.tempSubscriberMappingList = tempSubscriberMappingList;
    }

    public void addTempSubscriberMappingList(List<SubscriberMapping> tempSubscriberMappingList) {
        this.tempSubscriberMappingList.addAll(tempSubscriberMappingList);
    }

    public List<SubscriberMapping> getSubscriberMappingList() {
        return subscriberMappingList;
    }

    public void setSubscriberMappingList(List<SubscriberMapping> subscriberMappingList) {
        this.subscriberMappingList = subscriberMappingList;
    }

    public Set<String> getMappingSetByToDataSourceIdSchemaNameTableName() {
        return mappingSetByToDataSourceIdSchemaNameTableName;
    }

    public boolean isEmpty() {
        return subscriberMappingList == null || subscriberMappingList.isEmpty();
    }

    public Map<String, List<SubscriberMapping>> getMappingMapByFromDataSourceId() {
        return mappingMapByFromDataSourceId;
    }

    public PipeLineInfo getPipeLineInfo() {
        return pipeLineInfo;
    }

    public void setPipeLineInfo(PipeLineInfo pipeLineInfo) {
        this.pipeLineInfo = pipeLineInfo;
    }
}