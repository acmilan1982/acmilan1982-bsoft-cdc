package com.bsoft.common.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: TopicInfo
 * Package:   com.bsoft.bean
 * Description: 持有主题信息，
 *              kafka上的每一个主题，都对应唯一一个businessDataSourceId，唯一一个targetDataSourceId
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:25
 */
public class TopicInfoRegistry {

    private List<ClientTopicInfo> clientTopicInfoList = new ArrayList<>();

    private List<ServerTopicInfo> serverTopicInfoList = new ArrayList<>();


    public TopicInfoRegistry() {
    }



    public void registerServerTopicInfo(List<ServerTopicInfo> serverTopicInfoList) {
        this.serverTopicInfoList.addAll(serverTopicInfoList);
    }

    public void registerClientTopicInfo(List<ClientTopicInfo> clientTopicInfoList) {
        this.clientTopicInfoList.addAll(clientTopicInfoList);
    }

    public List<ClientTopicInfo> getClientTopicInfoList() {
        return clientTopicInfoList;
    }

    public void setClientTopicInfoList(List<ClientTopicInfo> clientTopicInfoList) {
        this.clientTopicInfoList = clientTopicInfoList;
    }

    public List<ServerTopicInfo> getServerTopicInfoList() {
        return serverTopicInfoList;
    }

    public void setServerTopicInfoList(List<ServerTopicInfo> serverTopicInfoList) {
        this.serverTopicInfoList = serverTopicInfoList;
    }
}
