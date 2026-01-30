package com.bsoft.common.bean;

/**
 * ClassName: TopicInfo
 * Package:   com.bsoft.bean
 * Description: 持有主题信息，
 *              kafka上的每一个主题，都对应唯一一个businessDataSourceId，唯一一个targetDataSourceId
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 15:25
 */
public class ServerTopicInfo extends ClientTopicInfo {

    // 目标表名
    private String targetTableName;



    public ServerTopicInfo() {
        super();
    }


    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }



}
