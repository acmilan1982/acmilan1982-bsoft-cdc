package com.bsoft.client.failure;

// 作业失败事件仓库接口。
public interface JobFailureEventRepository {


    // 插入作业失败事件。
    void insert(JobFailureEvent event) throws Exception;

}