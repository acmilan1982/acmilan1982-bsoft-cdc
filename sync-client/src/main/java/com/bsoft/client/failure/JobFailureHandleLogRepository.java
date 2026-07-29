package com.bsoft.client.failure;

// 作业失败处理记录仓库接口。
public interface JobFailureHandleLogRepository {

    // 插入作业失败处理记录。
    void insert(JobFailureHandleLog log) throws Exception;
}