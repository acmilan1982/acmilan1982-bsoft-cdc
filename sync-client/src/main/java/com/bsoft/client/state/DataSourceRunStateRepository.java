package com.bsoft.client.state;

// 业务库运行状态仓库接口
public interface DataSourceRunStateRepository {

    // 确保当前 client + dataSource 存在一条运行状态记录
    void ensureRunStateExists(String clientId, String dataSourceId) throws Exception;

    // 判断当前业务库 snapshot 是否已经完成
    boolean isSnapshotCompleted(String clientId, String dataSourceId) throws Exception;

    // 标记当前业务库 snapshot 正在运行
    void markSnapshotRunning(String clientId, String dataSourceId) throws Exception;

    // 标记当前业务库 snapshot 已经完成
    void markSnapshotCompleted(String clientId, String dataSourceId) throws Exception;
}