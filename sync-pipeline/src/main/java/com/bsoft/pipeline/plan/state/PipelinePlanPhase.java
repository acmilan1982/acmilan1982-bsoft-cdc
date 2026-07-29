package com.bsoft.pipeline.plan.state;

/**
 * ClassName: PipelinePlanPhase
 * Package:   com.bsoft.pipeline.plan.state
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/4/7 - 20:14
 */

public enum PipelinePlanPhase {
    // 快照正在执行；启动时看到该状态，认为上次未完成，需要重新取 SCN 重跑
    SNAPSHOT_RUNNING,

    // 快照完整完成；SNAPSHOT_SCN 从这个状态开始才是有效增量基线
    SNAPSHOT_DONE,

    // 快照失败；启动时需要重新取 SCN 重跑
    SNAPSHOT_FAILED,

    // 增量正在运行；启动时不回退快照，应按增量 SCN 恢复
    INCREMENTAL_RUNNING,

    // 增量失败；启动时不回退快照，应按增量 SCN 恢复
    INCREMENTAL_FAILED
}