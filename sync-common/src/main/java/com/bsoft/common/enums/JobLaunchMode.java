package com.bsoft.common.enums;

/**
 * ClassName: JobLaunchMode
 * Package:   com.bsoft.common.enums
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/8/3 - 19:32
 */
// 描述单个 Flink job 的启动场景。
public enum JobLaunchMode {

    // 客户端进程启动后首次提交当前 dataSourceId 对应的 job。
    INITIAL_START(true),

    // 当前 dataSourceId 对应的 job 失败后自动重新提交。
    RESTART_AFTER_FAILURE(false);

    // 是否在启动前清空 ZK scn 节点。
    private final boolean clearScnBeforeStart;

    // 保存当前启动场景对应的 scn 清理策略。
    JobLaunchMode(boolean clearScnBeforeStart) {
        this.clearScnBeforeStart = clearScnBeforeStart;
    }

    // 返回当前启动场景是否需要在启动前清空 scn。
    public boolean isClearScnBeforeStart() {
        return clearScnBeforeStart;
    }
}
