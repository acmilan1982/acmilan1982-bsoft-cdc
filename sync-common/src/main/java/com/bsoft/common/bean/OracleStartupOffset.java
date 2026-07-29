package com.bsoft.common.bean;

import java.io.Serializable;

/**
 * Oracle CDC 启动位点。
 */
public class OracleStartupOffset implements Serializable {

    // 序列化版本号。
    private static final long serialVersionUID = 1L;

    // LogMiner 事件 SCN。
    private final Long scn;

    // 事务提交 SCN，可能是纯数字，也可能是 Debezium 复合格式。
    private final String commitScn;

    /**
     * 创建 Oracle CDC 启动位点。
     */
    public OracleStartupOffset(Long scn, String commitScn) {
        // scn 是恢复的最低必要信息。
        if (scn == null) {
            throw new IllegalArgumentException("scn must not be null");
        }

        // 保存事件 SCN。
        this.scn = scn;

        // 快照消息没有 commit_scn 时，用 scn 兜底。
        this.commitScn = (commitScn == null || commitScn.trim().isEmpty())
                ? String.valueOf(scn)
                : commitScn.trim();
    }

    /**
     * 获取事件 SCN。
     */
    public Long getScn() {
        // 返回事件 SCN。
        return scn;
    }

    /**
     * 获取提交 SCN。
     */
    public String getCommitScn() {
        // 返回提交 SCN。
        return commitScn;
    }

    /**
     * 获取可比较的提交 SCN 数字。
     */
    public long comparableCommitScn() {
        // Debezium commit_scn 可能是 549577244:1:xxx 这种复合格式。
        String value = commitScn;

        // 复合格式只取第一个冒号前的数字部分。
        int colonIndex = value.indexOf(':');
        if (colonIndex > 0) {
            value = value.substring(0, colonIndex);
        }

        // 返回可比较的 long。
        return Long.parseLong(value);
    }

    @Override
    public String toString() {
        // 输出调试友好的位点信息。
        return "OracleStartupOffset{" +
                "scn=" + scn +
                ", commitScn='" + commitScn + '\'' +
                '}';
    }
}