package com.bsoft.server.batch;

/**
 * ClassName: GlobalCommitGate
 * Package:   com.bsoft.server.batch
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/12/26 - 9:51
 */
import java.util.concurrent.Semaphore;

public final class GlobalCommitGate {
    // 同时允许多少个线程进入“executeBatch+commit”关键段
    // 建议先从 64 开始试：32/64/96 做梯度对比
    public static final Semaphore COMMIT = new Semaphore(256);

    private GlobalCommitGate() {}
}
