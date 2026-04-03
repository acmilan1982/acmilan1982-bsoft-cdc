package com.bsoft.monitor.k2zk;

/**
 * ClassName: OffsetFlushDefaults
 * Package:   com.bsoft.monitor.k2zk
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/2 - 15:55
 */
/**
 * OFFSET 提交策略默认值
 */
public final class OffsetFlushDefaults {
    private OffsetFlushDefaults() {
    }

    // 达到该条数时触发 flush
    public static final int BATCH_SIZE = 40;

    // 距离上次 flush 超过该时间时触发 flush
    public static final long INTERVAL_MS = 2000L;
}