package com.bsoft.common.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

/**
 * 全局ID生成工具类，统一对外提供程序侧主键ID生成入口。
 */
public final class IdGenerateUtil {

    /** 自定义起始时间戳：2024-01-01 00:00:00 UTC，用于减少ID长度并保证长期为正数。 */
    private static final long EPOCH = 1704067200000L;

    /** workerId占用位数。 */
    private static final long WORKER_ID_BITS = 5L;

    /** datacenterId占用位数。 */
    private static final long DATACENTER_ID_BITS = 5L;

    /** sequence占用位数。 */

    private static final long SEQUENCE_BITS = 12L;

    /** workerId最大值。 */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);

    /** datacenterId最大值。 */
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);

    /** sequence最大值。 */
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    /** workerId左移位数。 */
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;

    /** datacenterId左移位数。 */
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    /** 时间戳左移位数。 */
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    /** 单例生成器，保证同一JVM内序列递增。 */
    private static final SnowflakeGenerator GENERATOR =
            new SnowflakeGenerator(resolveDatacenterId(), resolveWorkerId());

    /**
     * 私有构造方法，禁止实例化工具类。
     */
    private IdGenerateUtil() {
        // 工具类不允许实例化。
    }

    /**
     * 生成下一个long型全局ID。
     */
    public static long nextId() {
        // 统一入口，业务代码只调用这里，避免散落具体ID算法。
        return GENERATOR.nextId();
    }

    /**
     * 生成下一个字符串形式的全局ID。
     */
    public static String nextIdString() {
        // 兼容需要字符串主键或日志展示的场景。
        return String.valueOf(nextId());
    }

    /**
     * 解析datacenterId，优先使用系统属性，未配置时根据本机信息推导。
     */
    private static long resolveDatacenterId() {
        // 支持通过启动参数 -Dbsync.id.datacenter-id=1 显式指定。
        return resolveId("bsync.id.datacenter-id", buildMachineFingerprint(), MAX_DATACENTER_ID);
    }

    /**
     * 解析workerId，优先使用系统属性，未配置时根据本机信息推导。
     */
    private static long resolveWorkerId() {
        // 支持通过启动参数 -Dbsync.id.worker-id=1 显式指定。
        return resolveId("bsync.id.worker-id", buildMachineFingerprint() + "#" + getProcessName(), MAX_WORKER_ID);
    }

    /**
     * 根据系统属性或默认种子解析节点ID。
     */
    private static long resolveId(String propertyName, String fallbackSeed, long maxValue) {
        // 读取JVM系统属性。
        String configuredValue = System.getProperty(propertyName);

        // 如果配置了系统属性，则优先使用显式配置。
        if (configuredValue != null && configuredValue.trim().length() > 0) {
            return parseConfiguredId(propertyName, configuredValue, maxValue);
        }

        // 未配置时，用hash映射到允许范围内。
        return Math.abs(fallbackSeed.hashCode()) % (maxValue + 1);
    }

    /**
     * 解析显式配置的节点ID。
     */
    private static long parseConfiguredId(String propertyName, String configuredValue, long maxValue) {
        // 把配置值转换为long。
        long parsedValue = Long.parseLong(configuredValue.trim());

        // 校验配置值范围。
        if (parsedValue < 0 || parsedValue > maxValue) {
            throw new IllegalArgumentException(propertyName + " must be between 0 and " + maxValue);
        }

        // 返回合法配置值。
        return parsedValue;
    }

    /**
     * 构造机器指纹，用于未配置节点ID时自动推导。
     */
    private static String buildMachineFingerprint() {
        try {
            // 使用主机名和主机地址作为机器指纹。
            InetAddress address = InetAddress.getLocalHost();
            return address.getHostName() + "#" + address.getHostAddress();
        } catch (Exception e) {
            // 获取失败时回退到固定字符串，仍可依赖进程名区分部分场景。
            return "unknown-host";
        }
    }

    /**
     * 获取当前JVM进程名。
     */
    private static String getProcessName() {
        // 返回类似 pid@hostname 的运行时名称。
        return ManagementFactory.getRuntimeMXBean().getName();
    }

    /**
     * Snowflake ID生成器实现。
     */
    private static final class SnowflakeGenerator {

        /** 当前生成器所属datacenterId。 */
        private final long datacenterId;

        /** 当前生成器所属workerId。 */
        private final long workerId;

        /** 当前毫秒内序列号。 */
        private long sequence = 0L;

        /** 上一次生成ID的时间戳。 */
        private long lastTimestamp = -1L;

        /**
         * 创建Snowflake生成器。
         */
        private SnowflakeGenerator(long datacenterId, long workerId) {
            // 保存datacenterId。
            this.datacenterId = datacenterId;

            // 保存workerId。
            this.workerId = workerId;
        }

        /**
         * 生成下一个ID。
         */
        private synchronized long nextId() {
            // 获取当前毫秒时间戳。
            long timestamp = currentTimeMillis();

            // 检测系统时钟回拨。
            if (timestamp < lastTimestamp) {
                throw new IllegalStateException("System clock moved backwards, lastTimestamp="
                        + lastTimestamp + ", currentTimestamp=" + timestamp);
            }

            // 同一毫秒内递增序列号。
            if (timestamp == lastTimestamp) {
                sequence = (sequence + 1) & SEQUENCE_MASK;

                // 当前毫秒序列号耗尽时，等待下一毫秒。
                if (sequence == 0L) {
                    timestamp = waitNextMillis(lastTimestamp);
                }
            } else {
                // 新毫秒重置序列号。
                sequence = 0L;
            }

            // 保存本次生成时间戳。
            lastTimestamp = timestamp;

            // 拼装并返回最终ID。
            return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                    | (datacenterId << DATACENTER_ID_SHIFT)
                    | (workerId << WORKER_ID_SHIFT)
                    | sequence;
        }

        /**
         * 等待直到进入下一毫秒。
         */
        private long waitNextMillis(long lastTimestamp) {
            // 获取当前时间。
            long timestamp = currentTimeMillis();

            // 自旋等待下一毫秒。
            while (timestamp <= lastTimestamp) {
                timestamp = currentTimeMillis();
            }

            // 返回新的毫秒时间戳。
            return timestamp;
        }

        /**
         * 获取当前系统毫秒时间。
         */
        private long currentTimeMillis() {
            // 统一封装时间获取，便于后续测试或替换。
            return System.currentTimeMillis();
        }
    }

    public static void main(String[] args) {
        System.out.println(nextId());
    }
}