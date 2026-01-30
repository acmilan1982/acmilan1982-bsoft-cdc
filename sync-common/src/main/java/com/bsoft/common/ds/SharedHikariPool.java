package com.bsoft.common.ds;

/**
 * ClassName: SharedHikariPool
 * Package:   com.bsoft.ds
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/21 - 17:09
 */

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 每个 TaskManager 进程内共享的 HikariDataSource。
 * 通过 url+username(+schema) 指纹做 key，引用计数控制生命周期。
 */
public final class SharedHikariPool {
    private static final ConcurrentHashMap<String, Ref> POOLS = new ConcurrentHashMap<>();

    private SharedHikariPool() {}

    /** 获取/复用一个连接池 */
    public static HikariDataSource acquire(HikariConfig cfg) {
        final String key = fingerprint(cfg);
        return POOLS.compute(key, (k, old) -> {
            if (old == null || old.ds == null || old.ds.isClosed()) {
                HikariDataSource ds = new HikariDataSource(cfg);
                return new Ref(ds);
            } else {
                old.refs.incrementAndGet();
                return old;
            }
        }).ds;
    }

    /** 归还连接池；最后一个归还者才真正 close */
    public static void release(HikariConfig cfg) {
        final String key = fingerprint(cfg);
        Ref ref = POOLS.get(key);
        if (ref != null && ref.refs.decrementAndGet() == 0) {
            try { ref.ds.close(); } catch (Throwable ignore) {}
            POOLS.remove(key, ref);
        }
    }

    /** 进程退出兜底关闭（可选） */
    public static void closeAll() {
        for (Ref r : POOLS.values()) {
            try { r.ds.close(); } catch (Throwable ignore) {}
        }
        POOLS.clear();
    }

    private static String fingerprint(HikariConfig c) {
        return (nz(c.getJdbcUrl()) + "|" + nz(c.getUsername()) + "|" +
                nz(c.getSchema()) + "|" + nz(c.getDriverClassName()));
    }
    private static String nz(String s) { return s == null ? "" : s; }

    private static final class Ref {
        final HikariDataSource ds;
        final AtomicInteger refs = new AtomicInteger(1);
        Ref(HikariDataSource ds) { this.ds = ds; }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(SharedHikariPool::closeAll, "close-hikari-pools"));
    }
}
