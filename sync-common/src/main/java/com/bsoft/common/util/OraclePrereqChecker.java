package com.bsoft.common.util;

import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Oracle CDC 前置条件自检（11g～19c）。
 * 仅使用 CDC_USER 权限可读的视图：SESSION_PRIVS / SESSION_ROLES / ALL_TAB_PRIVS / USER_TS_QUOTAS 等。
 */
public final class OraclePrereqChecker {
    private static final Logger LOG = LoggerFactory.getLogger(OraclePrereqChecker.class);

    private OraclePrereqChecker() {}

    /** 运行全部检查。expectedUser 一般传 "CDC_USER"，expectedTs 一般传 "USERS"（配额检查用） */
    public static Result checkAll(Connection conn, String expectedUser, String expectedTs) {
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        Map<String, String> info = new LinkedHashMap<>();

        // 0) 基本信息 & 版本
        try {
            DatabaseMetaData md = conn.getMetaData();
            info.put("dbProduct", md.getDatabaseProductName());
            info.put("dbVersion", md.getDatabaseProductVersion());
            info.put("jdbcUser", md.getUserName());

            int major = md.getDatabaseMajorVersion(); // 11/12/18/19...
            info.put("dbMajor", String.valueOf(major));
            if (major > 19) {
                warnings.add("检测到数据库主版本为 " + major + "（>19），未做全面验证，建议先在测试环境评估。");
            }
        } catch (SQLException e) {
            errors.add("无法读取数据库元信息: " + e.getMessage());
        }

        // 1) 必须使用指定用户（如 CDC_USER）连接
        String currentUser = querySingleString(conn, "SELECT USER FROM dual");
        if (currentUser == null) {
            errors.add("无法获取当前会话用户（SELECT USER FROM dual 失败）");
        } else if (!currentUser.equalsIgnoreCase(expectedUser)) {
            errors.add("必须使用用户 " + expectedUser + " 登录，当前为 " + currentUser);
        } else {
            info.put("sessionUser", currentUser);
        }

        // 2) 收集当前会话的系统权限与启用的角色
        Set<String> sessionPrivs = queryStringSet(conn, "SELECT PRIVILEGE FROM SESSION_PRIVS");
        Set<String> sessionRoles = queryStringSet(conn, "SELECT ROLE FROM SESSION_ROLES");
        info.put("sessionPrivs", sessionPrivs.toString());
        info.put("sessionRoles", sessionRoles.toString());

        // 3) 必要系统权限（用 SESSION_PRIVS 判断）
        requirePriv(sessionPrivs, "CREATE SESSION", errors);
        requirePriv(sessionPrivs, "LOGMINING", errors);                 // LOGMINING 是系统权限（非角色）
        requirePriv(sessionPrivs, "SELECT ANY TRANSACTION", errors);
        // 字典访问：满足其一即可
        if (!(sessionPrivs.contains("SELECT ANY DICTIONARY") || sessionRoles.contains("SELECT_CATALOG_ROLE"))) {
            errors.add("需要 SELECT ANY DICTIONARY（系统权限）或 SELECT_CATALOG_ROLE（角色）其一，用于访问字典视图。");
        }
        // 推荐但可按需调整
        requirePriv(sessionPrivs, "FLASHBACK ANY TABLE", warnings);
        requirePriv(sessionPrivs, "CREATE TABLE", warnings);

        // 4) DBMS_LOGMNR 执行权限（对象权限）
        if (!hasExecuteOnDbmsLogmnr(conn)) {
            errors.add("缺少对象权限：EXECUTE ON SYS.DBMS_LOGMNR（请由 DBA 执行：GRANT EXECUTE ON SYS.DBMS_LOGMNR TO " + expectedUser + "）");
        }

        // 5) 关键 V$ 视图可读性（用探测查询判断是否有 SELECT 权限）
        requireViewReadable(conn, "V$LOGMNR_LOGS", errors);
        requireViewReadable(conn, "V$LOGMNR_CONTENTS", errors);
        requireViewReadable(conn, "V$ARCHIVED_LOG", errors);
        requireViewReadable(conn, "V$LOG", warnings);       // 读取不一定强制，但建议
        requireViewReadable(conn, "V$DATABASE", errors);
        requireViewReadable(conn, "V$THREAD", warnings);
        requireViewReadable(conn, "V$PARAMETER", warnings);
        requireViewReadable(conn, "V$NLS_PARAMETERS", warnings);
        requireViewReadable(conn, "V$TIMEZONE_NAMES", warnings);

        // 6) 归档与补充日志（需要能读 V$DATABASE）
        String logMode = querySingleString(conn, "SELECT LOG_MODE FROM V$DATABASE");
        if (logMode == null) {
            errors.add("无法读取 V$DATABASE.LOG_MODE，无法确认是否为 ARCHIVELOG 模式（缺少视图权限？）");
        } else if (!"ARCHIVELOG".equalsIgnoreCase(logMode)) {
            errors.add("数据库未开启归档（ARCHIVELOG），当前为：" + logMode);
        } else {
            info.put("log_mode", logMode);
        }

        // 补充日志（建议 MIN 与 ALL 都为 YES）
        Map<String,String> supp = querySingleRowAsMap(conn,
                "SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, " +
                        "       SUPPLEMENTAL_LOG_DATA_UI, SUPPLEMENTAL_LOG_DATA_ALL " +
                        "FROM V$DATABASE");
        if (supp.isEmpty()) {
            errors.add("无法读取补充日志配置（V$DATABASE），请确认视图可读权限。");
        } else {
            String min = supp.getOrDefault("SUPPLEMENTAL_LOG_DATA_MIN", "NO");
            String all = supp.getOrDefault("SUPPLEMENTAL_LOG_DATA_ALL", "NO");
            info.putAll(supp);
            if (!"YES".equalsIgnoreCase(min)) {
                errors.add("未启用最小补充日志：ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;");
            }
            if (!"YES".equalsIgnoreCase(all)) {
                errors.add("未启用列级补充日志(ALL)：ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;");
            }
        }

        // 7) 表空间配额（UNLIMITED on expectedTs）
        if (expectedTs != null && !expectedTs.trim().isEmpty()) {
            Long maxBytes = querySingleLong(conn,
                    "SELECT MAX_BYTES FROM USER_TS_QUOTAS WHERE TABLESPACE_NAME = ?",
                    ps -> ps.setString(1, expectedTs));
            if (maxBytes == null) {
                warnings.add("在表空间 " + expectedTs + " 未检测到配额记录；如需在该表空间建表/落盘，请执行：ALTER USER "
                        + expectedUser + " QUOTA UNLIMITED ON " + expectedTs + ";");
            } else if (maxBytes != -1L) {
                warnings.add("表空间 " + expectedTs + " 配额非 UNLIMITED（MAX_BYTES=" + maxBytes +
                        "）。建议：ALTER USER " + expectedUser + " QUOTA UNLIMITED ON " + expectedTs + ";");
            } else {
                info.put("quota_" + expectedTs, "UNLIMITED");
            }
        }

        return new Result(errors, warnings, info);
    }

    // ----------------- 新增：DBMS_LOGMNR EXECUTE 权限检查 -----------------

    /** 是否拥有 EXECUTE ON SYS.DBMS_LOGMNR 对象权限（优先 ALL_TAB_PRIVS，回退 DBA_TAB_PRIVS）。 */
    private static boolean hasExecuteOnDbmsLogmnr(Connection c) {
        // 先探测 ALL_TAB_PRIVS 的列名（可能是 OWNER 或 TABLE_SCHEMA）
        String ownerCol = resolveOwnerColumnName(c, "ALL_TAB_PRIVS");
        if (ownerCol != null) {
            String sql = "SELECT 1 FROM ALL_TAB_PRIVS " +
                    "WHERE " + ownerCol + "='SYS' AND TABLE_NAME='DBMS_LOGMNR' " +
                    "  AND PRIVILEGE='EXECUTE' AND GRANTEE=USER";
            if (exists(c, sql)) {
                return true;
            }
        }
        // 回退：尝试 DBA_TAB_PRIVS（需要有权访问）
        if (viewExists(c, "DBA_TAB_PRIVS")) {
            String sql = "SELECT 1 FROM DBA_TAB_PRIVS " +
                    "WHERE OWNER='SYS' AND TABLE_NAME='DBMS_LOGMNR' " +
                    "  AND PRIVILEGE='EXECUTE' AND GRANTEE=USER";
            if (exists(c, sql)) {
                return true;
            }
        }
        return false;
    }

    /** 解析指定视图中“拥有者列”的列名（优先 OWNER，找不到则 TABLE_SCHEMA；都无则返回 null）。 */
    private static String resolveOwnerColumnName(Connection c, String viewName) {
        String probe = "SELECT * FROM " + viewName + " WHERE 1=0";
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(probe)) {
            ResultSetMetaData m = rs.getMetaData();
            int n = m.getColumnCount();
            String found = null;
            for (int i = 1; i <= n; i++) {
                String col = m.getColumnLabel(i);
                if (col == null) continue;
                String up = col.trim().toUpperCase(Locale.ROOT);
                if ("OWNER".equals(up)) {
                    return "OWNER";
                }
                if ("TABLE_SCHEMA".equals(up)) {
                    found = "TABLE_SCHEMA";
                }
            }
            return found; // 可能为 null
        } catch (SQLException e) {
            return null;
        }
    }

    /** 视图是否存在（尝试 select 1 from view where 1=0）。 */
    private static boolean viewExists(Connection c, String view) {
        try (Statement st = c.createStatement()) {
            st.executeQuery("SELECT 1 FROM " + view + " WHERE 1=0").close();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    // ----------------- 辅助方法 -----------------

    private static void requirePriv(Set<String> sessionPrivs, String need, List<String> bag) {
        if (!sessionPrivs.contains(need)) {
            bag.add("缺少系统权限：" + need);
        }
    }

    private static void requireViewReadable(Connection c, String view, List<String> bag) {
        // 通过探测查询判断是否有 SELECT 权限
        String sql = "SELECT 1 FROM " + view + " WHERE 1=0";
        try (Statement st = c.createStatement()) {
            st.executeQuery(sql).close();
        } catch (SQLException e) {
            String msg = e.getMessage();
            // 授权建议使用 V_$ 同义词对象（如 V_$LOGMNR_CONTENTS），部分环境 DBA 更习惯授予这类对象
            bag.add("无法读取视图 " + view + "（请授予 SELECT ON " + view + " 或其同义词 V_$...）：错误=" + msg);
        }
    }

    private static String querySingleString(Connection c, String sql) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            return null;
        }
    }

    private static Long querySingleLong(Connection c, String sql, SqlSetter setter) {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            if (setter != null) setter.apply(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    private static Set<String> queryStringSet(Connection c, String sql) {
        Set<String> set = new LinkedHashSet<>();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                String v = rs.getString(1);
                if (v != null) set.add(v.trim().toUpperCase(Locale.ROOT));
            }
        } catch (SQLException e) {
            // 忽略，保持为空集合
        }
        return set;
    }

    private static boolean exists(Connection c, String sql) {
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    private static Map<String,String> querySingleRowAsMap(Connection c, String sql) {
        Map<String,String> map = new LinkedHashMap<>();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                ResultSetMetaData m = rs.getMetaData();
                for (int i = 1; i <= m.getColumnCount(); i++) {
                    String key = m.getColumnLabel(i);
                    String val = rs.getString(i);
                    map.put(key, val);
                }
            }
        } catch (SQLException e) {
            // ignore
        }
        return map;
    }

    @FunctionalInterface
    private interface SqlSetter { void apply(PreparedStatement ps) throws SQLException; }

    // ----------------- 结果对象 -----------------

    public static final class Result {
        private final List<String> errors;
        private final List<String> warnings;
        private final Map<String, String> info;

        public Result(List<String> errors, List<String> warnings, Map<String, String> info) {
            this.errors = Collections.unmodifiableList(new ArrayList<>(errors));
            this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
            this.info = Collections.unmodifiableMap(new LinkedHashMap<>(info));
        }

        public boolean ok() { return errors.isEmpty(); }
        public List<String> errors() { return errors; }
        public List<String> warnings() { return warnings; }
        public Map<String,String> info() { return info; }

        public void logToSlf4j() {
            info.forEach((k,v) -> LOG.info("[CDC-CHECK][INFO] {} = {}", k, v));
            warnings.forEach(w -> LOG.warn("[CDC-CHECK][WARN] {}", w));
            errors.forEach(e -> LOG.error("[CDC-CHECK][ERROR] {}", e));
        }

        public void logToStdout() {
            info.forEach((k,v) -> System.out.println("[CDC-CHECK][INFO] " + k + " = " + v));
            warnings.forEach(w -> System.out.println("[CDC-CHECK][WARN] " + w));
            errors.forEach(e -> System.out.println("[CDC-CHECK][ERROR] " + e));
        }
    }
}
