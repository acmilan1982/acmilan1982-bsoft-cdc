package com.bsoft.common.util;

/**
 * ClassName: OracleKeyInspector
 * Package:   com.bsoft.client.test
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 14:02
 */

import com.bsoft.common.bean.KeyDef;
import com.bsoft.common.bean.KeyDefBuilder;
import com.bsoft.common.bean.TableKeys;
import com.bsoft.common.enums.KeyType;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Oracle 19c / JDK 1.8 / 纯 JDBC
 * 目标：查询一批表的 主键(P)、唯一约束(U)、唯一索引(UNIQUE INDEX)、普通索引(NORMAL INDEX)
 * 返回：Map<"OWNER.TABLE", TableKeys>
 */
public class OracleKeyInspector {



    /**
     *   获取表的约束(P/U)，主键与唯一约束，外键不管
     *   获取表的唯一索引(不包含约束的唯一索引)
     *   获取表的普通索引
     *
     */
    public static TableKeys fetchKeys(Connection conn,
                                            String schema,
                                            String tableName,
                                            boolean includeUniqueIndexesWithoutConstraint,
                                            boolean includeNormalIndexes) throws SQLException {



        String owner = schema.toUpperCase(Locale.ROOT);
        String tableUpper = tableName.trim().toUpperCase(Locale.ROOT);

        // OWNER.TABLE -> KeyDef 列表
        Map<String, List<KeyDef>> tableToKeys = new LinkedHashMap<>();

        // 1) 获取表的约束(P/U)，主键与唯一约束，外键不管
        String consSql = buildConstraintsSql();
        try (PreparedStatement ps = conn.prepareStatement(consSql)) {
            ps.setString(1, owner);
            ps.setString(2, tableUpper);

            try (ResultSet rs = ps.executeQuery()) {
                Map<String, KeyDefBuilder> builderMap = new LinkedHashMap<>();

                while (rs.next()) {
                    String o  = rs.getString("OWNER");
                    String t  = rs.getString("TABLE_NAME");
                    String cn = rs.getString("CONSTRAINT_NAME");
                    String ct = rs.getString("CONSTRAINT_TYPE");
                    String col= rs.getString("COLUMN_NAME");

                    KeyType kt = "P".equals(ct) ? KeyType.PRIMARY_KEY : KeyType.UNIQUE_CONSTRAINT;
                    String key = o + "." + t + "|" + cn;

                    builderMap
                            .computeIfAbsent(key, k -> new KeyDefBuilder(cn, kt, o, t))
                            .addColumn(col);
                }

                for (KeyDefBuilder b : builderMap.values()) {
                    String ttKey = b.getOwner() + "." + b.getTable();
                    tableToKeys.computeIfAbsent(ttKey, k -> new ArrayList<>()).add(b.build());
                }
            }
        }

        // 2) 获取表的唯一索引(不包含约束的唯一索引)
        if (includeUniqueIndexesWithoutConstraint) {
            String idxSql = buildUniqueIndexSql();
            try (PreparedStatement ps = conn.prepareStatement(idxSql)) {
                ps.setString(1, owner);
                ps.setString(2, tableUpper);
                ps.setString(3, owner);

                try (ResultSet rs = ps.executeQuery()) {
                    Map<String, KeyDefBuilder> builderMap = new LinkedHashMap<>();

                    while (rs.next()) {
                        String o   = rs.getString("OWNER");
                        String t   = rs.getString("TABLE_NAME");
                        String in  = rs.getString("INDEX_NAME");
                        String col = rs.getString("COLUMN_NAME");

                        String key = o + "." + t + "|" + in;
                        builderMap
                                .computeIfAbsent(key, k -> new KeyDefBuilder(in, KeyType.UNIQUE_INDEX, o, t))
                                .addColumn(col);
                    }

                    for (KeyDefBuilder b : builderMap.values()) {
                        String ttKey = b.getOwner() + "." + b.getTable();
                        tableToKeys.computeIfAbsent(ttKey, k -> new ArrayList<>()).add(b.build());
                    }
                }
            }
        }

        // 3) 获取表的普通索引
        if (includeNormalIndexes) {
            String normalIdxSql = buildNormalIndexSql();
            try (PreparedStatement ps = conn.prepareStatement(normalIdxSql)) {
                ps.setString(1, owner);
                ps.setString(2, tableUpper);

                try (ResultSet rs = ps.executeQuery()) {
                    Map<String, KeyDefBuilder> builderMap = new LinkedHashMap<>();

                    while (rs.next()) {
                        String o   = rs.getString("OWNER");
                        String t   = rs.getString("TABLE_NAME");
                        String in  = rs.getString("INDEX_NAME");
                        String col = rs.getString("COLUMN_NAME");

                        String key = o + "." + t + "|" + in;
                        builderMap
                                .computeIfAbsent(key, k -> new KeyDefBuilder(in, KeyType.NORMAL_INDEX, o, t))
                                .addColumn(col);
                    }

                    for (KeyDefBuilder b : builderMap.values()) {
                        String ttKey = b.getOwner() + "." + b.getTable();
                        tableToKeys.computeIfAbsent(ttKey, k -> new ArrayList<>()).add(b.build());
                    }
                }
            }
        }

        // 4) 产出 TableKeys
        String tkey = owner + "." + tableUpper;
        List<KeyDef> kd = tableToKeys.getOrDefault(tkey, Collections.emptyList());
        kd.sort((a, b) -> {
            int ta = typeOrder(a.getType()), tb = typeOrder(b.getType());
            if (ta != tb) return Integer.compare(ta, tb);
            return a.getName().compareTo(b.getName());
        });
        return new TableKeys(owner, tableUpper, kd);
    }

    /**
     * 选择最佳的唯一标识策略
     */
    public static String selectBestUniqueIdentifier(TableKeys keys, String sourceId) {
        // 1. 优先使用稳定的唯一标识
        if (!keys.getPrimaryKeys().isEmpty()) {
            return "PRIMARY_KEY";
        }
        if (!keys.getUniqueConstraints().isEmpty()) {
            return "UNIQUE_CONSTRAINT";
        }
        if (!keys.getUniqueIndexes().isEmpty()) {
            return "UNIQUE_INDEX";
        }

        // 2. 兜底方案：SOURCE_ID + ROWID
        System.out.println("警告：表 " + keys.owner + "." + keys.table + " 没有稳定唯一键，使用 " + sourceId + " + ROWID");
        return "ROWID_WITH_SOURCE";
    }

    /**
     * 选择最佳的唯一键列（返回列名列表）
     */
    public static List<String> selectBestUniqueKey(TableKeys tableKeys) {
        // 1. 优先选择主键
        List<KeyDef> primaryKeys = tableKeys.getPrimaryKeys();
        if (!primaryKeys.isEmpty()) {
            // 主键通常只有一个，取第一个的所有列
            return primaryKeys.get(0).getColumns();
        }

        // 2. 其次选择唯一约束
        List<KeyDef> uniqueConstraints = tableKeys.getUniqueConstraints();
        if (!uniqueConstraints.isEmpty()) {
            // 如果有多个唯一约束，选择列数最少的（简单优先）
            return uniqueConstraints.stream()
                    .min(Comparator.comparing(k -> k.getColumns().size()))
                    .get()
                    .getColumns();
        }

        // 3. 最后选择独立唯一索引
        List<KeyDef> uniqueIndexes = tableKeys.getUniqueIndexes();
        if (!uniqueIndexes.isEmpty()) {
            // 选择列数最少的唯一索引
            return uniqueIndexes.stream()
                    .min(Comparator.comparing(k -> k.getColumns().size()))
                    .get()
                    .getColumns();
        }

        // 4. 都没有
        return Collections.emptyList();
    }

    /* ==========================  SQL 构造  ========================== */


    private static String buildConstraintsSql() {
        return ""
                + "SELECT c.OWNER, col.TABLE_NAME, c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, col.COLUMN_NAME, col.POSITION\n"
                + "FROM   ALL_CONSTRAINTS c\n"
                + "JOIN   ALL_CONS_COLUMNS col\n"
                + "  ON   c.OWNER = col.OWNER AND c.CONSTRAINT_NAME = col.CONSTRAINT_NAME\n"
                + "WHERE  c.CONSTRAINT_TYPE IN ('P','U')\n"
                + "  AND  c.OWNER = ?\n"
                + "  AND  col.TABLE_NAME = ?\n"
                + "ORDER BY c.OWNER, col.TABLE_NAME, c.CONSTRAINT_NAME, col.POSITION";
    }

    private static String buildUniqueIndexSql() {
        return ""
                + "SELECT i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_NAME, ic.COLUMN_POSITION\n"
                + "FROM   ALL_INDEXES i\n"
                + "JOIN   ALL_IND_COLUMNS ic\n"
                + "  ON   i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME\n"
                + "WHERE  i.OWNER = ?\n"
                + "  AND  i.TABLE_NAME = ?\n"
                + "  AND  i.UNIQUENESS = 'UNIQUE'\n"
                + "  AND  NOT EXISTS (\n"
                + "        SELECT 1 FROM ALL_CONSTRAINTS c\n"
                + "        WHERE c.OWNER = ?\n"
                + "          AND c.CONSTRAINT_TYPE IN ('P','U')\n"
                + "          AND c.INDEX_NAME = i.INDEX_NAME\n"
                + "      )\n"
                + "ORDER BY i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_POSITION";
    }

    private static String buildNormalIndexSql() {
        return ""
                + "SELECT i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_NAME, ic.COLUMN_POSITION\n"
                + "FROM   ALL_INDEXES i\n"
                + "JOIN   ALL_IND_COLUMNS ic\n"
                + "  ON   i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME\n"
                + "WHERE  i.OWNER = ?\n"
                + "  AND  i.TABLE_NAME = ?\n"
                + "  AND  i.UNIQUENESS = 'NONUNIQUE'\n"
                + "  AND  i.INDEX_TYPE = 'NORMAL'\n"
                + "ORDER BY i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_POSITION";
    }
    /* ==========================  工具方法  ========================== */



    private static int typeOrder(KeyType t) {
        switch (t) {
            case PRIMARY_KEY: return 1;
            case UNIQUE_CONSTRAINT: return 2;
            case UNIQUE_INDEX: return 3;
            case NORMAL_INDEX: return 4;  // 新增普通索引排序
            default: return 99;
        }
    }



    /* ==========================  使用示例（可删）  ========================== */

    public static void main(String[] args) throws Exception {
        // 显式加载驱动
        Class.forName("oracle.jdbc.OracleDriver");
        String url      = "jdbc:oracle:thin:@//192.168.1.196:1521/orcl"; // SERVICE_NAME
        String user     = "SPT_HIS_2023";
        String password = "SPT_HIS_2023";
        String schema   = "SPT_HIS_2023";

        String table = "PT_EXAMINATION_SUMMARY";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {


                TableKeys keys = fetchKeys(conn, schema, table, true, true);
                System.out.println("PK     : " + keys.getPrimaryKeys());
                System.out.println("UNIQUE : " + keys.getUniqueConstraints());
                System.out.println("U-INDEX: " + keys.getUniqueIndexes());
                System.out.println("N-INDEX: " + keys.getNormalIndexes());  // 新增普通索引显示

                // 显示选择的唯一标识策略
                String strategy = selectBestUniqueIdentifier(keys, "DB1");
                System.out.println("选择策略: " + strategy);

                List<String> selectedColumns = selectBestUniqueKey(keys);
                if (!selectedColumns.isEmpty()) {
                    System.out.println("选择的唯一键列: " + selectedColumns);
                } else {
                    System.out.println("将使用 ROWID 作为唯一标识");
                }
                System.out.println();

        }
    }
}