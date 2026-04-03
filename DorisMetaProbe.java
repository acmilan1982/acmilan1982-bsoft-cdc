import java.sql.*;

public class DorisMetaProbe {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://192.168.1.136:9030/spt_his_2025?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai";
        Class.forName("com.mysql.cj.jdbc.Driver");
        try (Connection conn = DriverManager.getConnection(url, "root", "123456")) {
            run(conn, "SHOW COLUMNS FROM dls_011_v2022_01_opt_fee_fucking");
            run(conn, "SHOW FULL COLUMNS FROM dls_011_v2022_01_opt_fee_fucking");
            run(conn, "SHOW INDEX FROM dls_011_v2022_01_opt_fee_fucking");
            run(conn, "SHOW KEYS FROM dls_011_v2022_01_opt_fee_fucking");
            run(conn, "SHOW CREATE TABLE dls_011_v2022_01_opt_fee_fucking");
        }
    }

    private static void run(Connection conn, String sql) throws Exception {
        System.out.println("=== " + sql + " ===");
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            for (int i = 1; i <= n; i++) {
                System.out.print(md.getColumnLabel(i));
                if (i < n) System.out.print("\t");
            }
            System.out.println();
            int row = 0;
            while (rs.next() && row < 20) {
                row++;
                for (int i = 1; i <= n; i++) {
                    Object v = rs.getObject(i);
                    System.out.print(v);
                    if (i < n) System.out.print("\t");
                }
                System.out.println();
            }
            System.out.println("rows=" + row);
        }
    }
}
