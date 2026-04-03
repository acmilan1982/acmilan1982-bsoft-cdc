import java.sql.*;

public class OracleListTables {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@//192.168.1.15:1521/orcl";
        String user = "CDC";
        String pass = "CDC";
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement ps = conn.prepareStatement("select table_name from user_tables order by table_name");
             ResultSet rs = ps.executeQuery()) {
            int count = 0;
            while (rs.next()) {
                count++;
                System.out.println(rs.getString(1));
            }
            System.out.println("TOTAL_TABLES=" + count);
        }
    }
}
