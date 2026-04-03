import java.sql.*;
import java.util.*;

public class OracleTableCounts {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@//192.168.1.15:1521/orcl";
        String user = "CDC";
        String pass = "CDC";
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement ps = conn.prepareStatement("select table_name from user_tables order by table_name");
             ResultSet rs = ps.executeQuery()) {
            List<String> tables = new ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            for (String t : tables) {
                String sql = "select count(*) from \"" + t + "\"";
                try (Statement st = conn.createStatement(); ResultSet crs = st.executeQuery(sql)) {
                    crs.next();
                    System.out.println(t + "\t" + crs.getLong(1));
                }
            }
        }
    }
}
