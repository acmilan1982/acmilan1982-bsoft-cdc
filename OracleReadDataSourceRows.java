import java.sql.*;

public class OracleReadDataSourceRows {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@//192.168.1.15:1521/orcl";
        String user = "CDC";
        String pass = "CDC";
        Class.forName("oracle.jdbc.OracleDriver");

        try (Connection conn = DriverManager.getConnection(url, user, pass);
             PreparedStatement ps = conn.prepareStatement("select * from CDC_DATA_SOURCE order by DATA_SOURCE_ID");
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            int row = 0;
            while (rs.next()) {
                row++;
                StringBuilder sb = new StringBuilder();
                sb.append("ROW ").append(row).append("\t");
                for (int i = 1; i <= n; i++) {
                    String k = md.getColumnName(i);
                    Object v = rs.getObject(i);
                    String s = v == null ? "NULL" : String.valueOf(v).replace('\n',' ').replace('\r',' ');
                    sb.append(k).append("=").append(s);
                    if (i < n) sb.append(" | ");
                }
                System.out.println(sb.toString());
            }
            System.out.println("TOTAL_ROWS=" + row);
        }
    }
}
