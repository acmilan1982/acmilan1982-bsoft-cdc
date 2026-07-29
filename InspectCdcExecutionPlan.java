import java.sql.*;

public class InspectCdcExecutionPlan {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@//192.168.1.15:1521/orcl";
        String user = "CDC";
        String pass = "CDC";
        Class.forName("oracle.jdbc.OracleDriver");
        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            System.out.println("CONNECTED=" + !conn.isClosed());

            query(conn, "select column_id, column_name, data_type, data_length, data_precision, data_scale, nullable from user_tab_columns where table_name = 'CDC_EXECUTION_PLAN' order by column_id");
            query(conn, "select index_name, uniqueness from user_indexes where table_name = 'CDC_EXECUTION_PLAN' order by index_name");
            query(conn, "select index_name, column_position, column_name from user_ind_columns where table_name = 'CDC_EXECUTION_PLAN' order by index_name, column_position");
            query(conn, "select count(*) as CNT from CDC_EXECUTION_PLAN");
            query(conn, "select * from (select * from CDC_EXECUTION_PLAN order by 1) where rownum <= 10");
        }
    }

    private static void query(Connection conn, String sql) throws Exception {
        System.out.println("SQL>>> " + sql);
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                System.out.print(md.getColumnLabel(i));
                if (i < cols) System.out.print("\t");
            }
            System.out.println();
            int row = 0;
            while (rs.next()) {
                row++;
                for (int i = 1; i <= cols; i++) {
                    Object v = rs.getObject(i);
                    System.out.print(v == null ? "NULL" : String.valueOf(v));
                    if (i < cols) System.out.print("\t");
                }
                System.out.println();
            }
            System.out.println("ROWS=" + row);
            System.out.println();
        }
    }
}
