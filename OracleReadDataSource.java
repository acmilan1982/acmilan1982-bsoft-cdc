import java.sql.*;

public class OracleReadDataSource {
    private static String nvl(String s){ return s==null?"":s; }
    public static void main(String[] args) throws Exception {
        String url = "jdbc:oracle:thin:@//192.168.1.15:1521/orcl";
        String user = "CDC";
        String pass = "CDC";
        Class.forName("oracle.jdbc.OracleDriver");

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            // table comment
            try (PreparedStatement ps = conn.prepareStatement(
                    "select comments from user_tab_comments where table_name = 'CDC_DATA_SOURCE'");
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    System.out.println("TABLE_COMMENT\t" + nvl(rs.getString(1)).replace('\n',' ').replace('\r',' '));
                } else {
                    System.out.println("TABLE_COMMENT\t");
                }
            }

            System.out.println("-- COLUMNS --");
            String colSql =
                    "select c.column_id, c.column_name, c.data_type, c.data_length, c.data_precision, c.data_scale, c.nullable, c.data_default, cc.comments " +
                    "from user_tab_columns c " +
                    "left join user_col_comments cc on cc.table_name = c.table_name and cc.column_name = c.column_name " +
                    "where c.table_name = 'CDC_DATA_SOURCE' order by c.column_id";
            try (PreparedStatement ps = conn.prepareStatement(colSql);
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int id = rs.getInt("column_id");
                    String name = rs.getString("column_name");
                    String type = rs.getString("data_type");
                    int len = rs.getInt("data_length");
                    Object precObj = rs.getObject("data_precision");
                    Object scaleObj = rs.getObject("data_scale");
                    String nullable = rs.getString("nullable");
                    String def = rs.getString("data_default");
                    String cmt = rs.getString("comments");

                    String typeDetail;
                    if ("VARCHAR2".equals(type) || "CHAR".equals(type) || "NVARCHAR2".equals(type) || "NCHAR".equals(type)) {
                        typeDetail = type + "(" + len + ")";
                    } else if ("NUMBER".equals(type)) {
                        if (precObj == null) typeDetail = "NUMBER";
                        else if (scaleObj == null) typeDetail = "NUMBER(" + precObj + ")";
                        else typeDetail = "NUMBER(" + precObj + "," + scaleObj + ")";
                    } else {
                        typeDetail = type;
                    }

                    String line = id + "\t" + name + "\t" + typeDetail + "\t" + nullable + "\t"
                            + nvl(def).replace('\n',' ').replace('\r',' ').trim() + "\t"
                            + nvl(cmt).replace('\n',' ').replace('\r',' ');
                    System.out.println(line);
                }
            }

            System.out.println("-- DATA (ROWNUM<=100) --");
            try (PreparedStatement ps = conn.prepareStatement("select * from CDC_DATA_SOURCE where rownum <= 100 order by ID");
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
                System.out.println("TOTAL_ROWS_FETCHED=" + row);
            }
        }
    }
}
