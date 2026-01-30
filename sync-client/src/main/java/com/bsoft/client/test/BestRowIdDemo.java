package com.bsoft.client.test;

/**
 * ClassName: BestRowIdDemo
 * Package:   com.bsoft.client.test
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/24 - 1:19
 */
import java.sql.*;

public class BestRowIdDemo {
    public static void main(String[] args) throws Exception {


         String FETCH_ALL_SOURCE_DATA_SOURCE_SQL =
                "SELECT T.*,Z.CLIENT_ID FROM CDC_DATA_SOURCE T  " +
                        "INNER JOIN( " +
                        "SELECT A.DATA_SOURCE_ID,C.CLIENT_ID FROM CDC_DATA_SOURCE A  " +
                        "INNER JOIN (  " +
                        "SELECT T.DATA_SUB_ID,  " +
                        "       T.FG_ACTIVE,  " +
                        "       REGEXP_SUBSTR(T.DATA_FROM_SOURCE_ID, '[^,]+', 1, LEVEL) AS DATA_FROM_SOURCE_ID  " +
                        "FROM CDC_DATA_SUBSCRIBE T  " +
                        "CONNECT BY LEVEL <= REGEXP_COUNT(T.DATA_FROM_SOURCE_ID, ',') + 1  " +
                        "       AND PRIOR DATA_SUB_ID = DATA_SUB_ID  " +
                        "       AND PRIOR DBMS_RANDOM.VALUE IS NOT NULL  " +
                        "  " +
                        ") B ON (B.DATA_FROM_SOURCE_ID = A.DATA_SOURCE_ID)  " +
                        "INNER JOIN CDC_CLIENT C ON (C.DATA_SOURCE_ID = B.DATA_FROM_SOURCE_ID)  " +
                        "WHERE upper(A.DATA_SOURCE_CATEGORY )= 'SOURCE' AND A.FG_ACTIVE = '1' AND B.FG_ACTIVE = '1'  " +
                        "GROUP BY A.DATA_SOURCE_ID,C.CLIENT_ID)Z ON Z.DATA_SOURCE_ID = T.DATA_SOURCE_ID ";

        System.out.println(FETCH_ALL_SOURCE_DATA_SOURCE_SQL);
        String url = "jdbc:oracle:thin:@192.168.1.120:1521/orcl";
        String user = "SPT_HIS_2023_JC";
        String pass = "SPT_HIS_2023_JC";

        try (Connection conn = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData meta = conn.getMetaData();

            try (ResultSet rs = meta.getBestRowIdentifier(
                    null,      // catalog 一般传 null
                    user.toUpperCase(), // schema
                    "IPT_DAILYCOURSER",  // 替换表名
                    DatabaseMetaData.bestRowSession,
                    true)) {

                ResultSetMetaData rsmd = rs.getMetaData();
                int colCount = rsmd.getColumnCount();

                while (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        System.out.print(rsmd.getColumnName(i) + "=" + rs.getString(i) + " | ");
                    }
                    System.out.println();
                }
            }
        }
    }
}
