package com.bsoft.common.dbmeta;

import com.bsoft.common.bean.BusinessDataSourceConfig;
import com.bsoft.common.bean.TableDDLRequest;
import com.bsoft.common.bean.TableKeys;
import com.bsoft.common.bean.TableMappingConfig;
import com.bsoft.common.util.OracleKeyInspector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * ClassName: OracleTableMetadataReader
 * Package:   com.bsoft.client.dbmeta
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/24 - 17:30
 */
public class OracleTableMetadataReader {

    private static final Logger logger = LogManager.getLogger(OracleTableMetadataReader.class);
    // 业务库的数据源
    private DataSource dataSource;

    private BusinessDataSourceConfig businessDataSourceConfig;

    public OracleTableMetadataReader(BusinessDataSourceConfig businessDataSourceConfig) {
        this.businessDataSourceConfig = businessDataSourceConfig;
        this.dataSource = businessDataSourceConfig.getDataSource();
    }



    // 抓取并设置所有表的建表语句和键信息
    public void getAllTableMetadata()  {
        try {

            for (TableMappingConfig tableMappingConfig : this.businessDataSourceConfig.getTableMappingConfigList()) {
                getSingleTableMetadata(tableMappingConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void getSingleTableMetadata(TableMappingConfig tableMappingConfig)  {
        try {

                // 获取并设置建表语句
                tableMappingConfig.setDdl(getTableDDL(tableMappingConfig));
                // 获取并设置表的键信息
                tableMappingConfig.setTableKeys(getTableKeys(tableMappingConfig));
                logger.info("已抓取建表语句： {}.{}: \n{}",
                        tableMappingConfig.getSchemaName(),
                        tableMappingConfig.getTableName(),
                        tableMappingConfig.getDdl());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    // 获取每个表的建表语句,纯纯的建表语句，没有任何多余信息
    private String getTableDDL(TableMappingConfig tableMappingConfig){
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // 1. 设置DBMS_METADATA转换参数，去掉不需要的部分
            String setupParams = "            BEGIN\n" +
                    "                -- 去掉存储参数（STORAGE子句）\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);\n" +
                    "                -- 去掉表空间信息\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);\n" +
                    "                -- 去掉段属性（PCTFREE, PCTUSED, INITRANS等）\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);\n" +
                    "                -- 去掉外键约束\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE);\n" +
                    "                -- 去掉检查约束\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS', FALSE);\n" +
                    "                -- 去掉索引\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER', FALSE);\n" +
                    "                -- 添加分号结尾\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);\n" +
                    "                -- 美化格式\n" +
                    "                DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);\n" +
                    "            END;";

            conn.createStatement().execute(setupParams);

            // 2. 获取清理后的DDL
            String ddlQuery = "SELECT DBMS_METADATA.GET_DDL('TABLE', ?, ?) FROM DUAL";
            try (PreparedStatement stmt = conn.prepareStatement(ddlQuery)) {
                stmt.setString(1, tableMappingConfig.getTableName());
                stmt.setString(2, tableMappingConfig.getSchemaName());

                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    String ddl = rs.getString(1);

                    // 3. 进一步清理DDL（可选，作为双重保险）
                    return furtherCleanDdl(ddl);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Oracle表DDL失败: " + tableMappingConfig.getSchemaName() + "." + tableMappingConfig.getTableName(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


    /**
     * 进一步清理DDL，去掉可能残留的不需要部分
     */
    private String furtherCleanDdl(String originalDdl) {
        if (originalDdl == null || originalDdl.trim().isEmpty()) {
            return originalDdl;
        }

        String cleaned = originalDdl
                // 去掉可能残留的表空间声明
                .replaceAll("(?i)\\s+TABLESPACE\\s+\\w+", "")
                // 去掉存储参数
                .replaceAll("(?i)\\s+STORAGE\\s*\\([^)]+\\)", "")
                // 去掉物理属性
                .replaceAll("(?i)\\s+PCTFREE\\s+\\d+", "")
                .replaceAll("(?i)\\s+PCTUSED\\s+\\d+", "")
                .replaceAll("(?i)\\s+INITRANS\\s+\\d+", "")
                .replaceAll("(?i)\\s+MAXTRANS\\s+\\d+", "")
                // 去掉日志属性
                .replaceAll("(?i)\\s+(NO)?LOGGING", "")
                // 去掉压缩属性
                .replaceAll("(?i)\\s+(NO)?COMPRESS", "")
                // 去掉并行属性
                .replaceAll("(?i)\\s+(NO)?PARALLEL(\\s+\\d+)?", "")
                // 清理多余的空白和换行
                .replaceAll("\\s+", " ")
                .trim();

        // 确保以分号结尾
        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }

        return cleaned;
    }


    // 获取指定表的索引信息
    public TableKeys getTableKeys(TableMappingConfig tableMappingConfig) {
        try (Connection conn = dataSource.getConnection()) {
            return OracleKeyInspector.fetchKeys(
                    conn,
                    tableMappingConfig.getSchemaName(),
                    tableMappingConfig.getTableName(),
                    true,
                    true
            );

        } catch (Exception e) {
            throw new RuntimeException("获取Oracle表索引信息失败: "
                    + tableMappingConfig.getSchemaName() + "." + tableMappingConfig.getTableName(), e);
        }
    }







    /**
     * 获取Oracle表的建表DDL
     *
     * @param jdbcUrl  JDBC连接字符串，例如：jdbc:oracle:thin:@//host:port/service
     * @param dbUser   数据库登录用户名
     * @param dbPass   数据库登录密码
     * @param tableName  目标表名（大写）
     * @param owner    目标表所属用户（大写）
     * @return 建表DDL字符串
     */
    public static String getTableDDL(String jdbcUrl, String dbUser, String dbPass, String tableName, String owner) throws Exception {
        String ddl = null;
        String sql = "SELECT dbms_metadata.get_ddl('TABLE', ?, ?) FROM dual";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, tableName);
            ps.setString(2, owner);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    ddl = rs.getString(1);
                }
            }
        }
        return ddl;
    }

    // 示例主方法
    public static void main(String[] args) throws Exception {
        // 1. 配置Oracle数据源信息
        String jdbcUrl = "jdbc:oracle:thin:@//127.0.0.1:1521/orcl";
        String dbUser = "system";   // 用于连接的账号
        String dbPass = "oracle";   // 密码
        String tableName = "OPT_FEE"; // 表名（大写）
        String owner = "DEV_EHRVIEW"; // 拥有者（大写）

        String ddl = getTableDDL(jdbcUrl, dbUser, dbPass, tableName, owner);
        System.out.println(ddl);
    }
}
