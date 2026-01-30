package com.bsoft.common.util;

/**
 * ClassName: CdcSqlConstants
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/1 - 9:19
 */

/**
 * ClassName: CdcSqlConstants
 * Package:   com.bsoft.util
 * Description: CDC相关的SQL常量定义
 *
 * @Author: snoopy
 * @Create: 2025/8/1
 */
public final class CdcSqlConstants {

    private CdcSqlConstants() {
        // 私有构造函数，防止实例化
    }

    // CDC相关常量
    public static final String CDC_ROWID_FIELD = "cdc_rowid";
    public static final String ROWID_HEADER_KEY = "ROWID";
    // ========== 客户端相关SQL ==========
    /**
     * 检查客户端是否存在
     */
    public static final String FETCH_CLIENT_SQL = "SELECT COUNT(1) FROM CDC_CLIENT WHERE client_id = ?";

    /**
     * 注册新客户端
     */
    public static final String REGISTER_CLIENT_SQL = "INSERT INTO CDC_CLIENT (CLIENT_ID, CLIENT_DESC, FG_ACTIVE) VALUES (?, ?, ?)";

    // ========== 服务端相关SQL ==========
    /**
     * 检查服务端是否存在
     */
    public static final String FETCH_SERVER_SQL = "SELECT COUNT(1) FROM CDC_SERVER WHERE SERVER_ID = ?";

    /**
     * 注册新服务端
     */
    public static final String REGISTER_SERVER_SQL = "INSERT INTO CDC_SERVER (SERVER_ID, SERVER_DESC, FG_ACTIVE) VALUES (?, ?, ?)";

    // ========== 数据源相关SQL ==========
    /**
     * 根据客户端ID获取数据源配置
     */
    public static final String FETCH_DATA_SOURCE_SQL =
            "SELECT * FROM CDC_CLIENT A " +
                    "INNER JOIN CDC_DATA_SOURCE B ON (B.DATA_SOURCE_ID = A.DATA_SOURCE_ID) " +
                    "WHERE A.FG_ACTIVE = '1' AND B.FG_ACTIVE = '1' AND A.CLIENT_ID = ?";

    /**
     * 获取所有有效的源数据源
     */
    public static final String FETCH_ALL_SOURCE_DATA_SOURCE_SQL =  
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

    // 获取所有多对一的表(多个源表汇总到一个目标表)
    public static final String FETCH_ALL_MERGE_TABLE_SQL =
            "SELECT * FROM CDC_MERGE_TABLE T WHERE T.FG_ACTIVE = '1'";

    /**
     * 获取目标数据源ID列表
     */
    public static final String FETCH_ALL_TARGET_DATA_SOURCE_ID_SQL =
            "SELECT T.DATA_TO_SOURCE_ID FROM CDC_DATA_SUBSCRIBE T WHERE T.FG_ACTIVE = '1'";

    /**
     * 获取指定业务库的目标数据源ID列表
     */
    public static final String FETCH_TARGET_DATA_SOURCE_ID_BY_BUSINESS_DATA_SOURCE_ID_SQL =
            "SELECT T.DATA_TO_SOURCE_ID FROM CDC_DATA_SUBSCRIBE T WHERE T.DATA_FROM_SOURCE_ID like ? and T.FG_ACTIVE = '1'";

    /**
     * 根据ID列表获取目标数据源配置
     */
    public static final String FETCH_TARGET_DATA_SOURCE_SQL =
            "SELECT * FROM CDC_DATA_SOURCE t WHERE t.data_source_id IN ({}) AND t.fg_active = '1'";

    // ========== 数据订阅相关SQL ==========
    /**
     * 根据数据源ID获取订阅表信息
     */
    public static final String FETCH_TABLE_SQL =
            "SELECT * FROM CDC_DATA_SUBSCRIBE t WHERE t.DATA_FROM_SOURCE_ID = ?";

    /**
     * 获取所有有效的数据订阅关系
     */
    public static final String FETCH_DATA_SUBSCRIBE_SQL = "SELECT * FROM CDC_DATA_SUBSCRIBE A WHERE A.FG_ACTIVE = '1' ";

    /**
     * 获取client在数据库中的配置
     */
    public static final String FETCH_SIMPLE_CLIENT_SQL = "SELECT * FROM CDC_CLIENT T WHERE T.FG_ACTIVE = '1'";

    /**
     * 获取所有有效的数据订阅关系
     */
    public static final String FETCH_DATA_SUBSCRIBE_BY_BUSINESS_DATA_SOURCE_ID_SQL =
            "SELECT * FROM CDC_DATA_SUBSCRIBE t WHERE T.DATA_FROM_SOURCE_ID like ? and t.fg_active = '1'";


    // ========== 执行计划相关SQL ==========
    /**
     * 获取数据源的执行计划
     */
    public static final String FETCH_EXECUTION_PLAN =
            "SELECT \n" +
            "* \n" +
            "FROM CDC_EXECUTION_PLAN A \n" +
            "LEFT JOIN CDC_CLIENT B ON (B.DATA_SOURCE_ID = A.FROM_DATA_SOURCE_ID)\n" +
            "WHERE A.FROM_DATA_SOURCE_ID = ? AND B.FG_ACTIVE = '1'";

    /**
     * 插入执行计划
     */
    public static final String INSERT_EXECUTION_PLAN =
            "INSERT INTO CDC_EXECUTION_PLAN (FROM_DATA_SOURCE_ID, TO_DATA_SOURCE_ID,SCHEMA_NAME, TABLE_NAME, TABLE_STATUS) VALUES (?, ?, ?, ?, ?)";

    /**
     * 删除执行计划
     */
    public static final String DELETE_EXECUTION_PLAN =
            "DELETE FROM CDC_EXECUTION_PLAN T WHERE T.FROM_DATA_SOURCE_ID = ? AND T.TO_DATA_SOURCE_ID = ? AND T.SCHEMA_NAME = ? AND T.TABLE_NAME = ?";

    /**
     * 更新执行计划状态
     */
    public static final String UPDATE_EXECUTION_PLAN =
            "UPDATE CDC_EXECUTION_PLAN t SET t.TABLE_STATUS = ? WHERE T.FROM_DATA_SOURCE_ID = ? AND T.TO_DATA_SOURCE_ID = ? AND t.schema_name = ? AND t.table_name = ?";

    // ========== 数据库元数据相关SQL ==========
//    /**
//     * MySQL - 检查表是否存在
//     */
//    public static final String MYSQL_CHECK_TABLE_EXISTS =
//            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?";

    /**
     * Oracle - 获取约束信息（主键等）
     */
    public static final String ORACLE_CONSTRAINT_QUERY =
            "SELECT ac.owner, ac.table_name, ac.constraint_name, ac.constraint_type, " +
                    "acc.column_name, acc.position " +
                    "FROM all_constraints ac " +
                    "JOIN all_cons_columns acc ON ac.owner = acc.owner AND ac.constraint_name = acc.constraint_name " +
                    "JOIN TEMP_SCHEMA_TABLE tt ON ac.owner = tt.SCHEMA_NAME AND ac.table_name = tt.TABLE_NAME " +
                    "WHERE ac.constraint_type IN ('P', 'U', 'R') " +
                    "ORDER BY ac.constraint_name, acc.position";

    /**
     * Oracle - 创建临时表
     */
    public static final String ORACLE_CREATE_TEMP_TABLE =
            "CREATE GLOBAL TEMPORARY TABLE TEMP_SCHEMA_TABLE (SCHEMA_NAME VARCHAR2(128), TABLE_NAME VARCHAR2(128)) ON COMMIT DROP";

    /**
     * Oracle - 删除临时表
     */
    public static final String ORACLE_DROP_TEMP_TABLE =
            "DROP TABLE TEMP_SCHEMA_TABLE";

    /**
     * Oracle - 插入临时表数据
     */
    public static final String ORACLE_INSERT_TEMP_TABLE =
            "INSERT INTO TEMP_SCHEMA_TABLE (SCHEMA_NAME, TABLE_NAME) VALUES (?, ?)";


    public static final String FETCH_TARGET_DATA_SOURCE_ID_SQL = "SELECT T.DATA_TO_SOURCE_ID FROM CDC_DATA_SUBSCRIBE T WHERE T.FG_ACTIVE = '1'";


    /**
     * 插入建表记录
     */
    public static final String INSERT_TABLE_CREATION_LOG =
            "INSERT INTO CDC_TABLE_CREATION_LOG (CDC_TABLE_CREATION_LOG_ID, SOURCE_DATA_SOURCE_ID, " +
                    "SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME,TARGET_DATA_SOURCE_ID, TARGET_SCHEMA_NAME, " +
                    "TARGET_TABLE_NAME, FG_ACTIVE) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    /**
     * 检查建表记录是否存在
     */
    public static final String CHECK_TABLE_CREATION_RECORD_EXISTS =
            "SELECT COUNT(1) FROM CDC_TABLE_CREATION_LOG WHERE SOURCE_DATA_SOURCE_ID = ? " +
                    "AND SOURCE_SCHEMA_NAME = ? AND SOURCE_TABLE_NAME = ? "+
                    "AND TARGET_DATA_SOURCE_ID = ? AND TARGET_SCHEMA_NAME = ? AND FG_ACTIVE = '1'";

    /**
     * 查询建表记录的目标表名
     */
    public static final String QUERY_TARGET_TABLE_NAME_FROM_CREATION_LOG =
            "SELECT TARGET_TABLE_NAME FROM CDC_TABLE_CREATION_LOG WHERE SOURCE_DATA_SOURCE_ID = ? " +
                    "AND SOURCE_SCHEMA_NAME = ? AND SOURCE_TABLE_NAME = ? " +
                    "AND TARGET_DATA_SOURCE_ID = ? AND TARGET_SCHEMA_NAME = ? AND FG_ACTIVE = '1'";

// ========== 数据库版本查询相关SQL ==========
    /**
     * Oracle - 查询数据库版本
     */
    public static final String ORACLE_VERSION_QUERY =
            "SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1";

    /**
     * Doris - 查询数据库版本
     */
    public static final String DORIS_VERSION_QUERY =
            "SELECT @@version_comment";

    /**
     * MySQL - 查询数据库版本
     */
    public static final String MYSQL_VERSION_QUERY =
            "SELECT VERSION()";

//// ========== 表存在性检查相关SQL ==========
    /**
     * MySQL - 检查表是否存在
     */
    public static final String MYSQL_CHECK_TABLE_EXISTS =
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?";

    /**
     * Oracle - 检查表是否存在
     */
    public static final String ORACLE_CHECK_TABLE_EXISTS =
            "SELECT COUNT(*) FROM user_tables WHERE table_name = ?";

    /**
     * Doris - 检查表是否存在
     */
    public static final String DORIS_CHECK_TABLE_EXISTS =
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";


// ========== 日志插入相关SQL ==========
    /**
     * 插入正确日志（包含 RAW_MESSAGE）
     */
    public static final String INSERT_CDC_LOG_CORRECT =
            "INSERT INTO CDC_LOG_CORRECT (CDC_LOG_ID, INSTRUCTION_TYPE, SOURCE_TIME, TARGET_TIME, INSERT_TIME, " +
                    "LOG_DETAIL, SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE_NAME, TARGET_TABLE_NAME, " +
                    "RESULT_DETAIL, RESULT_CODE, OFFSET, KAFKA_ENQUEUE_TIME, SOURCE_SCHEMA_NAME,RAW_MESSAGE) " +
                    "VALUES (?, ?, TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), sysdate, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), ?, ?)";


    /**
     * 插入错误日志
     */
    public static final String INSERT_CDC_LOG_ERROR =
            "INSERT INTO CDC_LOG_ERROR (CDC_LOG_ID, INSTRUCTION_TYPE, SOURCE_TIME, TARGET_TIME, INSERT_TIME, " +
                    "LOG_DETAIL, SOURCE_DATA_SOURCE_ID, TARGET_DATA_SOURCE_ID, SOURCE_TABLE_NAME, TARGET_TABLE_NAME, " +
                    "RESULT_DETAIL, RESULT_CODE, OFFSET, KAFKA_ENQUEUE_TIME, SOURCE_SCHEMA_NAME, RAW_MESSAGE) " +
                    "VALUES (?, ?, TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), sysdate, ?, ?, ?, ?, ?, ?, ?, ?, TO_DATE(?, 'yyyy-mm-dd hh24:mi:ss'), ?, ?)";




}
