package com.bsoft.common.constants;

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


    // ========== 客户端相关SQL ==========
    /**
     * 检查客户端是否存在
     */
    public static final String FETCH_CLIENT_SQL = "SELECT * FROM CDC_CLIENT_MULTIPLE WHERE FG_ACTIVE = '1' AND CLIENT_ID = ?  ";

    /**
     * 注册新客户端
     */
    public static final String REGISTER_CLIENT_SQL = "INSERT INTO CDC_CLIENT_MULTIPLE (CLIENT_ID, CLIENT_DESC, FG_ACTIVE) VALUES (?, ?,?)";

    // ========== 服务端相关SQL ==========
    /**
     * 检查服务端是否存在
     */
    public static final String FETCH_SERVER_SQL = "SELECT COUNT(1) FROM CDC_SERVER WHERE SERVER_ID = ?";

    /**
     * 注册新服务端
     */
    public static final String REGISTER_SERVER_SQL = "INSERT INTO CDC_SERVER (SERVER_ID, SERVER_DESC, FG_ACTIVE) VALUES (?, ?, ?)";


    /**
     * 检查当前账号是否可访问指定业务库表
     *
     * 说明：
     * 1. 这里是 SQL 模板，%s 会在代码里替换成 SCHEMA.TABLE
     * 2. 使用 ROWNUM = 0，不读取实际数据，只验证对象可访问性
     */
    public static final String ORACLE_CHECK_SOURCE_TABLE_SELECT_PRIVILEGE_SQL =
            "SELECT 1 FROM %s WHERE ROWNUM = 0";

    // ========== 数据源相关SQL ==========
    // 根据多个 DATA_SOURCE_ID 批量查询已经激活的数据源
    public static final String FETCH_DATA_SOURCE_BY_IDS_SQL =
            "SELECT * FROM CDC_DATA_SOURCE T WHERE T.DATA_SOURCE_ID IN ({}) AND T.FG_ACTIVE = '1'";


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





    /**
     * 读取 CDC_CLIENT_MULTIPLE 中所有激活客户端绑定的 DATA_SOURCE_ID
     */
    public static final String FETCH_ACTIVE_CLIENT_MULTIPLE_DATA_SOURCE_IDS_SQL =
            "SELECT T.DATA_SOURCE_ID " +
                    "FROM CDC_CLIENT_MULTIPLE T " +
                    "WHERE T.FG_ACTIVE = '1'";

    /**
     * 读取 CDC_DATA_SUBSCRIBE 中所有激活订阅的 DATA_FROM_SOURCE_ID
     */
    public static final String FETCH_ACTIVE_DATA_SUBSCRIBE_FROM_SOURCE_IDS_SQL =
            "SELECT T.DATA_FROM_SOURCE_ID " +
                    "FROM CDC_DATA_SUBSCRIBE T " +
                    "WHERE T.FG_ACTIVE = '1'";

    /**
     * 根据交集 DATA_SOURCE_ID 查询激活的源库配置，要求 CDC_DATA_SOURCE 与 CDC_DATA_SOURCE_EXTEND 存在关联
     */
    public static final String FETCH_ACTIVE_SOURCE_DATA_SOURCE_WITH_EXTEND_BY_IDS_SQL =
            "SELECT DISTINCT T.* " +
                    "FROM CDC_DATA_SOURCE T " +
                    "LEFT JOIN CDC_DATA_SOURCE_EXTEND E ON E.DATA_SOURCE_ID = T.DATA_SOURCE_ID " +
                    "WHERE T.FG_ACTIVE = '1' " +
                    "AND UPPER(T.DATA_SOURCE_CATEGORY) = 'SOURCE' " +
                    "AND T.DATA_SOURCE_ID IN ({})";


    /**
     * 按 DATA_SOURCE_ID 批量查询目标表命名规则
     */
    public static final String FETCH_DATA_SOURCE_EXTEND_BY_IDS_SQL =
            "SELECT * FROM CDC_DATA_SOURCE_EXTEND T WHERE T.DATA_SOURCE_ID IN ({})";

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
            "SELECT T.DATA_TO_SOURCE_ID " +
                    "FROM CDC_DATA_SUBSCRIBE T " +
                    "WHERE T.FG_ACTIVE = '1' " +
                    "AND ',' || T.DATA_FROM_SOURCE_ID || ',' LIKE ?";

    /**
     * 根据ID列表获取目标数据源配置
     */
    public static final String FETCH_TARGET_DATA_SOURCE_SQL =
            "SELECT * FROM CDC_DATA_SOURCE T WHERE T.DATA_SOURCE_ID IN ({}) AND T.FG_ACTIVE = '1'";

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
    public static final String FETCH_SIMPLE_CLIENT_SQL = "SELECT * FROM CDC_CLIENT_MULTIPLE T WHERE T.FG_ACTIVE = '1'";

    /**
     * 获取所有有效的数据订阅关系
     */
    public static final String FETCH_DATA_SUBSCRIBE_BY_BUSINESS_DATA_SOURCE_ID_SQL =
            "SELECT * FROM CDC_DATA_SUBSCRIBE t WHERE T.DATA_FROM_SOURCE_ID like ? and t.fg_active = '1'";


    // ========== 执行计划相关SQL ==========
    /**
     * 检查指定业务库是否已经被其他 sync-client 占用执行计划
     */
    public static final String CHECK_EXECUTION_PLAN_SOURCE_OWNERSHIP_SQL =
            "SELECT DISTINCT T.CLIENT_ID, T.FROM_DATA_SOURCE_ID " +
                    "FROM CDC_EXECUTION_PLAN T " +
                    "WHERE T.FROM_DATA_SOURCE_ID = ? " +
                    "AND T.CLIENT_ID <> ?";

    /**
     * 获取数据源的执行计划
     */
    public static final String FETCH_EXECUTION_PLAN_BY_SOURCE_IDS_SQL =
            "SELECT * FROM CDC_EXECUTION_PLAN T WHERE T.CLIENT_ID = ? AND FROM_DATA_SOURCE_ID IN ({})";

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


    /**
     * 根据服务端配置
     */
    public static final String FETCH_SERVER_CONFIG_SQL = "SELECT T.CONFIG_KEY,T.CONFIG_VALUE FROM CDC_SERVER_CONFIG T GROUP BY T.CONFIG_KEY,T.CONFIG_VALUE";

    public static final String FETCH_CDC_PIPELINE_SQL = "SELECT * FROM CDC_PIPELINE WHERE FG_ACTIVE = '1' ORDER BY PLAN_ID, SN ";


    // ========== topic offset 相关 SQL ==========
    /**
     * 按 serverId 查询全部 topic 的 nextOffset
     */
    public static final String FETCH_TOPIC_NEXT_OFFSETS_SQL = "SELECT KAFKA_TOPIC, NEXT_OFFSET FROM CDC_TOPIC_OFFSET WHERE SERVER_ID = ?";

    /**
     * 按 serverId + kafkaTopic 幂等 upsert nextOffset
     */
    public static final String MERGE_TOPIC_NEXT_OFFSET_SQL =
            "MERGE INTO CDC_TOPIC_OFFSET t " +
                    "USING ( " +                                                                      //  USING (...) s 这里先构造一行临时数据：
                    "    SELECT ? AS SERVER_ID, ? AS KAFKA_TOPIC, ? AS NEXT_OFFSET FROM dual " +      // dual 是 Oracle 里的虚拟表，常用于生成一行临时数据。
                    ") s " +                                                                          // 也就是说，s 相当于“这次我要写入的一条 offset 记录”。
                    "ON (t.SERVER_ID = s.SERVER_ID AND t.KAFKA_TOPIC = s.KAFKA_TOPIC) " +             // 这句定义“是否算同一条记录”。
                                                                                                      // 这里的判定条件是：
                                                                                                      // server_id 相同
                                                                                                      // kafka_topic 相同
                                                                                                      // 如果表里已经有这个 server + topic 的记录，就算匹配成功。
                    "WHEN MATCHED THEN " +
                    "    UPDATE SET t.NEXT_OFFSET = GREATEST(t.NEXT_OFFSET, s.NEXT_OFFSET), t.UPDATED_AT = SYSTIMESTAMP " +  //  WHEN MATCHED THEN UPDATE ... 如果表里已经有这条记录，就执行更新：
                    "WHEN NOT MATCHED THEN " +
                    "    INSERT (SERVER_ID, KAFKA_TOPIC, NEXT_OFFSET, UPDATED_AT) " +                // WHEN NOT MATCHED THEN INSERT ... 如果表里没有这条 server + topic 记录，就插入一条新记录：
                    "    VALUES (s.SERVER_ID, s.KAFKA_TOPIC, s.NEXT_OFFSET, SYSTIMESTAMP)";



    // ========== datasource scn 相关 SQL ==========
    /**
     * 按 serverId 查询全部 topic 的 nextOffset
     */
    public static final String FETCH_DATA_SOURCE_NEXT_SCNS_SQL = "SELECT * FROM CDC_DATA_SOURCE_SCN WHERE CLIENT_ID = ?";

    /**
     * 按 serverId + kafkaTopic 幂等 upsert nextOffset
     */
    public static final String MERGE_DATA_SOURCE_NEXT_SCN_SQL =
            "MERGE INTO CDC_DATA_SOURCE_SCN t " +
                    "USING ( " +                                                                      //  USING (...) s 这里先构造一行临时数据：
                    "    SELECT ? AS CLIENT_ID, ? AS DATA_SOURCE_ID, ? AS SCN FROM dual " +      // dual 是 Oracle 里的虚拟表，常用于生成一行临时数据。
                    ") s " +                                                                          // 也就是说，s 相当于“这次我要写入的一条 offset 记录”。
                    "ON (t.CLIENT_ID = s.CLIENT_ID AND t.DATA_SOURCE_ID = s.DATA_SOURCE_ID) " +             // 这句定义“是否算同一条记录”。
                    // 这里的判定条件是：
                    // DATA_SOURCE_ID 相同
                    // DATA_SOURCE_ID 相同
                    // 如果表里已经有这个 CLIENT_ID + DATA_SOURCE_ID 的记录，就算匹配成功。
                    "WHEN MATCHED THEN " +
                    "    UPDATE SET t.NEXT_OFFSET = GREATEST(t.NEXT_OFFSET, s.NEXT_OFFSET), t.UPDATED_AT = SYSDATE " +  //  WHEN MATCHED THEN UPDATE ... 如果表里已经有这条记录，就执行更新：
                    "WHEN NOT MATCHED THEN " +
                    "    INSERT (CLIENT_ID, DATA_SOURCE_ID, SCN, UPDATED_AT) " +                // WHEN NOT MATCHED THEN INSERT ... 如果表里没有这条 server + topic 记录，就插入一条新记录：
                    "    VALUES (s.CLIENT_ID, s.DATA_SOURCE_ID, s.SCN, SYSDATE)";















        // ========== 业务库运行状态相关 SQL ==========

        // 检查当前 client + dataSource 是否存在运行状态记录
        public static final String CHECK_DATA_SOURCE_RUN_STATE_EXISTS =
                "SELECT COUNT(1) FROM CDC_DATA_SOURCE_RUN_STATE WHERE CLIENT_ID = ? AND DATA_SOURCE_ID = ?";

        // 插入当前 client + dataSource 的运行状态记录
        public static final String INSERT_DATA_SOURCE_RUN_STATE =
                "INSERT INTO CDC_DATA_SOURCE_RUN_STATE " +
                        "(CLIENT_ID, DATA_SOURCE_ID, SNAPSHOT_STATUS, SNAPSHOT_LAST_SEEN_AT, SNAPSHOT_COMPLETED_AT, UPDATED_AT) " +
                        "VALUES (?, ?, ?, NULL, NULL, SYSDATE)";

        // 查询当前 client + dataSource 的 snapshot 状态
        public static final String FETCH_DATA_SOURCE_SNAPSHOT_STATUS =
                "SELECT SNAPSHOT_STATUS FROM CDC_DATA_SOURCE_RUN_STATE WHERE CLIENT_ID = ? AND DATA_SOURCE_ID = ?";

        // 标记当前 client + dataSource 的 snapshot 正在运行
        public static final String UPDATE_DATA_SOURCE_SNAPSHOT_RUNNING =
                "UPDATE CDC_DATA_SOURCE_RUN_STATE " +
                        "SET SNAPSHOT_STATUS = ?, SNAPSHOT_LAST_SEEN_AT = SYSDATE, SNAPSHOT_COMPLETED_AT = NULL, UPDATED_AT = SYSDATE " +
                        "WHERE CLIENT_ID = ? AND DATA_SOURCE_ID = ?";

        // 标记当前 client + dataSource 的 snapshot 已经完成
        public static final String UPDATE_DATA_SOURCE_SNAPSHOT_COMPLETED =
                "UPDATE CDC_DATA_SOURCE_RUN_STATE " +
                        "SET SNAPSHOT_STATUS = ?, SNAPSHOT_COMPLETED_AT = SYSDATE, UPDATED_AT = SYSDATE " +
                        "WHERE CLIENT_ID = ? AND DATA_SOURCE_ID = ?";







// ========== sync-log topic offset 相关 SQL ==========

    /**
     * 查询某个 logId + kafkaTopic 的日志 nextOffset。
     */
    public static final String FETCH_LOG_TOPIC_NEXT_OFFSET_SQL =
            "SELECT NEXT_OFFSET " +
                    "FROM CDC_LOG_TOPIC_OFFSET " +
                    "WHERE LOG_ID = ? AND KAFKA_TOPIC = ?";

    /**
     * 插入日志 topic 初始 nextOffset。
     */
    public static final String INSERT_LOG_TOPIC_INITIAL_OFFSET_SQL =
            "INSERT INTO CDC_LOG_TOPIC_OFFSET " +
                    "    (LOG_ID, KAFKA_TOPIC, NEXT_OFFSET, UPDATED_AT) " +
                    "VALUES " +
                    "    (?, ?, ?, SYSDATE)";

    /**
     * 更新日志 topic nextOffset。
     *
     * 使用 GREATEST 防止运行时代码误传较小 offset 导致位点回退。
     */
    public static final String UPDATE_LOG_TOPIC_NEXT_OFFSET_SQL =
            "UPDATE CDC_LOG_TOPIC_OFFSET " +
                    "SET NEXT_OFFSET = GREATEST(NEXT_OFFSET, ?), " +
                    "    UPDATED_AT = SYSDATE " +
                    "WHERE LOG_ID = ? AND KAFKA_TOPIC = ?";



    /**
     * 按配置 key 查询 CDC_SERVER_CONFIG 的配置值。
     */
    public static final String FETCH_SERVER_CONFIG_VALUE_SQL =
            "SELECT T.CONFIG_VALUE FROM CDC_SERVER_CONFIG T WHERE T.CONFIG_KEY = ?";



    // 插入作业失败事件。
    public static final String INSERT_JOB_FAILURE_EVENT =
            "INSERT INTO CDC_JOB_FAILURE_EVENT ("
                    + "ID, CLIENT_ID, DATA_SOURCE_ID, FAILED_JOB_ID, FAILURE_TIME, "
                    + "FLINK_STATUS, FAILURE_REASON, FAILURE_DETAIL, "
                    + "EVENT_RESULT, IGNORE_REASON, CREATED_AT"
                    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE)";



    // 插入作业失败处理记录。
    public static final String INSERT_JOB_FAILURE_HANDLE_LOG =
            "INSERT INTO CDC_JOB_FAILURE_HANDLE_LOG ("
                    + "ID, FAILURE_EVENT_ID, CLIENT_ID, DATA_SOURCE_ID, FAILED_JOB_ID, "
                    + "ATTEMPT_NO, HANDLE_STAGE, HANDLE_TIME, "
                    + "CONSECUTIVE_FAILURES, RESTART_COUNT_TOTAL, RESTART_DELAY_SECONDS, "
                    + "NEXT_RESTART_TIME, RESTART_START_TIME, RESTART_END_TIME, "
                    + "NEW_JOB_ID, ERROR_DETAIL, REMARK, CREATED_AT"
                    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE)";

}
