//package com.bsoft.server.k2db;
//
//import com.alibaba.druid.sql.ast.SQLStatement;
//import com.alibaba.druid.sql.ast.statement.*;
//import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleAlterTableModify;
//import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
//import com.bsoft.common.bean.*;
//import com.bsoft.common.ds.SharedHikariPool;
//import com.bsoft.common.enums.DatabaseType;
//import com.bsoft.common.enums.TableRowDeleteStrategy;
//import com.bsoft.common.global.GlobalLogCollectorManager;
//import com.bsoft.common.log.LogCollector;
////import com.bsoft.server.sql.ddl.DdlStrategy;
////import com.bsoft.server.sql.ddl.DdlStrategyFactory;
//import com.bsoft.server.sql.ddl.DdlStrategy;
//import com.bsoft.server.sql.ddl.DdlStrategyFactory;
//import com.bsoft.server.sql.delete.DeleteStrategy;
//import com.bsoft.server.sql.delete.DeleteStrategyFactory;
//import com.bsoft.server.sql.insert.InsertStrategy;
//import com.bsoft.server.sql.insert.InsertStrategyFactory;
//import com.bsoft.server.sql.update.UpdateStrategy;
//import com.bsoft.server.sql.update.UpdateStrategyFactory;
//import com.bsoft.common.util.CdcSqlConstants;
//import com.bsoft.common.global.GlobalZooKeeperManager;
//import com.zaxxer.hikari.HikariConfig;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.util.Collector;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import javax.sql.DataSource;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.time.LocalDateTime;
//import java.util.*;
//
//import java.util.stream.Collectors;
//
//
//public class DatabaseRichProcessFunction extends ProcessFunction<KafkaPayload, OperationResult> {
//
//
//
//    private static final Logger logger = LogManager.getLogger(DatabaseRichProcessFunction.class);
//    // 共享池需要保存 cfg 做 release 的指纹
//    private transient HikariConfig hikariConfig;
//    // 目标库的数据源
//    private transient DataSource hikariDataSource;
//
//    private  ServerInfo serverInfo;
//
//    private ServerTopicInfo serverTopicInfo;
//
//    // 目标库配置
//    private TargetDataSourceConfig targetDataSourceConfig;
//
//    // 目标表表名
//    private String tableName;
//
//    // 表行删除策略
//    private TableRowDeleteStrategy tableRowDeleteStrategy;
//
//    // 用于写入当前消息的偏移量
//    private transient GlobalZooKeeperManager offsetManager;
//
//    // 移除原有的 LogCollector logCollector 字段
//    private transient LogCollector logCollector;
//
//
//
//
//
//    // 每个DatabaseRichProcessFunction，只写入指定目标库中的指定表
//    public DatabaseRichProcessFunction(ServerInfo serverInfo, ServerTopicInfo serverTopicInfo) {
//
//        this.serverInfo = serverInfo;
//        this.serverTopicInfo = serverTopicInfo;
//        // 获取目标表名
//        this.tableName = serverTopicInfo.getTargetTableName();
//        // 获取目标库配置
//        this.targetDataSourceConfig = serverInfo.getTargetDataSourceConfigRegistry().findTargetDataSourceConfigByDataSourceId(serverTopicInfo.getTargetDataSourceId());
//        // 获取表行删除策略
//        this.tableRowDeleteStrategy = serverInfo.getTableRowDeleteStrategy();
//
//
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        // 1. 构建 HikariConfig
//        this.hikariConfig = buildHikariConfig(this.targetDataSourceConfig);
//
//        // 2. 从共享池获取（可能是复用）
//        this.hikariDataSource = SharedHikariPool.acquire(this.hikariConfig);
//
//        // 3.初始化全局offset管理器（单例模式，只会创建一个实例）
//        this.offsetManager = GlobalZooKeeperManager.getInstance(serverInfo);
//
//
//        // 4.使用全局共享的 LogCollector
//        this.logCollector = GlobalLogCollectorManager.getInstance(
//                this.serverInfo.getConfigDataSourceConfig(),
//                this.targetDataSourceConfig.getDataSourceId()
//        );
//
//        // 5. 注册当前处理的表名到全局offset管理器
//        this.offsetManager.addSourceTableName(this.serverTopicInfo);
//    }
//
//    public void processElement(KafkaPayload row, Context ctx, Collector<OperationResult> out) throws Exception {
//        long startTime = System.currentTimeMillis();
//        String op = row.getOp();
//
//        // 当前消息的操作结果，输出到窗口进行统计
//        OperationResult result = new OperationResult();
//        result.setServerId(serverInfo.getServerId());
//        result.setDataFromSourceId(row.getBusinessDataSourceId());
//        result.setDateToSourceId(targetDataSourceConfig.getDataSourceId());
//        result.setTableName(tableName);
//        result.setOperation(op);
//        result.setOriginalPayload(row);
//
//
//        try {
//            switch (op) {
//                case "r":
//                case "c":
//                    invokeInsert(row);
//                    break;
//                case "u":
//                    invokeUpdate(row);
//                    break;
//                case "d":
//                    invokeDelete(row);
//                    break;
//                case "ddl":
//                    invokeDDL(row);
//                    break;
//                default:
//                    logger.info("⚠️ Unknown operation type: " + op + ", skipped.");
//                    result.setSuccess(false);
//                    result.setErrorMessage("Unknown operation type: " + op);
//            }
//
//            result.setSuccess(true);
//        } catch (Exception e) {
//            result.setSuccess(false);
//            result.setErrorMessage(e.getMessage());
//            logger.info("❌ Operation failed: " + e.getMessage());
//        } finally {
//            long executionTime = System.currentTimeMillis() - startTime;
//            result.setExecutionTimeMs(executionTime);
//
//            Map<String,String> metric = new HashMap<>();
//            metric.put("topic",this.serverTopicInfo.getTopic());
//            metric.put("targetDataSourceId",this.targetDataSourceConfig.getDataSourceId());
//            metric.put("targetTableName",this.tableName);
//            metric.put("offset",row.getOffset().toString());
//
//            // 异步更新offset到全局管理器（不阻塞主流程）
//            offsetManager.updateOffset(metric);
//            // 输出结果供下游统计使用
//            out.collect(result);
//        }
//    }
//
//
//    public void invokeInsert(KafkaPayload row) throws Exception {
//        // 获取待插入数据的 字段名->字段值
//        Map<String, Object> afterData = row.getAfterData();
//
//
//
//        // 增加 op,dml_Time，kafka_enqueue_time
//        afterData.put("cdc_op",row.getOp());
//        afterData.put("cdc_dml_Time",row.getDmlTime());
//        afterData.put("cdc_kafka_enqueue_time", row.getKafkaEnqueueTime());
//
//
//        // 增加 cdc_insert_time 字段
//        afterData.put("cdc_insert_time", LocalDateTime.now());
//
//        // 获取待插入数据的 字段名
//        List<String>  filedList = new ArrayList<>(afterData.keySet());
//
//        // 生产库中 date 类型的字段
//        Set<String> dateFieldSet = row.getDateFieldSet();
//
//        // 获取主键信息
//        Set<String> primaryKeys = row.getPrimaryKeySet();
//
//        // 获取插入策略
//        DatabaseType dbType = targetDataSourceConfig.getDataSourceType();
//        InsertStrategy strategy = InsertStrategyFactory.getStrategy(dbType);
//
//        // 构建SQL
//        String sql = strategy.buildInsertSql(tableName, filedList, primaryKeys,dateFieldSet);
//
//        boolean success = true;
//        String logDetail = "插入成功";
//
//        try (Connection conn = hikariDataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            strategy.setParameters(ps, afterData, filedList,dateFieldSet);
//
//
//            // 执行插入
//            ps.executeUpdate();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            success = false;
//            logDetail = LogCollector.getSafeStackTraceHead(e, 40, 4000);
//        } finally {
//            log(row, success, logDetail);
//        }
//
//
//    }
//
//    public void invokeUpdate(KafkaPayload row) throws Exception {
//        Map<String, Object> afterData = row.getAfterData();
//        List<String> keys = new ArrayList<>(afterData.keySet());
//        Set<String> primaryKeySet = row.getPrimaryKeySet();
//
//        // 没有主键的表，用ROWID
//        if(primaryKeySet.isEmpty()){
//            primaryKeySet.add(CdcSqlConstants.CDC_ROWID_FIELD);
//        }
//
//        // 生产库中 date 类型的字段
//        Set<String> dateFieldSet = row.getDateFieldSet();
//
//        // 过滤掉主键字段，检查是否有可更新的字段
//        List<String> updateKeys = keys.stream()
//                .filter(key -> !primaryKeySet.contains(key))
//                .collect(Collectors.toList());
//
//        if (updateKeys.isEmpty()) {
//            logger.info("⚠️ No update columns in afterData for table: " + tableName);
//            log(row, true, "跳过更新：无可更新字段");
//            return;
//        }
//
//        keys.add("cdc_op");
//        keys.add("cdc_update_time");
//        // 增加 op
//        afterData.put("cdc_op",row.getOp());
//        // 增加 update_time 字段
//        afterData.put("cdc_update_time", LocalDateTime.now());
//
//        // 获取更新策略
//        DatabaseType dbType = targetDataSourceConfig.getDataSourceType();
//        UpdateStrategy strategy = UpdateStrategyFactory.getStrategy(dbType);
//
//        // 构建SQL
//        String sql = strategy.buildUpdateSql(tableName, keys, primaryKeySet);
//
//        boolean success = true;
//        String logDetail = "更新成功";
//
//        try (Connection conn = hikariDataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            strategy.setParameters(ps, afterData, keys, primaryKeySet,dateFieldSet);
//
//            // 执行更新
//            int affectedRows = ps.executeUpdate();
//            logger.info("🔄 Updated " + affectedRows + " rows in table: " + tableName);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            success = false;
//            logDetail = LogCollector.getSafeStackTraceHead(e, 40, 4000);
//        } finally {
//            log(row, success, logDetail);
//        }
//
//    }
//
//
//    public void invokeDelete(KafkaPayload row) throws Exception {
//        Map<String, Object> beforeData = row.getBeforeData();
//        Set<String> primaryKeySet = row.getPrimaryKeySet();
//
//
//
//
//        // 没有主键的表，用ROWID
//        if(primaryKeySet.isEmpty()){
//            primaryKeySet.add(CdcSqlConstants.CDC_ROWID_FIELD);
//        }
//
//        // 检查beforeData是否包含所有主键值
//        for (String pk : primaryKeySet) {
//            if (!beforeData.containsKey(pk.toLowerCase()) || beforeData.get(pk.toLowerCase()) == null) {
//                logger.info("⚠️ Primary key value missing in beforeData: " + pk + " for table: " + tableName);
//                log(row, false, "删除失败：主键值缺失 - " + pk);
//                return;
//            }
//        }
//
//
//
//        // 获取删除策略
//        DatabaseType dbType = targetDataSourceConfig.getDataSourceType();
//        DeleteStrategy strategy = DeleteStrategyFactory.getStrategy(dbType);
//
//        // 构建SQL
//        String sql = strategy.buildDeleteSql(tableName, primaryKeySet, tableRowDeleteStrategy);
//
//        boolean success = true;
//        String logDetail = "";
//
//        try (Connection conn = hikariDataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(sql)) {
//
//            // 设置参数
//            strategy.setParameters(ps, beforeData, primaryKeySet, tableRowDeleteStrategy);
//
//            // 执行操作
//            int affectedRows = ps.executeUpdate();
//
//            if (tableRowDeleteStrategy == TableRowDeleteStrategy.DELETE) {
//                logDetail = "删除成功";
//                logger.info("🗑️ Deleted " + affectedRows + " rows from table: " + tableName);
//            } else {
//                logDetail = "逻辑删除成功";
//                logger.info("🏷️ Marked " + affectedRows + " rows as deleted in table: " + tableName);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            success = false;
//            logDetail = LogCollector.getSafeStackTraceHead(e, 40, 4000);
//        } finally {
//            log(row, success, logDetail);
//        }
//
//    }
//
//
//    public void invokeDDL(KafkaPayload row) throws Exception {
//        // 获取DDL执行策略
//        DatabaseType dbType = targetDataSourceConfig.getDataSourceType();
//        DdlStrategy strategy = DdlStrategyFactory.getStrategy(dbType, hikariDataSource);
//
//        // 原始DDL语句
//        String ddlStatement = row.getDdlStatement();
//
//        // 解析具体的字段变更信息
//        ColumnChangeInfo columnChange = parseOracleColumnDDL(ddlStatement);
//
//        boolean success = true;
//        String logDetail = "DDL执行成功";
//
//        try {
//            if (columnChange == null) {
//                logger.warn("⚠️ 无法解析DDL语句: {}", ddlStatement);
//                success = false;
//                logDetail = "DDL解析失败：无法识别的DDL语句格式";
//                return;
//            }
//
//            // 记录DDL操作信息
//            logger.info("🔧 开始执行DDL操作: 表={}, 操作={}, 字段={}",
//                    columnChange.getTableName(),
//                    columnChange.getOperationType(),
//                    columnChange.getColumnName());
//
//            // 根据操作类型调用相应的策略方法
//            switch (columnChange.getOperationType().toUpperCase()) {
//                case "ADD":
//                    strategy.handleAddColumn(columnChange);
//                    logDetail = String.format("新增字段成功: %s %s",
//                            columnChange.getColumnName(),
//                            columnChange.getColumnType());
//                    break;
//
//                case "MODIFY":
//                    strategy.handleModifyColumn(columnChange);
//                    logDetail = String.format("修改字段成功: %s %s",
//                            columnChange.getColumnName(),
//                            columnChange.getColumnType());
//                    break;
//
//                case "DROP":
//                    strategy.handleDropColumn(columnChange);
//                    logDetail = String.format("删除字段成功: %s", columnChange.getColumnName());
//                    break;
//
//                case "RENAME":
//                    strategy.handleRenameColumn(columnChange);
//                    logDetail = String.format("重命名字段成功: %s -> %s",
//                            columnChange.getOldColumnName(),
//                            columnChange.getColumnName());
//                    break;
//
//                default:
//                    logger.warn("⚠️ 不支持的DDL操作类型: {}", columnChange.getOperationType());
//                    success = false;
//                    logDetail = "不支持的DDL操作类型: " + columnChange.getOperationType();
//            }
//
//            if (success) {
//                logger.info("✅ DDL操作执行成功: {}", logDetail);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            success = false;
////            logDetail = LogCollector.getSafeStackTraceHead(e, 40, 4000);
//            logger.error("❌ DDL操作执行失败: {}", ddlStatement, e);
////            throw e;
//        } finally {
////            log(row, success, logDetail);
//        }
//    }
//
//
//
//
//    /**
//     * 使用Druid 1.2.20解析Oracle表字段DDL语句
//     * @param ddlStatement Oracle DDL语句
//     * @return 字段变更信息
//     */
//    public  ColumnChangeInfo parseOracleColumnDDL(String ddlStatement) {
//        if (ddlStatement == null || ddlStatement.trim().isEmpty()) {
//            return null;
//        }
//
//        try {
//            // 使用Druid解析Oracle SQL
//            OracleStatementParser parser = new OracleStatementParser(ddlStatement);
//            SQLStatement statement = parser.parseStatement();
//
//            if (statement instanceof SQLAlterTableStatement) {
//                SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
//
//
//                // 获取所有ALTER操作项
//                List<SQLAlterTableItem> items = alterTable.getItems();
//
//                if (items != null && !items.isEmpty()) {
//                    SQLAlterTableItem item = items.get(0); // 处理第一个操作项
//
//                    if (item instanceof SQLAlterTableAddColumn) {
//                        // ADD COLUMN操作
//                        SQLAlterTableAddColumn addItem = (SQLAlterTableAddColumn) item;
//                        if (!addItem.getColumns().isEmpty()) {
//                            SQLColumnDefinition column = addItem.getColumns().get(0);
//
//                            String columnName = column.getColumnName();
//                            String columnType = column.getDataType().toString();
//                            return new ColumnChangeInfo(this.tableName,columnName, columnType, "ADD");
//                        }
//
//                    } else if (item instanceof SQLAlterTableDropColumnItem) {
//                        // DROP COLUMN操作
//                        SQLAlterTableDropColumnItem dropItem = (SQLAlterTableDropColumnItem) item;
//                        if (!dropItem.getColumns().isEmpty()) {
//                            String columnName = dropItem.getColumns().get(0).getSimpleName();
//                            return new ColumnChangeInfo(this.tableName,columnName, null, "DROP");
//                        }
//
//                    } else if (item instanceof OracleAlterTableModify) {
//                        // MODIFY COLUMN操作
//                        OracleAlterTableModify oracleAlterTableModify = (OracleAlterTableModify) item;
//                        String columnName = oracleAlterTableModify.getColumns().get(0).getColumnName();
//                        String columnType = oracleAlterTableModify.getColumns().get(0).getDataType() != null ?
//                                oracleAlterTableModify.getColumns().get(0).getDataType().toString() : null;
//                        return new ColumnChangeInfo(this.tableName,columnName, columnType, "MODIFY");
//
//                    } else if (item instanceof SQLAlterTableRenameColumn) {
//                        // RENAME COLUMN操作
//                        SQLAlterTableRenameColumn renameItem = (SQLAlterTableRenameColumn) item;
//                        String oldColumnName = renameItem.getColumn().getSimpleName();
//                        String newColumnName = renameItem.getTo().getSimpleName();
//                        return new ColumnChangeInfo(this.tableName,newColumnName, null, "RENAME", oldColumnName);
//                    }
//
//                    logger.debug("🔍 未识别的ALTER操作类型: {}", item.getClass().getSimpleName());
//                }
//
//                logger.info("🔍 使用Druid解析DDL成功: {}", ddlStatement);
//            }
//
//        } catch (Exception e) {
//            logger.error("❌ Druid解析Oracle DDL失败: {}", ddlStatement, e);
//        }
//
//        return null;
//    }
//
//    public static void main(String[] args) {
//        // 测试各种DDL语句
//        String[] testDDLs = {
//                "ALTER TABLE test_table ADD column_name VARCHAR2(100)",
//                "ALTER TABLE test_table MODIFY column_name NUMBER(10,2)",
//                "ALTER TABLE test_table DROP COLUMN column_name",
//                "ALTER TABLE test_table RENAME COLUMN old_name TO new_name"
//        };
//
//        for (String ddl : testDDLs) {
////            System.out.println("DDL: " + ddl);
//////            ColumnChangeInfo result = parseOracleColumnDDL(ddl);
////            System.out.println("Result: " + result);
////            System.out.println("---");
//        }
//    }
//    private void log(KafkaPayload row,
//                     boolean success,
//                     String logDetail) {
//        SyncLogEntry syncLogEntry = SyncLogEntry.builder()
//                .setInstructionType(row.getOp())              // 操作类型
////                .setSourceTime(row.getDmlTime())              // dml事务提交时间
//                .setTargetTime(LocalDateTime.now())           // 当前时间，近似落盘到目标库的时间
//                .setLogDetail(logDetail)                       // 日志详情
//                .setSourceDataSourceId(row.getBusinessDataSourceId())  // 源库的 数据源 id
//                .setTargetDataSourceId(this.targetDataSourceConfig.getDataSourceId()) //目标库  数据源 id
//                .setSourceTableName(row.getTableName())   // 源表名
//                .setTargetTableName(row.getTableName())  // 目标库表名
//                .setResultCode(success ? 0 : 1)          // 是否执行成功
//                .setOffset(row.getOffset())
////                .setKafkaEnqueueTime(row.getKafkaEnqueueTime())
//                .setRawMessage(row.getRawMessage())
//                .setSourceSchemaName(row.getSchemaName())
//                .build();
//
//        // 使用共享的日志收集器
//        logCollector.add(syncLogEntry);
//    }
//
//
//
//    @Override
//    public void close() throws Exception {
//        if (logCollector != null) logCollector.shutdown();
//        if (hikariConfig != null) SharedHikariPool.release(hikariConfig);
//
//    }
//
//
//
//    /** 根据目标库配置构造一个保守的 HikariConfig（不在 open 之外共享/序列化） */
//    private HikariConfig buildHikariConfig(TargetDataSourceConfig t) {
//        HikariConfig c = new HikariConfig();
//        c.setJdbcUrl(t.getUrl());
//        c.setUsername(t.getDataSourceUserName());
//        c.setPassword(t.getDataSourcePassword());
//
//        DatabaseType tp = t.getDataSourceType();
//        if (tp == DatabaseType.MYSQL) {
//            c.setDriverClassName("com.mysql.cj.jdbc.Driver");
//            c.setMaximumPoolSize(100);
//            c.setMinimumIdle(0);
//            c.setInitializationFailTimeout(-1); // 初始化拿不到连接也不失败
//            c.setConnectionTimeout(30000);
//            c.setValidationTimeout(5000);
//            c.setIdleTimeout(600000);
//            c.setMaxLifetime(1800000);
//            c.setConnectionTestQuery("SELECT 1");
//            c.addDataSourceProperty("useUnicode", "true");
//            c.addDataSourceProperty("characterEncoding", "utf8");
//            c.addDataSourceProperty("useSSL", "false");
//            c.addDataSourceProperty("allowMultiQueries", "true");
//        } else if (tp == DatabaseType.ORACLE) {
//            c.setDriverClassName("oracle.jdbc.OracleDriver");
//            c.setMaximumPoolSize(100);          // 单 TM 1~2 条连接足够
//            c.setMinimumIdle(0);
//            c.setInitializationFailTimeout(-1);
//            c.setConnectionTimeout(60000);
//            c.setValidationTimeout(5000);
//            c.setIdleTimeout(600000);
//            c.setMaxLifetime(1800000);
//            c.setConnectionTestQuery("SELECT 1 FROM DUAL");
//            // Oracle 网络超时
//            c.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "60000");
//            c.addDataSourceProperty("oracle.jdbc.ReadTimeout", "60000");
//        }
//
//        c.setPoolName(tp + "-" + t.getDataSourceUserName());
//        return c;
//    }
//
//
//
//
//}
