package com.bsoft.server.api;

/**
 * ClassName: CdcControlController
 * Package:   com.bsoft.client.http
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/23 - 17:48
 */



import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.bsoft.common.bean.*;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.dbmeta.OracleColumnMetadata;
import com.bsoft.common.dbmeta.OracleSqlColumnsQueryRequest;
import com.bsoft.common.dbmeta.SqlResultCreateTableRequest;
import com.bsoft.common.dbmeta.TableExistsCheckRequest;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.DorisColumnModel;
import com.bsoft.common.enums.DorisTableModel;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.factory.TableNameGenerator;
import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;
import com.bsoft.server.sql.adapter.meta.MetadataReader;
import com.bsoft.server.sql.adapter.meta.MetadataWriter;
import com.bsoft.server.sql.adapter.meta.TableMetadataReaderWriterPair;
import com.bsoft.server.sql.adapter.meta.TableMetadataReaderWriterRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * 控制 CDC 任务的接口，供 Web 管理平台调用。
 */
@RestController
@RequestMapping("/api")
public class ServerController {
    private static final Logger logger = LogManager.getLogger(ServerController.class);

    // 生成目标表名的工具
    @Autowired
    private TableNameGenerator tableNameGenerator;

    // 客户端信息
    @Autowired
    private ServerInfo serverInfo;

    // 目标库的读写对象注册表
    private final TableMetadataReaderWriterRegistry tableMetadataReaderWriterRegistry = new TableMetadataReaderWriterRegistry();

    // 用于存储待处理请求的队列
    private final BlockingQueue<TableDDLRequestWrapper> requestQueue = new LinkedBlockingQueue<>();
    // SQL结果建表请求（先缓存，后串行处理）
    private final BlockingQueue<SqlResultCreateTableRequest> sqlResultRequestQueue = new LinkedBlockingQueue<>();



    // 两个常驻消费者线程
    private Thread ddlWorkerThread;
    private Thread sqlWorkerThread;

    // 关闭标记
    private final AtomicBoolean running = new AtomicBoolean(false);


    // 所有bbx流程
    private List<RulePlan> rulePlans = Collections.emptyList();

    // key: SQL类型PlanNodeSpec.id
    // value: 该SQL节点 params["doris"] 转成 DorisBizAttr
    private  Map<String, DorisBizAttr> dorisBizAttrBySqlNodeId = new LinkedHashMap<>();

    // 初始化，创建目标库对应的 读写 对象，读对象用于检查表是否存在，写对象用于创建表
    // 每个目标库，都有自己的 读写对象
    public void init(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry){
        for (TargetDataSourceConfig targetDataSourceConfig : targetDataSourceConfigRegistry.getTargetDataSourceConfigList()) {
            tableMetadataReaderWriterRegistry.register(targetDataSourceConfig);
        }

        logger.info("ServerController初始化完成...................");
    }


    public void startWorkers() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        ddlWorkerThread = new Thread(this::runDdlWorker, "ddl-request-worker");
        ddlWorkerThread.setDaemon(true);

        sqlWorkerThread = new Thread(this::runSqlResultWorker, "sql-result-worker");
        sqlWorkerThread.setDaemon(true);

        ddlWorkerThread.start();
        sqlWorkerThread.start();

        logger.info("ServerController workers started.");
    }

    @PreDestroy
    public void stopWorkers() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 通过 interrupt 打断 take() 阻塞
        if (ddlWorkerThread != null) {
            ddlWorkerThread.interrupt();
        }
        if (sqlWorkerThread != null) {
            sqlWorkerThread.interrupt();
        }

        // 可选：等待线程退出，避免资源泄露
        joinQuietly(ddlWorkerThread, 5000);
        joinQuietly(sqlWorkerThread, 5000);

        logger.info("ServerController workers stopped.");
    }

    private void joinQuietly(Thread t, long millis) {
        if (t == null) return;
        try {
            t.join(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting thread {} to stop.", t.getName());
        }
    }

    // ===== 两个消费者循环 =====

    private void runDdlWorker() {
        while (running.get()) {
            try {
                // 无数据时阻塞等待
                TableDDLRequestWrapper req = requestQueue.take();
                createTableIfNecessary(req);
            } catch (InterruptedException e) {
                logger.info("DDL worker interrupted, exiting. running={}", running.get());
                break;
            } catch (Throwable t) {
                logger.error("DDL worker process failed", t);
            }
        }
        logger.info("DDL worker exited.");
    }

    private void runSqlResultWorker() {
        while (running.get()) {
            try {
                // 无数据时阻塞等待
                SqlResultCreateTableRequest req = sqlResultRequestQueue.take();
                processSqlResultCreateTable(req);
            } catch (InterruptedException e) {
                logger.info("SQL-result worker interrupted, exiting. running={}", running.get());
                break;
            } catch (Throwable t) {
                logger.error("SQL-result worker process failed", t);
            }
        }
        logger.info("SQL-result worker exited.");
    }






    @PostMapping("/tables/import")
    private ResponseEntity<String> importTableMetadata(@RequestBody  TableDDLRequestWrapper tableDDLRequestWrapper) {
        try {
            // 将请求添加到队列中
            requestQueue.offer(tableDDLRequestWrapper);
            logger.info("收到建表请求，已加入处理队列，当前队列大小: {}", requestQueue.size());
            // 立即返回成功响应
            return ResponseEntity.ok("中心端建表成功.");
        } catch (Exception e) {
            logger.error("接收建表请求失败", e);
            return ResponseEntity.status(500).body("接收建表请求失败: " + e.getMessage());
        }
    }





    // 最常规的单表建表：根据传入的单表配置列表，创建目标库中的表（如果不存在的话），
    private void createTableIfNecessary(TableDDLRequestWrapper tableDDLRequestWrapper) throws Exception {
        // TableDDLRequestWrapper 中包含原始请求数据，包括目标库的TargetDataSourceConfig
        TargetDataSourceConfig targetDataSourceConfig = requireTargetDataSourceConfig(tableDDLRequestWrapper.getTableDDLRequest().getTargetDataSourceId());
        // 一般情况下，tableMappingConfigDTOList 只会有一个元素，表示单个表的建表请求
        TableMappingConfigDTO tableMappingConfigDTO = tableDDLRequestWrapper.getTableMappingConfigDTOList().get(0);

        TableDDLRequest tableDDLRequest = tableDDLRequestWrapper.getTableDDLRequest();

        // 目标表的类型
        DatabaseType dataSourceType = targetDataSourceConfig.getDataSourceType();

        // 走表名推导流程
        CreateTableContext ctx = CreateTableContext.forSourceTable(
                tableDDLRequest.getClientInfo(),
                tableMappingConfigDTO,
                tableMetadataReaderWriterRegistry.getTableMetadataReaderWriterPair(targetDataSourceConfig),
                dataSourceType,
                DDLBuildContext.systemDefault()  // 库级别建表参数
        );
        createTableIfNecessary(ctx);

    }



    // 通用的建表方法
    private void createTableIfNecessary(CreateTableContext ctx) throws Exception {

        MetadataReader reader = ctx.getTableMetadataReaderWriterPair().getMetadataReader();
        MetadataWriter writer = ctx.getTableMetadataReaderWriterPair().getMetadataWriter();

        TableMappingConfigDTO tableMappingConfigDTO = ctx.getTableMappingConfigDTO();

        // 目标库类型
        DatabaseType databaseType = ctx.getDatabaseType();

        // 建表参数
        DDLBuildContext ddlBuildContext = ctx.getDdlBuildContext();
        boolean needsConstraints = ctx.needsConstraints();


        // 1. 根据表的命名策略，确定最终表名 和 表是否存在
        TargetTableState targetTableState = resolveTargetTableState(reader, ctx);


        // 2. 表的最终表名
        String targetTableName = targetTableState.targetTableName;

        // 如果表存在，根据数据库类型决定是否需要扩展字段长度
        if (handleExistingTableIfNecessary(
                targetTableState.tableExists,
                databaseType,
                reader,
                writer,
                targetTableName,
                tableMappingConfigDTO.getDdl())) {
            return;
        }



        // 3. 创建表
        // 考虑到表数据合并的场景，且合并模式取决于客户端，即使现在不合并，将来可能有合并，因此干脆不使用源表的主键，统一使用CDC_ROWID作为主键,绝对不会冲突
        createTable(writer, targetTableName, tableMappingConfigDTO.getDdl(), tableMappingConfigDTO.getTableKeys(), ddlBuildContext);

        // 4. 统一新增额外的字段，比如操作类型，插入时间等
        addExtraColumnsToTable(writer, targetTableName);

        // 5. 根据数据库类型决定是否需要处理约束
        if (needsConstraints) {
            // 应用源表的主键等约束
            createTableIndexesAndConstraints(writer, targetTableName, tableMappingConfigDTO.getTableKeys());
            // 如果源表没有主键或唯一键，则为目标表添加一个主键字段
            ensureTableHasUniqueKey(writer, targetTableName);
        }

//            // 4. 记录建表信息
//            configFetcher.insertTableCreationRecord(tableMappingConfig, reader.getTargetDataSourceConfig(), targetTableName);

    }

    // 根据表的命名策略，确定最终表名 和 表是否存在
    private TargetTableState resolveTargetTableState(MetadataReader reader,
                                                     CreateTableContext ctx) throws Exception {

        TableMappingConfig tableMappingConfig = ctx.getTableMappingConfigDTO().toTableMappingConfig();

        // 判断最终表名是否已经确定，如已确定，不再生成表名
        if (ctx.usesFixedTargetTableName()) {
            String targetTableName = ctx.getFixedTargetTableName();
            boolean tableExists = reader.isTableExists(targetTableName);
            return new TargetTableState(targetTableName, tableExists);
        }

        SimpleClientInfo clientInfo = ctx.getClientInfo();
        String clientId = clientInfo.getClientId();
        TableNamingStrategy tableNamingStrategy = clientInfo.getTableNamingStrategy();
        String tableNamePrefix = clientInfo.getTableNamePrefix();
        String tableNameSuffix = clientInfo.getTableNameSuffix();
        DatabaseType databaseType = ctx.getDatabaseType();

        // 初步定义表名(可能不是最终表名)：根据目标表名的生成策略，生成目标表名
        // TABLE_MERGE,CLIENTID_SCHEMA_TABLE，CUSTOM_PREFIX:这三种模式，此时生成的表名不会变，现在生成的表名就是最终的表名
        // TABLE_PREFIX：根据表是否存在，可能会被修改
        String targetTableName = tableNameGenerator.generateTargetTableName(
                clientId,
                tableMappingConfig,
                databaseType,
                tableNamingStrategy,
                tableNamePrefix,
                tableNameSuffix
        );

        // 1. 判断目标表名是否存在
        boolean tableExists = reader.isTableExists(targetTableName);

        // 2. 如果目标表名存在，且当前的表命名策略是 TABLE_PREFIX(前缀)，生成表名，再次检查是否存在
        if (tableExists && TableNamingStrategy.TABLE_PREFIX == tableNamingStrategy) {
            targetTableName = tableNameGenerator.generateTargetTableName(
                    clientId,
                    tableMappingConfig,
                    databaseType,
                    TableNamingStrategy.CLIENTID_SCHEMA_TABLE);
            // 再次判断目标表名是否存在
            tableExists = reader.isTableExists(targetTableName);
        }

        return new TargetTableState(targetTableName, tableExists);
















    }

    // 处理表是否存在，如果存在，根据数据库类型决定是否需要扩展字段长度
    private boolean handleExistingTableIfNecessary(boolean tableExists,
                                                   DatabaseType databaseType,
                                                   MetadataReader reader,
                                                   MetadataWriter writer,
                                                   String targetTableName,
                                                   String ddl) {
        if (!tableExists) {
            return false;
        }

        // 3. 如果目标表存在且需要扩展字段长度，执行对比更新操作
        if (DatabaseType.ORACLE == databaseType && serverInfo.isAutoExpandColumnLength()) {
            // 对比并更新字段长度
            compareAndUpdateColumnLengths(reader, writer, targetTableName, ddl);
        }
        return true;
    }

    // 扩充VARCHAR字段长度
    private  void compareAndUpdateColumnLengths(
            MetadataReader reader, MetadataWriter writer, String targetTableName, String ddl) {
        try {

            logger.info("目标库-表 {} 字段自适应处理开始-------------------->", targetTableName);

            // 1. 读取目标表的列信息
            Map<String, ColumnChangeInfo> existingColumns = reader.getTableColumns(targetTableName);

            // 2. 解析DDL获取源表列信息
            Map<String, ColumnChangeInfo> sourceColumns = parseDDLColumns(ddl, targetTableName);

            // 3. 对比VARCHAR字段长度并更新
            for (Map.Entry<String, ColumnChangeInfo> sourceEntry : sourceColumns.entrySet()) {
                String columnName = sourceEntry.getKey();
                ColumnChangeInfo sourceColumn = sourceEntry.getValue();
                ColumnChangeInfo existingColumn = existingColumns.get(columnName);

                if (existingColumn != null &&
                        "VARCHAR2".equalsIgnoreCase(sourceColumn.getColumnType()) &&
                        "VARCHAR2".equalsIgnoreCase(existingColumn.getColumnType())) {

                    int sourceLength = sourceColumn.getLength();
                    int existingLength = existingColumn.getLength();

                    if (sourceLength > existingLength) {
                        logger.info("字段 {} 长度需要更新: {} -> {}", columnName, existingLength, sourceLength);
                        writer.alterColumnLength(targetTableName, columnName, sourceLength);
                    }
                }
            }

            logger.info("目标库-表 {} 字段自适应处理结束-------------------->", targetTableName);
        } catch (Exception e) {
            logger.error("对比和更新字段长度失败: {}", e.getMessage(), e);
        }
    }


    // 解析DDL，获取列信息
    private Map<String, ColumnChangeInfo> parseDDLColumns(String ddl, String tableName) {
        Map<String, ColumnChangeInfo> columns = new HashMap<>();
        OracleStatementParser parser = new OracleStatementParser(ddl);
        SQLStatement stmt = parser.parseStatement();

        if (stmt instanceof SQLCreateTableStatement) {
            SQLCreateTableStatement table = (SQLCreateTableStatement) stmt;
            for (SQLTableElement element : table.getTableElementList()) {
                if (element instanceof SQLColumnDefinition) {
                    SQLColumnDefinition colDef = (SQLColumnDefinition) element;
                    String columnName = colDef.getColumnName().replace("\"", "");
                    String dataType = colDef.getDataType().getName();

                    int length = 0;
                    if (colDef.getDataType().getArguments() != null &&
                            !colDef.getDataType().getArguments().isEmpty()) {
                        length = Integer.parseInt(colDef.getDataType().getArguments().get(0).toString());
                    }

                    ColumnChangeInfo info = new ColumnChangeInfo(tableName, columnName, dataType, length);
                    columns.put(columnName.toUpperCase(), info);
                }
            }
        }
        return columns;
    }
//
//    // oracle目标库中单个表的创建
//    private void createTableIfNecessaryForOracle(TableMappingConfigDTO tableMappingConfigDTO) throws Exception {
//        // 获取Oracle的读对象
//        for (OracleTableMetadataReaderWriterPair oracleTableMetadataReaderWriterPair : oracleTableMetadataReaderWriterRegistry.getOracleTableMetadataReaderWriterPairList()) {
//            OracleTableMetadataReader oracleTableMetadataReader = oracleTableMetadataReaderWriterPair.getOracleTableMetadataReader();
//
//            // 将 DTO 转换为 TableMappingConfig 对象
//            TableMappingConfig tableMappingConfig = tableMappingConfigDTO.toTableMappingConfig();
//            // 根据目标表名的生成策略，生成目标表名
//            String targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfig,DatabaseType.ORACLE);
//
//
//
//            // 目标表名是否存在
//            boolean tableExists = oracleTableMetadataReader.isTableExists(targetTableName);
//            // 是否有建表记录
//            boolean tableCreationRecordExists = configFetcher.checkTableCreationRecordExists(tableMappingConfig, oracleTableMetadataReader.getTargetDataSourceConfig());
//
//            // 如果目标表名存在，且有建表记录，跳过创建
//            if(tableExists && tableCreationRecordExists){
//                logger.info("表 {} 已存在，且已有建表记录，跳过创建", targetTableName);
//                continue;
//            }
//
//            // 如果目标表名存在，但没有创建记录，说明是同名的表，需要重新创建表，加前缀
//            if(tableExists && !tableCreationRecordExists){
//                targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfig,DatabaseType.ORACLE, TableNamingStrategy.CLIENTID_SCHEMA_TABLE);
//            }
//
//            // 获取Oracle的写对象
//            OracleTableMetadataWriter oracleTableMetadataWriter = oracleTableMetadataReaderWriterPair.getOracleTableMetadataWriter();
//            // 1.创建表
//            createTable(oracleTableMetadataWriter,targetTableName,tableMappingConfigDTO.getDdl(),tableMappingConfigDTO.getTableKeys());
//            // 2.统一新增额外的字段
//            addExtraColumnsToTable(oracleTableMetadataWriter,targetTableName);
//            // 3.应用源表的主键等约束
//            createTableIndexesAndConstraints(oracleTableMetadataWriter,targetTableName,tableMappingConfigDTO.getTableKeys());
//            // 4.如果源表没有主键或唯一键，则为目标表添加一个主键字段:CDC_ROWID，否则无法跟新或删除
//            ensureTableHasUniqueKey(oracleTableMetadataWriter,targetTableName);
//            this.configFetcher.insertTableCreationRecord(tableMappingConfig, oracleTableMetadataWriter.getTargetDataSourceConfig(), targetTableName);
//
//
//
//        }
//    }
//
//
//
//    // mysql目标库中单个表的创建
//    private void createTableIfNecessaryForMysql(TableMappingConfigDTO tableMappingConfigDTO) throws Exception {
//        for (MySqlTableMetadataReaderWriterPair mySqlTableMetadataReaderWriterPair : mySqlTableMetadataReaderWriterRegistry.getMySqlTableMetadataReaderWriterPairList()) {
//            // 获取MySQL的读对象
//            MySqlTableMetadataReader mySqlTableMetadataReader = mySqlTableMetadataReaderWriterPair.getMySqlTableMetadataReader();
//
//            // 将 DTO 转换为 TableMappingConfig 对象
//            TableMappingConfig tableMappingConfig = tableMappingConfigDTO.toTableMappingConfig();
//            // 根据目标表名的生成策略，生成目标表名
//            String targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfigDTO.toTableMappingConfig(),  // 将 DTO 转换为 TableMappingConfig 对象
//                                                                                DatabaseType.MYSQL);
//            // 目标表名是否存在
//            boolean tableExists = mySqlTableMetadataReader.isTableExists(targetTableName);
//            // 是否有建表记录
//            boolean tableCreationRecordExists = configFetcher.checkTableCreationRecordExists(tableMappingConfig, mySqlTableMetadataReader.getTargetDataSourceConfig());
//
//
//            // 如果目标表名存在，且有建表记录，跳过创建
//            if(tableExists && tableCreationRecordExists){
//                logger.info("表 {} 已存在，且已有建表记录，跳过创建", targetTableName);
//                continue;
//            }
//
//            // 如果目标表名存在，但没有创建记录，说明是同名的表，需要重新创建表，加前缀
//            if(tableExists && !tableCreationRecordExists){
//                targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfig,DatabaseType.ORACLE, TableNamingStrategy.CLIENTID_SCHEMA_TABLE);
//            }
//
//
//            // 获取MySQL的写对象
//            MySqlTableMetadataWriter mySqlTableMetadataWriter = mySqlTableMetadataReaderWriterPair.getMySqlTableMetadataWriter();
//
//            // 1. 创建表
//            createTable(mySqlTableMetadataWriter,targetTableName,tableMappingConfigDTO.getDdl(),tableMappingConfigDTO.getTableKeys());
//            // 2. 统一新增额外的字段
//            addExtraColumnsToTable(mySqlTableMetadataWriter,targetTableName);
//            // 3.应用源表的主键等约束
//            createTableIndexesAndConstraints(mySqlTableMetadataWriter,targetTableName,tableMappingConfigDTO.getTableKeys());
//            // 4.如果源表没有主键或唯一键，则为目标表添加一个主键字段:CDC_ROWID，否则无法跟新或删除
//            ensureTableHasUniqueKey(mySqlTableMetadataWriter,targetTableName);
//            this.configFetcher.insertTableCreationRecord(tableMappingConfig, mySqlTableMetadataWriter.getTargetDataSourceConfig(), targetTableName);
//
//
//        }
//    }
//
//
//    // doris目标库中单个表的创建
//    private void createTableIfNecessaryForDoris(TableMappingConfigDTO tableMappingConfigDTO) throws Exception {
//        // 获取Oracle的读对象
//        for (DorisTableMetadataReaderWriterPair dorisTableMetadataReaderWriterPair : dorisTableMetadataReaderWriterRegistry.getDorisTableMetadataReaderWriterPairList()) {
//            // 获取doris的读对象
//            DorisTableMetadataReader dorisTableMetadataReader = dorisTableMetadataReaderWriterPair.getDorisTableMetadataReader();
//
//            // 将 DTO 转换为 TableMappingConfig 对象
//            TableMappingConfig tableMappingConfig = tableMappingConfigDTO.toTableMappingConfig();
//            // 根据目标表名的生成策略，生成目标表名
//            String targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfigDTO.toTableMappingConfig(), // 将 DTO 转换为 TableMappingConfig 对象
//                                                                                DatabaseType.DORIS);
//
//
//            // 目标表名是否存在
//            boolean tableExists = dorisTableMetadataReader.isTableExists(targetTableName);
//            // 是否有建表记录
//            boolean tableCreationRecordExists = configFetcher.checkTableCreationRecordExists(tableMappingConfig, dorisTableMetadataReader.getTargetDataSourceConfig());
//
//            // 如果目标表名存在，且有建表记录，跳过创建
//            if(tableExists && tableCreationRecordExists){
//                logger.info("表 {} 已存在，且已有建表记录，跳过创建", targetTableName);
//                continue;
//            }
//
//            // 如果目标表名存在，但没有创建记录，说明是同名的表，需要重新创建表，加前缀
//            if(tableExists && !tableCreationRecordExists){
//                targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfig,DatabaseType.ORACLE, TableNamingStrategy.CLIENTID_SCHEMA_TABLE);
//            }
//
//
//            // 获取MySQL的写对象
//            DorisTableMetadataWriter dorisTableMetadataWriter = dorisTableMetadataReaderWriterPair.getDorisTableMetadataWriter();
//
//            // 1. 创建表
//            createTable(dorisTableMetadataWriter,targetTableName,tableMappingConfigDTO.getDdl(),tableMappingConfigDTO.getTableKeys());
//            // 2. 统一新增额外的字段
//            addExtraColumnsToTable(dorisTableMetadataWriter,targetTableName);
//
//            // doris在建表时就已经创建了主键等约束，无需额外处理
//
//            this.configFetcher.insertTableCreationRecord(tableMappingConfig, dorisTableMetadataWriter.getTargetDataSourceConfig(), targetTableName);
//
//
//
//        }
//    }

    // 创建表的通用方法
    private void createTable(MetadataWriter metadataWriter,String targetTableName,String ddl,
                             TableKeys tableKeys,DDLBuildContext ddlBuildContext){
        logger.info("表 {} 不存在，创建", targetTableName);
        String currentDbDDL = metadataWriter.convertOracleCreateTableToCurrentDB(targetTableName,ddl,tableKeys,ddlBuildContext);
        metadataWriter.executeCreateTable(currentDbDDL);
    }

    // 向目标表中添加额外的列（如有需要）
    private void addExtraColumnsToTable(MetadataWriter metadataWriter,String targetTableName){
        metadataWriter.addExtraColumnsToTable(targetTableName);
    }



    // 应用源表的主键等约束
    private void createTableIndexesAndConstraints(MetadataWriter metadataWriter, String targetTableName, TableKeys tableKeys){
        logger.info("向表 {} 应用源表的主键等约束", targetTableName);
        metadataWriter.createTableIndexesAndConstraints(targetTableName,tableKeys);

    }

    // 如果源表没有主键或唯一键，则为目标表添加一个主键字段:CDC_ROWID，否则无法跟新或删除
    private void ensureTableHasUniqueKey(MetadataWriter metadataWriter, String targetTableName){
        logger.info("为表 {} 挑选主键", targetTableName);
        metadataWriter.ensureTableHasUniqueKey(targetTableName);
    }


    @GetMapping("/ping")
    public String ping() {
        return "pong";
    }






    @PostMapping("/tables/exists")
    private ResponseEntity<Boolean> checkTableExists(@RequestBody TableExistsCheckRequest request) {
        try {
            if (request == null) {
                return ResponseEntity.ok(false);
            }

            String dataSourceId = request.getDataSourceId() == null ? null : request.getDataSourceId().trim();
            String tableName = request.getTableName() == null ? null : request.getTableName().trim();
            if (dataSourceId == null || dataSourceId.isEmpty() || tableName == null || tableName.isEmpty()) {
                return ResponseEntity.ok(false);
            }

            // 改为直接从 serverInfo 获取
            TargetDataSourceConfig targetDataSourceConfig = requireTargetDataSourceConfig(dataSourceId);

            TableMetadataReaderWriterPair pair =
                    tableMetadataReaderWriterRegistry.getTableMetadataReaderWriterPair(targetDataSourceConfig);
            if (pair == null || pair.getMetadataReader() == null) {
                logger.warn("No metadata reader found for dataSourceId={}", dataSourceId);
                return ResponseEntity.ok(false);
            }

            boolean exists = pair.getMetadataReader().isTableExists(tableName);
            return ResponseEntity.ok(exists);
        } catch (Exception e) {
            logger.error("checkTableExists failed, dataSourceId={}, tableName={}",
                    request == null ? null : request.getDataSourceId(),
                    request == null ? null : request.getTableName(),
                    e);
            return ResponseEntity.ok(false);
        }
    }


    // 改造：SQL结果建表不再直接执行，改为先入队
    @PostMapping("/tables/sql-result/import")
    private ResponseEntity<String> importSqlResultTable(@RequestBody SqlResultCreateTableRequest request) {
        try {
            sqlResultRequestQueue.offer(request);
            logger.info("收到SQL结果建表请求，已入队，queue={}", sqlResultRequestQueue.size());
            return ResponseEntity.ok("accepted");
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (Exception e) {
            logger.error("接收SQL结果建表请求失败", e);
            return ResponseEntity.status(500).body("failed: " + e.getMessage());
        }
    }


    // 自定义 sql 建表
    private void processSqlResultCreateTable(SqlResultCreateTableRequest req) throws Exception {

        TargetDataSourceConfig targetDataSourceConfig =
                requireTargetDataSourceConfig(req.getTargetDataSourceId());

        DatabaseType databaseType = targetDataSourceConfig.getDataSourceType();
        String targetTableName = req.getTargetTableName().trim();
        // SQL列元数据 -> Oracle DDL
        String oracleDDL = buildOracleCreateTableDdl(req.getTargetTableName(), req.getColumns());


        // 构造 createTableIfNecessary(...) 所需参数，复用表名冲突/重命名/约束兜底逻辑
        TableMappingConfigDTO tableMapping = new TableMappingConfigDTO();
        tableMapping.setDdl(oracleDDL);
        tableMapping.setSourceTableName(req.getTargetTableName());
        tableMapping.setSourceSchema("");

        TableMetadataReaderWriterPair pair = tableMetadataReaderWriterRegistry.getTableMetadataReaderWriterPair(targetDataSourceConfig);


        DDLBuildContext ddlBuildContext;
        if (databaseType == DatabaseType.DORIS) {
            DdlBizAttr tableBizAttr = targetDataSourceConfig.getTableLevelDdlBizAttr(targetTableName);
            ddlBuildContext = (tableBizAttr == null)
                    ? DDLBuildContext.systemDefault()
                    : DDLBuildContext.tableOverride(tableBizAttr);
        } else {
            ddlBuildContext = DDLBuildContext.systemDefault();
        }

        CreateTableContext ctx = CreateTableContext.forFixedTargetTable(
                req.getClientInfo(),
                tableMapping,
                pair,
                databaseType,
                ddlBuildContext,
                targetTableName                   // 直接指定 目标表名，无需再生成
        );

        createTableIfNecessary(ctx);

        logger.info("SQL结果建表处理完成，table={}, sqlNodeId={}", req.getTargetTableName(), req.getSqlNodeId());
    }



    // 根据列信息，生成Oracle建表DDL
    private String buildOracleCreateTableDdl(String targetTableName, List<OracleColumnMetadata> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(targetTableName).append(" (\n");

        for (int i = 0; i < columns.size(); i++) {
            OracleColumnMetadata c = columns.get(i);
            sb.append("  ").append(normalizeColumnName(c.getColumnName()))
                    .append(" ")
                    .append(formatOracleTypeFromMetadata(c));

            if ("N".equalsIgnoreCase(c.getNullable())) {
                sb.append(" NOT NULL");
            }
            if (i < columns.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    private String formatOracleTypeFromMetadata(OracleColumnMetadata c) {
        String t = normalizeOracleTypeName(c.getDataType());

        if ("NUMBER".equals(t) || "DECIMAL".equals(t) || "NUMERIC".equals(t)) {
            Integer p = c.getDataPrecision();
            Integer s = c.getDataScale();
            if (p != null && p > 0 && s != null && s >= 0) return "NUMBER(" + p + "," + s + ")";
            if (p != null && p > 0) return "NUMBER(" + p + ")";
            return "NUMBER";
        }

        if ("VARCHAR2".equals(t) || "NVARCHAR2".equals(t) || "CHAR".equals(t) || "NCHAR".equals(t)) {
            Integer len = firstPositive(c.getCharLength(), c.getDataLength());
            return t + "(" + (len == null ? 255 : len) + ")";
        }

        if ("RAW".equals(t)) {
            Integer len = firstPositive(c.getDataLength(), null);
            return "RAW(" + (len == null ? 2000 : len) + ")";
        }

        if ("TIMESTAMP".equals(t)) {
            Integer scale = c.getDataScale();
            if (scale != null && scale >= 0) return "TIMESTAMP(" + scale + ")";
            return "TIMESTAMP";
        }

        return t; // DATE/CLOB/BLOB/NCLOB/BINARY_FLOAT/... 直接保留Oracle类型
    }

    private String normalizeOracleTypeName(String raw) {
        if (raw == null || raw.trim().isEmpty()) return "VARCHAR2";
        String t = raw.trim().toUpperCase(Locale.ROOT);
        if ("VARCHAR".equals(t)) return "VARCHAR2";
        if (t.startsWith("TIMESTAMP")) return "TIMESTAMP";
        return t;
    }

    private Integer firstPositive(Integer a, Integer b) {
        if (a != null && a > 0) return a;
        if (b != null && b > 0) return b;
        return null;
    }

    private String normalizeColumnName(String name) {
        if (name == null || name.trim().isEmpty()) return "COL";
        String v = name.trim().replaceAll("[^A-Za-z0-9_]", "_");
        while (v.contains("__")) v = v.replace("__", "_");
        v = v.replaceAll("^_+", "").replaceAll("_+$", "");
        if (v.isEmpty()) v = "COL";
        if (Character.isDigit(v.charAt(0))) v = "C_" + v;
        return v.toUpperCase(Locale.ROOT);
    }


    @SuppressWarnings("unchecked")
    public void buildDorisBizAttrBySqlNodeId(List<RulePlan> plans) {
        if (plans == null || plans.isEmpty()) {
            return ;
        }
        for (RulePlan plan : plans) {
            if (plan == null || plan.getNodes() == null) continue;

            for (PlanNodeSpec node : plan.getNodes()) {
                if (node == null || node.getType() != PlanNodeType.SQL) continue;
                if (node.getId() == null || node.getId().trim().isEmpty()) continue;
                if (node.getParams() == null) continue;

                Object dorisObj = node.getParams().get("doris");
                if (!(dorisObj instanceof Map)) continue;

                Map<String, Object> dorisMap = (Map<String, Object>) dorisObj;
                DorisBizAttr bizAttr = toDorisBizAttr(dorisMap);
                this.dorisBizAttrBySqlNodeId.put(node.getId().trim(), bizAttr);
            }
        }


    }

    @SuppressWarnings("unchecked")
    private DorisBizAttr toDorisBizAttr(Map<String, Object> dorisMap) {
        DorisBizAttr biz = new DorisBizAttr();
        if (dorisMap == null || dorisMap.isEmpty()) return biz;

        Object tableModel = dorisMap.get("table_model");
        if (tableModel != null && !String.valueOf(tableModel).trim().isEmpty()) {
            biz.setTableModel(DorisTableModel.valueOf(String.valueOf(tableModel).trim().toUpperCase(Locale.ROOT)));
        }

        Object columnModel = dorisMap.get("column_model");
        if (columnModel != null && !String.valueOf(columnModel).trim().isEmpty()) {
            biz.setColumnModel(DorisColumnModel.valueOf(String.valueOf(columnModel).trim().toUpperCase(Locale.ROOT)));
        }

        Object keyColumns = dorisMap.get("key_columns");
        if (keyColumns instanceof List) {
            biz.setKeyColumns((List<String>) keyColumns);
        }

        Object hashColumns = dorisMap.get("distributed_hash_columns");
        if (hashColumns instanceof List) {
            biz.setDistributedHashColumns((List<String>) hashColumns);
        }

        Object customMapping = dorisMap.get("custom_column_mapping");
        if (customMapping instanceof Map) {
            biz.setCustomColumnMapping((Map<String, String>) customMapping);
        }

        Object buckets = dorisMap.get("buckets");
        if (buckets instanceof Number) {
            biz.setBuckets(((Number) buckets).intValue());
        }

        Object replicationNum = dorisMap.get("replication_num");
        if (replicationNum instanceof Number) {
            biz.setReplicationNum(((Number) replicationNum).intValue());
        }

        return biz;
    }

    public DorisBizAttr getDorisBizAttrBySqlNodeId(String sqlNodeId) {
        if (sqlNodeId == null) return null;
        return dorisBizAttrBySqlNodeId.get(sqlNodeId.trim());
    }

    public Map<String, DorisBizAttr> getDorisBizAttrBySqlNodeIdMap() {
        return dorisBizAttrBySqlNodeId;
    }


    private TargetDataSourceConfig requireTargetDataSourceConfig(String dataSourceId) {
        if (dataSourceId == null || dataSourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("targetDataSourceId cannot be blank");
        }

        TargetDataSourceConfigRegistry registry = serverInfo.getTargetDataSourceConfigRegistry();
        if (registry == null) {
            throw new IllegalStateException("targetDataSourceConfigRegistry is not initialized");
        }

        TargetDataSourceConfig targetDataSourceConfig =
                registry.findTargetDataSourceConfigByDataSourceId(dataSourceId.trim());

        if (targetDataSourceConfig == null) {
            throw new IllegalArgumentException("targetDataSourceConfig not found, dataSourceId=" + dataSourceId);
        }

        return targetDataSourceConfig;
    }

    private static final class TargetTableState {
        private final String targetTableName;
        private final boolean tableExists;

        private TargetTableState(String targetTableName, boolean tableExists) {
            this.targetTableName = targetTableName;
            this.tableExists = tableExists;
        }
    }




}
