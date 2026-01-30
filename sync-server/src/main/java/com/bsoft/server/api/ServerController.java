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
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.enums.DatabaseType;
import com.bsoft.common.enums.TableNamingStrategy;
import com.bsoft.common.factory.TableNameGenerator;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

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


    // 处理线程
    private final ExecutorService processingExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r, "table-creation-processor");
        thread.setDaemon(true);
        return thread;
    });

    @PostConstruct
    public void initProcessor() {
        // 启动处理线程
        processingExecutor.submit(this::processRequests);
    }

    @PreDestroy
    public void shutdown() {
        processingExecutor.shutdown();
        try {
            if (!processingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                processingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            processingExecutor.shutdownNow();
        }
    }

    /**
     * 处理请求队列中的建表请求
     */
    private void processRequests() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 从队列中获取请求，如果队列为空则阻塞等待
                TableDDLRequestWrapper tableDDLRequestWrapper = requestQueue.take();
                logger.info("开始处理建表请求，剩余队列大小: {}", requestQueue.size());

                // 处理建表请求
                createTableIfNecessary(tableDDLRequestWrapper);
                logger.info("建表请求处理完成");

            }  catch (Exception e) {
                e.printStackTrace();
                logger.error("处理建表请求时发生异常", e);
                // 继续处理下一个请求
            }
        }
    }

    // 初始化，创建目标库对应的 读写 对象，读对象用于检查表是否存在，写对象用于创建表
    // 每个目标库，都有自己的 读写对象
    public void init(TargetDataSourceConfigRegistry targetDataSourceConfigRegistry){
        for (TargetDataSourceConfig targetDataSourceConfig : targetDataSourceConfigRegistry.getTargetDataSourceConfigList()) {
            tableMetadataReaderWriterRegistry.register(targetDataSourceConfig);
        }

        logger.info("ServerController初始化完成...................");
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





    // 根据传入的表配置列表，创建目标库中的表（如果不存在的话），
    private void createTableIfNecessary(TableDDLRequestWrapper tableDDLRequestWrapper) throws Exception {
        // TableDDLRequestWrapper 中包含原始请求数据，包括目标库的TargetDataSourceConfig
        TargetDataSourceConfig targetDataSourceConfig = tableDDLRequestWrapper.getTableDDLRequest().getTargetDataSourceConfig();
        // 一般情况下，tableMappingConfigDTOList 只会有一个元素，表示单个表的建表请求
        TableMappingConfigDTO tableMappingConfigDTO = tableDDLRequestWrapper.getTableMappingConfigDTOList().get(0);
        // 命名策略，其实是探针端指定的命名策略
        TableNamingStrategy tableNamingStrategy = tableDDLRequestWrapper.getTableDDLRequest().getTableNamingStrategy();

        // 目标表的类型
        DatabaseType dataSourceType = targetDataSourceConfig.getDataSourceType();

        createTableIfNecessary(tableMappingConfigDTO,
                               tableNamingStrategy,
                               tableMetadataReaderWriterRegistry.getTableMetadataReaderWriterPair(targetDataSourceConfig),
                               dataSourceType,
                               dataSourceType == DatabaseType.DORIS ? false : true);

    }



    // 通用的建表方法
    private void createTableIfNecessary(
            TableMappingConfigDTO tableMappingConfigDTO,
            TableNamingStrategy tableNamingStrategy,
            TableMetadataReaderWriterPair tableMetadataReaderWriterPair,
            DatabaseType databaseType,
            boolean needsConstraints) throws Exception {

            MetadataReader reader = tableMetadataReaderWriterPair.getMetadataReader();
            MetadataWriter writer = tableMetadataReaderWriterPair.getMetadataWriter();

            // 将 DTO 转换为 TableMappingConfig 对象
            TableMappingConfig tableMappingConfig = tableMappingConfigDTO.toTableMappingConfig();

            // 初步定义表名(可能不是最终表名)：根据目标表名的生成策略，生成目标表名
            // TABLE_MERGE,CLIENTID_SCHEMA_TABLE:这两种模式，表名不会变，现在生成的表名就是最终的表名
            // TABLE_PREFIX：根据表是否存在，可能会被修改
            String targetTableName = tableNameGenerator.generateTargetTableName(tableMappingConfig, databaseType,tableNamingStrategy);

            // 1. 判断目标表名是否存在
            boolean tableExists = reader.isTableExists(targetTableName);

            // 2. 如果目标表名存在，且当前的表命名策略是 TABLE_PREFIX(前缀)，生成表名，再次检查是否存在
           if(tableExists && TableNamingStrategy.TABLE_PREFIX == tableNamingStrategy){
                targetTableName = tableNameGenerator.generateTargetTableName(
                        tableMappingConfig, databaseType, TableNamingStrategy.CLIENTID_SCHEMA_TABLE);
               // 再次判断目标表名是否存在
               tableExists = reader.isTableExists(targetTableName);
            }

            // 3. 如果目标表存在且需要扩展字段长度，执行对比更新操作
            if (tableExists) {
                if (DatabaseType.ORACLE == databaseType && serverInfo.isAutoExpandColumnLength()) {
                    // 对比并更新字段长度
                    compareAndUpdateColumnLengths(reader, writer, targetTableName, tableMappingConfigDTO.getDdl());
                }
                return;
            }


            // 1. 创建表
            // 考虑到表数据合并的场景，且合并模式取决于客户端，即使现在不合并，将来可能有合并，因此干脆不使用源表的主键，统一使用CDC_ROWID作为主键,绝对不会冲突
            createTable(writer, targetTableName, tableMappingConfigDTO.getDdl(), tableMappingConfigDTO.getTableKeys());

            // 2. 统一新增额外的字段
            addExtraColumnsToTable(writer, targetTableName);

            // 3. 根据数据库类型决定是否需要处理约束
            if (needsConstraints) {
                // 应用源表的主键等约束
                createTableIndexesAndConstraints(writer, targetTableName, tableMappingConfigDTO.getTableKeys());
                // 如果源表没有主键或唯一键，则为目标表添加一个主键字段
                ensureTableHasUniqueKey(writer, targetTableName);
            }

//            // 4. 记录建表信息
//            configFetcher.insertTableCreationRecord(tableMappingConfig, reader.getTargetDataSourceConfig(), targetTableName);

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
    private void createTable(MetadataWriter metadataWriter,String targetTableName,String ddl,TableKeys tableKeys){
        logger.info("表 {} 不存在，创建", targetTableName);
        String currentDbDDL = metadataWriter.convertOracleCreateTableToCurrentDB(targetTableName,ddl,tableKeys);
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



}
