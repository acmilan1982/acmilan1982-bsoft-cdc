package com.bsoft.common.util;

/**
 * ClassName: DatasourceConfigFetcher
 * Package:   com.bsoft.util
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/7/3 - 19:26
 */

import com.bsoft.common.bean.*;
import com.bsoft.common.dto.BusinessDataSourceConfigDTO;
import com.bsoft.common.dto.TableMappingConfigDTO;
import com.bsoft.common.enums.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class DatasourceConfigFetcher {

    private static final Logger logger = LogManager.getLogger(DatasourceConfigFetcher.class);

    // 客户端信息
    private ClientInfo clientInfo;

    // 服务端信息
    @Autowired
    private ServerInfo serverInfo;

    // 上一次运行时的执行计划
    @Autowired
    private ExecutionPlan lastExecutionPlan;



    // 业务库的配置对象
    @Autowired
    private ConfigDataSourceConfig configDataSourceConfig;


    // 配置库的DataSource
    private DataSource dataSource;


    //业务库的配置对象
    private BusinessDataSourceConfig businessDataSourceConfig;

    @PostConstruct
    public void initConnection() throws Exception {
        // 创建配置库的DataSource
        this.dataSource = configDataSourceConfig.initDataSource();
    }



    // 检查 Oracle CDC 前置条件
    public OraclePrereqChecker.Result assertOracleCdcReady(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
        try  {
            DataSource dataSource = businessDataSourceConfig.getDataSource();
            Connection c = dataSource.getConnection();
            OraclePrereqChecker.Result r = OraclePrereqChecker.checkAll(c, "CDC_USER", "USERS");

            return r;
        }finally {
            // 关闭业务库连接
            if(businessDataSourceConfig.getDataSource() != null){
                businessDataSourceConfig.getDataSource().getConnection().close();
            }

        }

    }
    // 向配置库注册客户端，如果不存在
    public void registerClientIfAbsent(ClientInfo clientInfo) throws Exception {
        this.clientInfo  = clientInfo;
        String ipDesc = String.join(", ", getLocalIPAddresses());
        String desc = clientInfo.getDescription() + " IP: " + ipDesc;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement checkStmt = conn.prepareStatement(CdcSqlConstants.FETCH_CLIENT_SQL)) {

            checkStmt.setString(1, clientInfo.getClientId());
            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    // 已存在，直接返回
                    return;
                }
            }
        }

        // 2. 不存在才插入（分开写更易扩展，便于后续扩展如更新描述等）
        try (Connection conn = dataSource.getConnection();
             PreparedStatement insertStmt = conn.prepareStatement(CdcSqlConstants.REGISTER_CLIENT_SQL)) {
            insertStmt.setString(1, clientInfo.getClientId());
            insertStmt.setString(2, desc);
            insertStmt.setString(3, "1");
            insertStmt.executeUpdate();
        }
    }



//    // 向 配置库 注册中心库
//    public void registerServerIfAbsent(ServerInfo serverInfo) throws Exception {
//        this.serverInfo = serverInfo;
//        String serverId =  serverInfo.getServerId();
//
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement checkStmt = conn.prepareStatement(CdcSqlConstants.FETCH_SERVER_SQL)) {
//            checkStmt.setString(1, serverId);
//            try (ResultSet rs = checkStmt.executeQuery()) {
//                if (rs.next() && rs.getInt(1) == 0) {
//                    String ipDesc = String.join(", ", getLocalIPAddresses());
//                    try (PreparedStatement insertStmt = conn.prepareStatement(CdcSqlConstants.REGISTER_SERVER_SQL)) {
//                        insertStmt.setString(1, serverId);
//                        insertStmt.setString(2, "服务端注册自: " + serverId + " IP: " + ipDesc);
//                        insertStmt.setString(3, "1");
//                        insertStmt.executeUpdate();
//                    }
//                }
//            }
//        }
//
//    }
//
//    // 读取业务库的配置，当前客户端可能尚未配置对应的业务库
//    // 循环调用，直到配置完整
//    public BusinessDataSourceConfig retryFetchBusinessDataSourceConfig() throws InterruptedException {
//        BusinessDataSourceConfig businessDataSourceConfig = null;
//        while (businessDataSourceConfig == null) {
//            try {
//                logger.info("尝试从配置库读取数据源配置，clientId = {}", clientInfo.getClientId());
//                businessDataSourceConfig = fetchBusinessDataSourceConfig();
//                logger.info("成功获取数据源配置: {}", businessDataSourceConfig);
//            } catch (Exception e) {
//                e.printStackTrace();
//                logger.warn("未能获取到数据源配置，10秒后重试: {}", e.getMessage());
//                Thread.sleep(10000);
//            }
//        }
//        return businessDataSourceConfig;
//    }


//    // 读取业务库配置
//    public BusinessDataSourceConfig fetchBusinessDataSourceConfig() throws Exception {
//
//        // 1.读取业务库数据源的基本配置，包括url，用户名，密码等
//        BusinessDataSourceConfig businessDataSourceBasicConfig = getBusinessDataSourceBasicConfig();
//
//        // 2.读取当前业务库的订阅关系
//        SubscriberMapping subscriberMapping = getSubscriberMapping(businessDataSourceBasicConfig);
//
//        // 3.读取当前业务库的所有目标库
//        TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry = getTargetDataSourceConfigRegistry(businessDataSourceBasicConfig);
//
//
//
//        // 2.获取业务库需要同步的表
//        Set<Table> tableSet = getTableSetBySourceId(config.getDataSourceId());
//
//        // 3.根据表名，创建对应的topic名等
//        businessDataSourceBasicConfig.init(tableSet,clientInfo.getClientId());
//
//        // 4.获取所有表的主键
//        /*
//         *   key:schema
//         *   value: key:   tableName
//         *          value: 主键列表
//         */
//        Map<String,  Map<String, List<String>>> primaryKeysForAllTables = getPrimaryKeysForAllTables(config);
//
//        // 5.表的主键设置到每一个 TableMappingConfig 中
//        config.assignPrimaryKeyToTableMappingConfig(primaryKeysForAllTables);
//
//        return config;
//    }


//    // 读取业务库数据源的基本配置，包括url，CDC_USER，schema等
//    public BusinessDataSourceConfig getBusinessDataSourceBasicConfig() throws Exception {
//        this.businessDataSourceConfig = getDataSourceBasicConfig();
//        if (businessDataSourceConfig == null) {
//            logger.error("当前客户端 client_id = {} 正在等待分配数据源，", clientInfo.getClientId());
//            throw new RuntimeException("当前客户端 client_id = " +  clientInfo.getClientId() + " 正在等待分配数据源");
//        }
//
//        return businessDataSourceConfig;
//    }

    //  读取 业务库数据源 中的基本配置，比如url,用户名,密码
    public BusinessDataSourceConfig getBusinessDataSourceBasicConfig() throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SOURCE_SQL)) {
            ps.setString(1, clientInfo.getClientId());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    this.businessDataSourceConfig = buildBusinessDataSourceConfig(rs);
                }
            }
        }
        return this.businessDataSourceConfig;
    }


    // CDC_DATA_SOURCE中的单条记录封装成一个 BusinessDataSourceConfig
    private BusinessDataSourceConfig buildBusinessDataSourceConfig(ResultSet rs) throws Exception {
        return BusinessDataSourceConfig.builder()
                .dataSourceId(rs.getString("DATA_SOURCE_ID"))
                .dataSourceHost(rs.getString("DATA_SOURCE_HOST"))
                .dataSourcePort(rs.getInt("DATA_SOURCE_PORT"))
                .dataSourceUserName(rs.getString("DATA_SOURCE_USER_NAME"))
                .dataSourcePassword(rs.getString("DATA_SOURCE_PASSWORD"))
                .dataSourceType(DatabaseType.fromCode(rs.getString("DATA_SOURCE_TYPE")))
                .dataSourceServiceName(rs.getString("DATA_SOURCE_SERVICE_NAME"))
                .fgActive(rs.getString("FG_ACTIVE"))
                .clientId(rs.getString("CLIENT_ID"))
                .build();
    }


    // 读取 当前服务端 对应的 目标库数据源 配置
    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
        Set<String> targetDataSourceIdSet = new HashSet<>();

        // 先拿到所有 argetDataSourceId
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_TARGET_DATA_SOURCE_ID_BY_BUSINESS_DATA_SOURCE_ID_SQL)) {
            ps.setString(1, "%" + businessDataSourceConfig.getDataSourceId()+ "%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // 所有的targetDataSourceId都写在了一行，用 , 分割，所以这里解析一下
                    String[] targetDataSourceIdArray = rs.getString("DATA_TO_SOURCE_ID").split(",");
                    Arrays.stream(targetDataSourceIdArray).forEach(targetDataSourceId -> targetDataSourceIdSet.add(targetDataSourceId));
                }
            }
        }

        if (targetDataSourceIdSet == null || targetDataSourceIdSet.isEmpty()) {
            logger.warn("No target data source id found for business data source: {}", businessDataSourceConfig.getDataSourceId());
            return null; // 返回一个空的 TargetDataSourceConfigRegistry
        }

        return getTargetDataSourceConfig(targetDataSourceIdSet);
    }

    // 读取当前业务库的订阅关系(专供客户端使用)
    public SubscriberMappingRegistry getSubscriberMapping(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
        // 为了安全起见，把所有 当前业务库 的订阅关系都读出来，因为搞不好一个业务库会有多个订阅关系
        // CDC_DATA_SUBSCRIBE表中的单条记录 构建一个 SubscriberMapping 对象
        List<SubscriberMapping> tempSubscriberMappingList = getSubscriberMappingList(businessDataSourceConfig);

        for (SubscriberMapping subscriberMapping : tempSubscriberMappingList) {

            // 移除非本业务库的 FROM_SOURCE_ID
            subscriberMapping.getTempFromDataSourceIdList()
                    .removeIf(fromDataSourceId -> !fromDataSourceId.equals(businessDataSourceConfig.getDataSourceId()));

            // 移除非本业务库的表
            subscriberMapping.getSourceTableNameList()
                    .removeIf(table -> !table.getDataSourceId().equals(businessDataSourceConfig.getDataSourceId()));
        }


        if (tempSubscriberMappingList.isEmpty()) {
            logger.error("当前业务库 {} 没有订阅关系，请检查配置", businessDataSourceConfig.getDataSourceId());
            return null;
        }

        // 当前业务库的订阅关系，都集中到 SubscriberMappingRegistry的tempSubscriberMappingList，稍后会详细解析成一对一的关系
        SubscriberMappingRegistry subscriberMappingRegistry = new SubscriberMappingRegistry();

        subscriberMappingRegistry.setTempSubscriberMappingList(tempSubscriberMappingList);



        // 这里处理一对一，多对一，一对多，多对多的所有关系，都转换成一对多的关系
        subscriberMappingRegistry.afterPropertiesSet();
        return subscriberMappingRegistry;
    }

    // 读取当前业务库的订阅关系列表
    public List<SubscriberMapping> getSubscriberMappingList(BusinessDataSourceConfig businessDataSourceConfig) throws Exception {
        List<SubscriberMapping> subscriberMappingList = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SUBSCRIBE_BY_BUSINESS_DATA_SOURCE_ID_SQL)) {

            ps.setString(1, "%" + businessDataSourceConfig.getDataSourceId() + "%");
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    subscriberMappingList.add(buildSubscriberMapping(rs));
                }
            }
        }
        return subscriberMappingList;
    }




    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------

    // 读取 目标库数据源 配置,目前一个中心端，向所有目标库写入
    public TargetDataSourceConfigRegistry generateTargetDataSourceConfigRegistry() throws Exception {
        // 读取 当前服务端 对应的 目标库数据源 配置
        // 创建TargetDataSourceConfig，直接注册到TargetDataSourceConfigRegistry中
        TargetDataSourceConfigRegistry targetDataSourceConfigRegistry = getTargetDataSourceConfig();
        // 向serverInfo设置 目标库数据源
        this.serverInfo.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
        // 向serverInfo设置 配置库数据源
        this.serverInfo.setConfigDataSourceConfig(configDataSourceConfig);

        return targetDataSourceConfigRegistry;
    }


    // 读取 当前服务端 对应的 目标库数据源 配置
    // 创建TargetDataSourceConfig，直接注册到TargetDataSourceConfigRegistry中
    private TargetDataSourceConfigRegistry getTargetDataSourceConfig() throws Exception {
        Set<String> targetDataSourceIdSet = new HashSet<>();

        // 先拿到所有 TargetDataSourceId
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_TARGET_DATA_SOURCE_ID_SQL)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // 所有的targetDataSourceId都写在了一行，用 , 分割，所以这里解析一下
                    String[] targetDataSourceIdArray = rs.getString("DATA_TO_SOURCE_ID").split(",");
                    Arrays.stream(targetDataSourceIdArray).forEach(targetDataSourceId -> targetDataSourceIdSet.add(targetDataSourceId));
                }
            }
        }

        return getTargetDataSourceConfig(targetDataSourceIdSet);
    }

    // 读取 当前服务端 对应的 目标库数据源 配置，用DATA_TO_SOURCE_ID作为查询条件
    private TargetDataSourceConfigRegistry getTargetDataSourceConfig(Set<String> targetDataSourceIdSet) throws Exception {
        TargetDataSourceConfigRegistry targetDataSourceConfigRegistry = new TargetDataSourceConfigRegistry();


        // 构建 IN 子句的占位符，如 (?,?,?)
        String placeholders = targetDataSourceIdSet.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));


        // 替换 SQL 中的占位符 {}
        String sql = CdcSqlConstants.FETCH_TARGET_DATA_SOURCE_SQL.replace("{}", placeholders);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 设置参数值
            int index = 1;
            for (String id : targetDataSourceIdSet) {
                ps.setString(index++, id);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // 创建TargetDataSourceConfig，直接注册到TargetDataSourceConfigRegistry中
                    TargetDataSourceConfig targetDataSourceConfig = buildTargetDataSourceConfig(rs);
                    if(this.clientInfo == null){
                        targetDataSourceConfig.setDatabaseInfo(buildDatabaseInfo(targetDataSourceConfig));
                    }
                    targetDataSourceConfigRegistry.register(targetDataSourceConfig );
                }

            }
        }

        return targetDataSourceConfigRegistry;
    }

    // CDC_DATA_SOURCE中的单条记录封装成一个 BusinessDataSourceConfig
    private TargetDataSourceConfig buildTargetDataSourceConfig(ResultSet rs) throws Exception {
        return TargetDataSourceConfig.builder()
                .dataSourceId(rs.getString("DATA_SOURCE_ID"))
                .dataSourceHost(rs.getString("DATA_SOURCE_HOST"))
                .dataSourcePort(rs.getInt("DATA_SOURCE_PORT"))
                .dataSourceUserName(rs.getString("DATA_SOURCE_USER_NAME"))
                .dataSourcePassword(rs.getString("DATA_SOURCE_PASSWORD"))
                .dataSourceType(DatabaseType.fromCode(rs.getString("DATA_SOURCE_TYPE").toUpperCase()))
                .dataSourceServiceName(rs.getString("DATA_SOURCE_SERVICE_NAME"))
                .fgActive(rs.getString("FG_ACTIVE"))
                .build();
    }


    public DatabaseInfo buildDatabaseInfo(TargetDataSourceConfig targetDataSourceConfig) {
        // DatabaseInfo
        DatabaseInfo dbInfo = null;
        // 数据库版本查询语句
        String dbVersionQuerySql;
        DataSource targetDataSource = targetDataSourceConfig.initTargetDataSource();
        // 根据数据库类型选择对应的查询语句
        switch (targetDataSourceConfig.getDataSourceType()) {
            case ORACLE:
                dbVersionQuerySql = CdcSqlConstants.ORACLE_VERSION_QUERY;
                break;
            case DORIS:
                dbVersionQuerySql = CdcSqlConstants.DORIS_VERSION_QUERY;
                break;
            case MYSQL:
                dbVersionQuerySql = CdcSqlConstants.MYSQL_VERSION_QUERY;
                break;
            default:
                throw new RuntimeException("不支持的数据库类型: " + targetDataSourceConfig.getDataSourceType());
        }

        try (Connection conn = targetDataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(dbVersionQuerySql)) {

            if (rs.next()) {
                String fullVersion = rs.getString(1);
                logger.info("{} 完整版本: {}", targetDataSourceConfig.getDataSourceType(), fullVersion);


                switch (targetDataSourceConfig.getDataSourceType()) {
                    case ORACLE:
                        dbInfo = parseOracleVersion(fullVersion);
                        break;
                    case DORIS:
                        dbInfo = parseDorisVersion(fullVersion);
                        break;
                    case MYSQL:

                        break;
                    default:
                        throw new RuntimeException("不支持的数据库类型: " + targetDataSourceConfig.getDataSourceType());
                }



                return dbInfo;
            }

            throw new RuntimeException("无法获取数据库版本信息");

        } catch (SQLException e) {
            throw new RuntimeException("查询数据库版本失败: " + e.getMessage(), e);
        }
    }

    /**
     * 解析 Oracle 版本信息
     * 例如: "Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production" -> "19.0"
     *       "Oracle Database 11g Enterprise Edition Release 11.2.0.4.0" -> "11.2"
     *
     * @param fullVersion Oracle 完整版本字符串
     * @return DatabaseInfo 包含版本信息的对象
     */
    private DatabaseInfo parseOracleVersion(String fullVersion) {
        DatabaseInfo dbInfo = new DatabaseInfo(DatabaseType.ORACLE, fullVersion);

        try {
            // 提取 Release 后的版本号部分
            String versionPart = fullVersion.toLowerCase();

            // 查找 "release " 后的版本号
            int startIndex = versionPart.indexOf("release ");
            if (startIndex == -1) {
                logger.warn("无法识别的 Oracle 版本格式: {}", fullVersion);
                return dbInfo;
            }

            // 跳过 "release " 前缀
            versionPart = versionPart.substring(startIndex + 8).trim();

            // 去除后缀 (如 " - Production")
            int suffixIndex = versionPart.indexOf(" -");
            if (suffixIndex != -1) {
                versionPart = versionPart.substring(0, suffixIndex);
            }

            dbInfo.setVersion(versionPart);

            // 分割主版本号和次版本号 (如 19.0.0.0.0 -> 主版本19, 次版本0)
            String[] parts = versionPart.split("\\.");
            if (parts.length >= 2) {
                dbInfo.setMajorVersion(Integer.parseInt(parts[0]));
                dbInfo.setMinorVersion(Integer.parseInt(parts[1]));
            }

            logger.info("Oracle 版本解析: 完整={}, 版本={}, 主版本={}, 次版本={}",
                    fullVersion, versionPart, dbInfo.getMajorVersion(), dbInfo.getMinorVersion());

        } catch (Exception e) {
            logger.error("解析 Oracle 版本失败: {}", fullVersion, e);
        }

        return dbInfo;
    }

    /**
     * 解析 Doris 版本信息
     * 例如: "doris version doris-4.0.0-rc02-522414ca9bb" -> "4.0"
     *       "doris version doris-3.2.1-522414ca9bb" -> "3.2"
     *
     * @param fullVersion Doris 完整版本字符串
     * @return DatabaseInfo 包含版本信息的对象
     */
    private DatabaseInfo parseDorisVersion(String fullVersion) {
        DatabaseInfo dbInfo = new DatabaseInfo(DatabaseType.DORIS, fullVersion);

        try {
            // 提取版本号部分,格式如: doris-3.2.0-rc01
            String versionPart = fullVersion.toLowerCase();

            // 查找 "doris-" 后的版本号
            int startIndex = versionPart.indexOf("doris-");
            if (startIndex == -1) {
                logger.warn("无法识别的 Doris 版本格式: {}", fullVersion);
                return dbInfo;
            }

            // 跳过 "doris-" 前缀
            versionPart = versionPart.substring(startIndex + 6);

            // 去除后缀 (如 -rc01, -beta 等)
            int suffixIndex = versionPart.indexOf('-');
            if (suffixIndex != -1) {
                versionPart = versionPart.substring(0, suffixIndex);
            }

            dbInfo.setVersion(versionPart);

            // 分割主版本号和次版本号
            String[] parts = versionPart.split("\\.");
            if (parts.length >= 2) {
                dbInfo.setMajorVersion(Integer.parseInt(parts[0]));
                dbInfo.setMinorVersion(Integer.parseInt(parts[1]));
            }

            logger.info("Doris 版本解析: 完整={}, 版本={}, 主版本={}, 次版本={}",
                    fullVersion, versionPart, dbInfo.getMajorVersion(), dbInfo.getMinorVersion());

        } catch (Exception e) {
            logger.error("解析 Doris 版本失败: {}", fullVersion, e);
        }

        return dbInfo;
    }








    // 读取所有有效的 订阅关系-仅服务端使用
    public SubscriberMappingRegistry generateSubscriberMappingRegistry() throws Exception {
        // 所有的订阅关系，都集中到 SubscriberMappingRegistry
        SubscriberMappingRegistry subscriberMappingRegistry = new SubscriberMappingRegistry();
        subscriberMappingRegistry.setServerInfo(this.serverInfo);

        // 从CDC_DATA_SUBSCRIBE中读取fg_active = '1'的所有订阅关系，
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SUBSCRIBE_SQL)) {

            // 向 SubscriberMappingRegistry 注册所有订阅关系，
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // CDC_DATA_SUBSCRIBE中的单条记录，对应一个 SubscriberMapping 对象，一个列中的多个值，已经被解析
                    subscriberMappingRegistry.register(buildSubscriberMapping(rs));
                }
            }
        }

        // 注册所有的 SimpleClientInfo 列表
        subscriberMappingRegistry.register(getSimpleClientInfoList());

        // 这里处理一对一，多对一，一对多，多对多的所有关系，都转换成一对一的关系
        subscriberMappingRegistry.afterPropertiesSet();
        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);


        return subscriberMappingRegistry;
    }

    // CDC_DATA_SUBSCRIBE表中的单条记录 构建一个 SubscriberMapping 对象
    private SubscriberMapping buildSubscriberMapping(ResultSet rs) throws SQLException {
        SubscriberMapping subscriberMapping = new SubscriberMapping();
        subscriberMapping.addTempFromDataSourceId(rs.getString("DATA_FROM_SOURCE_ID"));
        subscriberMapping.addTempToDataSourceId(rs.getString("DATA_TO_SOURCE_ID"));
        subscriberMapping.addTempSourceTableName(rs.getString("DATA_SOURCE_TABLE"));
        return subscriberMapping;
    }



    // 读取所有有效的 订阅关系-仅服务端使用
    public List<SimpleClientInfo> getSimpleClientInfoList() throws Exception {
        // 所有的订阅关系，都集中到 SubscriberMappingRegistry
        List<SimpleClientInfo> simpleClientInfoList = new ArrayList<>();


        // 从 CDC_CLIENT 中读取fg_active = '1'
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_SIMPLE_CLIENT_SQL)) {


            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    SimpleClientInfo simpleClientInfo = new SimpleClientInfo();
                    simpleClientInfo.setClientId(rs.getString("CLIENT_ID"));
                    simpleClientInfo.setDataSourceId(rs.getString("DATA_SOURCE_ID"));
                    simpleClientInfo.setTableNamingStrategy(TableNamingStrategy.fromCode(rs.getString("TABLENAMINGSTRATEGY")));
                    simpleClientInfoList.add(simpleClientInfo);
                }
            }
        }

        return simpleClientInfoList;
    }


    // 中心端读取所有业务库配置，目前只有一个中心端
    public BusinessDataSourceConfigRegistry generateBusinessDataSourceConfigRegistry() throws Exception {

        // 2.读取所有 业务库数据源 的基本配置，包括url，用户名等，读取已经与客户端绑定的源
        BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry = getAllBasicBusinessDataSourceConfig();
        businessDataSourceConfigRegistry.afterPropertiesSet();


        return businessDataSourceConfigRegistry;
    }



    //  读取所有源-数据源中(CDC_DATA_SOURCE表中data_source_category = 'source')的基本配置，比如用户名密码，每个源封装成一个BusinessDataSourceConfig
    public BusinessDataSourceConfigRegistry getAllBasicBusinessDataSourceConfig() throws Exception {
        BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry = new BusinessDataSourceConfigRegistry();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_ALL_SOURCE_DATA_SOURCE_SQL)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                businessDataSourceConfigRegistry.register(buildBusinessDataSourceConfig(rs));
            }

        }
        return businessDataSourceConfigRegistry;
    }



    // 服务端：根据订阅关系，批量生成表的执行计划
    public ExecutionPlan batchGenerateTableExecutionPlan(SubscriberMappingRegistry subscriberMappingRegistry){
        // 根据订阅关系，生成表级执行计划
        for (SubscriberMapping subscriberMapping : subscriberMappingRegistry.getSubscriberMappingList()) {
            batchGenerateTableExecutionPlan(subscriberMapping, TableStatus.RECEIVE);
        }

        // 应用执行计划的变更，
        // 如果是服务端的执行计划，此时，deletedTableExecutionPlan和tableExecutionPlans都是null
        // 基本上就是把addedTableExecutionPlan都加到tableExecutionPlans
        lastExecutionPlan.applyChanges();
        return lastExecutionPlan;
    }

//
//
//    // 读取所有有效的 表的汇总关系
//    public MergeTableRegistry generateMergeTableRegistry() throws Exception {
//        // 所有的订阅关系，都集中到 SubscriberMappingRegistry
//        MergeTableRegistry mergeTableRegistry = new MergeTableRegistry();
//
//        // 从CDC_DATA_SUBSCRIBE中读取fg_active = '1'的所有订阅关系，
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_ALL_MERGE_TABLE_SQL)) {
//
//            // 向 SubscriberMappingRegistry 注册所有订阅关系，
//            try (ResultSet rs = ps.executeQuery()) {
//                while (rs.next()) {
//                    // CDC_DATA_SUBSCRIBE中的单条记录，对应一个 SubscriberMapping 对象，一个列中的多个值，已经被解析
//                    subscriberMappingRegistry.register(buildSubscriberMapping(rs));
//                }
//            }
//        }
//
//        // 这里处理一对一，多对一，一对多，多对多的所有关系，都转换成一对一的关系
//        subscriberMappingRegistry.afterPropertiesSet();
//        serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);
//
//
//        return subscriberMappingRegistry;
//    }
//
//    // CDC_DATA_SUBSCRIBE表中的单条记录 构建一个 SubscriberMapping 对象
//    private MergeTable buildMergeTable(ResultSet rs) throws SQLException {
//        SubscriberMapping subscriberMapping = new SubscriberMapping();
//        subscriberMapping.addTempFromDataSourceId(rs.getString("DATA_FROM_SOURCE_ID"));
//        subscriberMapping.addTempToDataSourceId(rs.getString("DATA_TO_SOURCE_ID"));
//        subscriberMapping.addSourceTableName(rs.getString("DATA_SOURCE_TABLE"));
//        return subscriberMapping;
//    }
















//
//
//
//
//
//
//    //  获取一批表的 主键(P) 与 唯一约束(U)；可选合入“未绑定约束的唯一索引(UNIQUE INDEX)
//    public static Map<String, TableKeys> getPrimaryKeysForAllTables(BusinessDataSourceConfig businessDataSourceConfig) throws SQLException {
//
//        // 当前方法的返回值： key: 全限定类名 value:TableKeys,持有该表 约束/索引 信息
//        Map<String, TableKeys> tableKeysMapByFullyQualifiedTableName = new HashMap<>();
//
//        // 创建到业务库的 Connection
//        Connection businessDataSourceConnection = DriverManager.getConnection(businessDataSourceConfig.getUrl(),
//                                                  businessDataSourceConfig.getDataSourceUserName(),
//                                                  businessDataSourceConfig.getDataSourcePassword());
//
//
//        // 所有需要同步的表 ，以schema为key分组
//        Map<String, List<TableMappingConfig>> tableMappingConfigBySchema = businessDataSourceConfig.getTableMappingConfigBySchema();
//
//        for (String schema : tableMappingConfigBySchema.keySet()) {
//            List<TableMappingConfig> tableMappingConfigList = tableMappingConfigBySchema.get(schema);
//            List<String> tableNames = tableMappingConfigList.stream().map(TableMappingConfig::getTableName).collect(Collectors.toList());
//
//            // key : OWNER.TABLE   value: 单个表的所有约束/索引
//            Map<String, List<KeyDef>> tableToKeys = new LinkedHashMap<>();
//
//            // 1. 查找约束
//            String consSql = buildConstraintsSql(tableNames.size());
//            try (PreparedStatement ps = businessDataSourceConnection.prepareStatement(consSql)) {
//                int idx = 1;
//                // 参数绑定
//                ps.setString(idx++, schema);
//                for (String t : tableNames) ps.setString(idx++, t);
//
//                try (ResultSet rs = ps.executeQuery()) {
//                    // key = owner.table.constraint_name，暂存rs中的一行
//                    Map<String, KeyDefBuilder> builderMap = new LinkedHashMap<>();
//
//                    // 一个约束可能包含多个列，每个列在独立的一行中
//                    while (rs.next()) {
//                        String owner  = rs.getString("OWNER");
//                        String tableName  = rs.getString("TABLE_NAME");
//                        String constraintName = rs.getString("CONSTRAINT_NAME");
//                        String constraintType = rs.getString("CONSTRAINT_TYPE"); // 'P' or 'U'
//                        String columnName= rs.getString("COLUMN_NAME");
//
//                        // 约束类型，P=主键，U=唯一约束
//                        KeyType kt = "P".equals(constraintType) ? KeyType.PRIMARY_KEY : KeyType.UNIQUE_CONSTRAINT;
//                        String key = owner + "." + tableName + "." + constraintName;
//
//                        // 保存当前行，同一个约束的多行会被合并
//                        builderMap.computeIfAbsent(key, k -> new KeyDefBuilder(constraintName, kt, owner, tableName))
//                                .addColumn(columnName);
//                    }
//
//                    // 产出 KeyDef,用KeyDefBuilder.build()方法,按表归类
//                    for (KeyDefBuilder b : builderMap.values()) {
//                        String key = b.getOwner() + "." + b.getTable();
//                        tableToKeys.computeIfAbsent(key, k -> new ArrayList<>()).add(b.build());
//                    }
//                }
//            }
//
//            // 2 （可选）仅唯一索引（排除与 P/U 约束绑定的索引）
//            String idxSql = buildUniqueIndexSql(tableNames.size());
//            try (PreparedStatement ps = businessDataSourceConnection.prepareStatement(idxSql)) {
//                // 绑定参数
//                int idx = 1;
//                ps.setString(idx++, schema);
//                for (String t : tableNames) ps.setString(idx++, t);
//                ps.setString(idx++, schema); // 用于 NOT EXISTS 子句
//
//                try (ResultSet rs = ps.executeQuery()) {
//                    Map<String, KeyDefBuilder> builderMap = new LinkedHashMap<>();
//
//                    while (rs.next()) {
//                        String owner   = rs.getString("OWNER");
//                        String tableName   = rs.getString("TABLE_NAME");
//                        String indexName  = rs.getString("INDEX_NAME");
//                        String columnName = rs.getString("COLUMN_NAME");
//
//                        String key = owner + "." + tableName + "." + indexName;
//                        builderMap
//                                .computeIfAbsent(key, k -> new KeyDefBuilder(indexName, KeyType.UNIQUE_INDEX, owner, tableName))
//                                .addColumn(columnName);
//                    }
//
//                    for (KeyDefBuilder b : builderMap.values()) {
//                        String key = b.getOwner() + "." + b.getTable();
//                        tableToKeys.computeIfAbsent(key, k -> new ArrayList<>()).add(b.build());
//                    }
//                }
//            }
//
//
//            // 3) 产出 TableKeys（同表内先 PK、再 UNIQUE、再 UNIQUE_INDEX；同类按名字）
//            for (String tableName : tableNames) {
//
//                String key = schema + "." + tableName;
//                // 当前表的所有约束/索引
//                List<KeyDef> keyDefList = tableToKeys.getOrDefault(key, Collections.emptyList());
//
//                keyDefList.sort((a, b) -> {
//                    int ta = typeOrder(a.getType()), tb = typeOrder(b.getType());
//                    if (ta != tb) return Integer.compare(ta, tb);
//                    return a.getName().compareTo(b.getName());
//                });
//
//                tableKeysMapByFullyQualifiedTableName.put(key, new TableKeys(schema, tableName, keyDefList));
//
//            }
//
//        }  // 遍历一个schema结束
//
//        return tableKeysMapByFullyQualifiedTableName;
//
//    }
//
//    /* ==========================  SQL 构造  ========================== */
//
//    private static String buildConstraintsSql(int tableCount) {
//        String in = repeatBind(tableCount);
//        return ""
//                + "SELECT c.OWNER, col.TABLE_NAME, c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, col.COLUMN_NAME, col.POSITION\n"
//                + "FROM   ALL_CONSTRAINTS c\n"
//                + "JOIN   ALL_CONS_COLUMNS col\n"
//                + "  ON   c.OWNER = col.OWNER AND c.CONSTRAINT_NAME = col.CONSTRAINT_NAME\n"
//                + "WHERE  c.CONSTRAINT_TYPE IN ('P','U')\n"
//                + "  AND  c.OWNER = ?\n"
//                + "  AND  col.TABLE_NAME IN (" + in + ")\n"
//                + "ORDER BY c.OWNER, col.TABLE_NAME, c.CONSTRAINT_NAME, col.POSITION";
//    }
//
//    private static String buildUniqueIndexSql(int tableCount) {
//        String in = repeatBind(tableCount);
//        return ""
//                + "SELECT i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_NAME, ic.COLUMN_POSITION\n"
//                + "FROM   ALL_INDEXES i\n"
//                + "JOIN   ALL_IND_COLUMNS ic\n"
//                + "  ON   i.OWNER = ic.INDEX_OWNER AND i.INDEX_NAME = ic.INDEX_NAME\n"
//                + "WHERE  i.OWNER = ?\n"
//                + "  AND  i.TABLE_NAME IN (" + in + ")\n"
//                + "  AND  i.UNIQUENESS = 'UNIQUE'\n"
//                + "  AND  NOT EXISTS (\n"
//                + "        SELECT 1 FROM ALL_CONSTRAINTS c\n"
//                + "        WHERE c.OWNER = ?\n"
//                + "          AND c.CONSTRAINT_TYPE IN ('P','U')\n"
//                + "          AND c.INDEX_NAME = i.INDEX_NAME\n"
//                + "      )\n"
//                + "ORDER BY i.OWNER, i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_POSITION";
//    }
//
//    /* ==========================  工具方法  ========================== */
//
//    private static String repeatBind(int n) {
//        if (n <= 0) return "?";
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < n; i++) {
//            if (i > 0) sb.append(", ");
//            sb.append("?");
//        }
//        return sb.toString();
//    }
//
//    private static int typeOrder(KeyType t) {
//        switch (t) {
//            case PRIMARY_KEY: return 1;
//            case UNIQUE_CONSTRAINT: return 2;
//            case UNIQUE_INDEX: return 3;
//            default: return 99;
//        }
//    }




    // 决定当前作业的启动模式
    public ExecutionPlan determineJobStartupMode() throws Exception {
        // job的默认启动模式
        JobStartupMode jobStartupMode = JobStartupMode.NORMAL;
        // 获取上一次执行计划
        getLastExecutionPlan();


        // 当前数据源第一次启动，没有任何执行计划
        if(lastExecutionPlan.hasNoPlanItems()){
            // 为所有的表生成执行计划，job的为启动模式：NORMAL
            batchGenerateTableExecutionPlan(TableStatus.NORMAL);
        }else{  // 当前数据源不是第一次启动了，那得检查一下执行计划了

            // 处理 删除表 级执行计划
            handleRemovedTableExecutionPlan();

            // 处理 新增表 级执行计划d
            handleNewTableExecutionPlan();
        }

        // 执行计划持久化
        batchSaveExecutionPlan();
        batchDeleteExecutionPlans();

        // 应用执行计划的变更，并判断是否需要执行快照
        lastExecutionPlan.applyChanges();

        // 如果表的状态是SNAPSHOT，一般是作业的SNAPSHOT模式被中断了，需要继续执行快照
        if(lastExecutionPlan.hasTablesInSnapshotState()){
            jobStartupMode = JobStartupMode.SNAPSHOT;
        }
        // 设置当前作业的启动模式
        this.clientInfo.setJobStartupMode(jobStartupMode);

        return this.lastExecutionPlan;
    }

    // 批量生成表的执行计划，根据订阅关系
    public void batchGenerateTableExecutionPlan(TableStatus tableStatus){
        // 订阅关系集合
        SubscriberMappingRegistry subscriberMappingRegistry = businessDataSourceConfig.getSubscriberMappingRegistry();
        // 根据订阅关系，生成表级执行计划
        for (SubscriberMapping subscriberMapping : subscriberMappingRegistry.getSubscriberMappingList()) {
            batchGenerateTableExecutionPlan(subscriberMapping, tableStatus);

        }

    }

    // 根据单个订阅关系，批量生成表的执行计划
    public void batchGenerateTableExecutionPlan(SubscriberMapping subscriberMapping,TableStatus tableStatus){
        // 根据订阅关系中的Table生成订阅关系
        for (Table table : subscriberMapping.getSourceTableNameList()) {
            lastExecutionPlan.getAddedTableExecutionPlan().add(generateTableExecutionPlan(table,
                                                                                          subscriberMapping,
                                                                                          tableStatus));
        }

    }

    // 处理需要删除的 表级执行计划
    public void handleRemovedTableExecutionPlan(){
        // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName
        List<ExecutionPlan.TableExecutionPlan> lastTableExecutionPlans = lastExecutionPlan.getTableExecutionPlans();
        // 遍历上一次的执行计划，判断是否有被删除的表级执行计划
        for (ExecutionPlan.TableExecutionPlan lastTableExecutionPlan : lastTableExecutionPlans) {
            String tableExecutionPlanKey = lastTableExecutionPlan.getKey();
            // 如果已经不存在于当前订阅关系中，则认为是被删除的表级执行计划，该主题不会再新加数据
            if(!businessDataSourceConfig.getSubscriberMappingRegistry().getMappingSetByToDataSourceIdSchemaNameTableName().contains(tableExecutionPlanKey)){
                lastExecutionPlan.getDeletedTableExecutionPlan().add(lastTableExecutionPlan);
            }
        }

    }

    // 处理新增的 表级执行计划
    public void handleNewTableExecutionPlan(){
        // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName
        // 新的执行计划，向里面添加，移除旧的执行计划
        Map<String, ExecutionPlan.TableExecutionPlan> newTableExecutionPlanMap = lastExecutionPlan.toMap();
        // 从订阅关系中生成所有的表级执行计划，判断是否有新增的表级执行计划
        for (SubscriberMapping subscriberMapping : businessDataSourceConfig.getSubscriberMappingRegistry().getSubscriberMappingList()) {
            handleNewTableExecutionPlan(subscriberMapping, newTableExecutionPlanMap);

        }
    }

    // 处理单个订阅中，新增的 表级 执行计划
    public void handleNewTableExecutionPlan(SubscriberMapping subscriberMapping,
                                            Map<String, ExecutionPlan.TableExecutionPlan> newTableExecutionPlanMap){ // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName

        String toDataSourceId = subscriberMapping.getToDataSourceId();

        for (Table table : subscriberMapping.getSourceTableNameList()) {
            String executionPlanKey = toDataSourceId + "." + table.getSchemaName() + "." + table.getTableName();
            // 是否已经存在执行计划
            ExecutionPlan.TableExecutionPlan tableExecutionPlan = newTableExecutionPlanMap.get(executionPlanKey);
            // tableExecutionPlan为null，通常表示该表是新增的，需要执行快照
            if(tableExecutionPlan == null){
                // 新增的执行计划，需要被持久化
                lastExecutionPlan.getAddedTableExecutionPlan().add(generateTableExecutionPlan(table,
                                                                                              subscriberMapping,
                                                                                              TableStatus.SNAPSHOT));

            }

        }

    }





    // 生成单表的执行计划
    public ExecutionPlan.TableExecutionPlan generateTableExecutionPlan(Table table,
                                                                       SubscriberMapping subscriberMapping,
                                                                       TableStatus tableStatus){
        return new ExecutionPlan.TableExecutionPlan(table.getDataSourceId(),
                                                    subscriberMapping.getToDataSourceId(),
                                                    table.getSchemaName(),
                                                    table.getTableName(),
                                                    tableStatus,
                                                    subscriberMapping.getClientId(),
                                                    subscriberMapping.getTableNamingStrategy());

    }



    // 获取当前 数据源上一次的 执行计划
    public ExecutionPlan getLastExecutionPlan() throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_EXECUTION_PLAN)) {
            ps.setString(1, businessDataSourceConfig.getDataSourceId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    this.lastExecutionPlan.addPlanItem(rs.getString("FROM_DATA_SOURCE_ID"),
                                                       rs.getString("TO_DATA_SOURCE_ID"),
                                                       rs.getString("SCHEMA_NAME"),
                                                       rs.getString("TABLE_NAME"),
                                                       rs.getString("TABLE_STATUS"),
                                                       rs.getString("CLIENT_ID"),
                                                       TableNamingStrategy.fromCode(rs.getString("TABLENAMINGSTRATEGY"))
                                                       );
                }
            }
        }
        return this.lastExecutionPlan;
    }



    // 批量保存 数据源的 执行计划
    public void batchSaveExecutionPlan() throws Exception {
        List<ExecutionPlan.TableExecutionPlan> addedTableExecutionPlan = lastExecutionPlan.getAddedTableExecutionPlan();

        // 如果没有新增的表级执行计划，则不需要保存
        if(addedTableExecutionPlan.isEmpty()){
            return;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.INSERT_EXECUTION_PLAN)) {
            for (ExecutionPlan.TableExecutionPlan item : addedTableExecutionPlan) {
                ps.setString(1, item.getFromDataSourceId());
                ps.setString(2, item.getToDataSourceId());
                ps.setString(3, item.getSchemaName());
                ps.setString(4, item.getTableName());
                ps.setString(5, item.getTableStatus().getCode());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }




    // 批量删除 数据源的 执行计划
    public void batchDeleteExecutionPlans() throws Exception {
        List<ExecutionPlan.TableExecutionPlan> deletedTableExecutionPlan = lastExecutionPlan.getDeletedTableExecutionPlan();

        // 如果没有删除的表级执行计划，则不需要删除
        if(deletedTableExecutionPlan.isEmpty()){
            return;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.DELETE_EXECUTION_PLAN)) {
            for (ExecutionPlan.TableExecutionPlan plan : deletedTableExecutionPlan) {
                ps.setString(1, plan.getFromDataSourceId());
                ps.setString(2, plan.getToDataSourceId());
                ps.setString(3, plan.getSchemaName());
                ps.setString(4, plan.getTableName());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    // 批量更新数据源的执行计划为普通模式
    public void batchUpdateExecutionPlans(List<ExecutionPlan.TableExecutionPlan> planList) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.UPDATE_EXECUTION_PLAN)) {
            for (ExecutionPlan.TableExecutionPlan plan : planList) {
                ps.setString(1, TableStatus.NORMAL.getCode());
                ps.setString(2, plan.getFromDataSourceId());
                ps.setString(3, plan.getToDataSourceId());
                ps.setString(4, plan.getSchemaName());
                ps.setString(5, plan.getTableName());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }


//    // 批量更新 数据源的 执行计划
//    public void batchUpdateExecutionPlans(List<ExecutionPlan.TableExecutionPlan> planList,TableStatus tableStatus) throws Exception {
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.UPDATE_EXECUTION_PLAN)) {
//            for (ExecutionPlan.TableExecutionPlan plan : planList) {
//                ps.setString(1, tableStatus.getCode());
//                ps.setString(2, plan.getDataSourceId());
//                ps.setString(3, plan.getSchemaName());
//                ps.setString(4, plan.getTableName());
//                ps.addBatch();
//            }
//            ps.executeBatch();
//        }
//    }

    private Set<String> getLocalIPAddresses() throws Exception {
        Set<String> ipSet = new HashSet<>();
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface ni = interfaces.nextElement();
            Enumeration<InetAddress> addresses = ni.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                if (!addr.isLoopbackAddress() && addr.getHostAddress().indexOf(':') == -1) {
                    ipSet.add(addr.getHostAddress());
                }
            }
        }
        return ipSet;
    }

    public  void clearState() {
        this.clientInfo = null;
        this.businessDataSourceConfig = null;
        this.lastExecutionPlan = new ExecutionPlan();
    }

    // 向配置库注册客户端，如果不存在
    public void insertTableCreationRecord(TableMappingConfig tableMappingConfig,
                                          TargetDataSourceConfig targetDataSourceConfig,
                                          String targetTableName) throws Exception {


        try (Connection conn = dataSource.getConnection();
             PreparedStatement insertStmt = conn.prepareStatement(CdcSqlConstants.INSERT_TABLE_CREATION_LOG)) {
            insertStmt.setString(1, UUID.randomUUID().toString().replace("-", ""));
            insertStmt.setString(2, tableMappingConfig.getBusinessDataSourceConfig().getDataSourceId());
            insertStmt.setString(3, tableMappingConfig.getSchemaName());
            insertStmt.setString(4, tableMappingConfig.getTableName());
            insertStmt.setString(5, targetDataSourceConfig.getDataSourceId());
            insertStmt.setString(6, targetDataSourceConfig.getDataSourceUserName());
            insertStmt.setString(7, targetTableName);
            insertStmt.setString(8, "1");
            insertStmt.executeUpdate();
        }
    }

    /**
     * 检查建表记录是否存在
     */
    public boolean checkTableCreationRecordExists(TableMappingConfig tableMappingConfig,
                                                  TargetDataSourceConfig targetDataSourceConfig) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.CHECK_TABLE_CREATION_RECORD_EXISTS)) {
            ps.setString(1, tableMappingConfig.getBusinessDataSourceConfig().getDataSourceId());
            ps.setString(2, tableMappingConfig.getSchemaName());
            ps.setString(3, tableMappingConfig.getTableName());
            ps.setString(4, targetDataSourceConfig.getDataSourceId());
            ps.setString(5, targetDataSourceConfig.getDataSourceUserName());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    /**
     * 查询建表记录的目标表名
     */
    public String queryTargetTableNameFromCreationLog(ServerTopicInfo serverTopicInfo,TargetDataSourceConfig targetDataSourceConfig) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.QUERY_TARGET_TABLE_NAME_FROM_CREATION_LOG)) {
            ps.setString(1, serverTopicInfo.getBusinessDataSourceId());
            ps.setString(2, serverTopicInfo.getSchemaName());
            ps.setString(3, serverTopicInfo.getTableName());
            ps.setString(4, serverTopicInfo.getTargetDataSourceId());
            ps.setString(5, targetDataSourceConfig.getDataSourceUserName());

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("TARGET_TABLE_NAME");
                }
            }
        }
        return null;
    }


}
