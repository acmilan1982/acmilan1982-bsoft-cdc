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
import com.bsoft.common.config.properties.ClientInfo;
import com.bsoft.common.config.properties.PipeLineInfo;
import com.bsoft.common.config.properties.ServerInfo;
import com.bsoft.common.config.properties.SimpleClientInfo;
import com.bsoft.common.constants.CdcSqlConstants;
import com.bsoft.common.enums.*;
import com.bsoft.common.pipeline.PlanNodeType;
import com.bsoft.common.pipeline.model.PlanNodeSpec;
import com.bsoft.common.pipeline.model.RulePlan;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;

@Component
public class DatasourceConfigFetcher {

    private static final Logger logger = LogManager.getLogger(DatasourceConfigFetcher.class);

    // 客户端信息
    private ClientInfo clientInfo;

    // 服务端信息
    private ServerInfo serverInfo;

    // 流程编排端信息
    private PipeLineInfo pipeLineInfo;

    // 业务库的配置对象
    @Autowired
    private ConfigDataSourceConfig configDataSourceConfig;


    // 配置库的DataSource
    private DataSource dataSource;


//    //业务库的配置对象
//    private BusinessDataSourceConfig businessDataSourceConfig;

    // 业务库配置中心
    private BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry;


//    private BusinessDataSourceConfig businessDataSourceConfig;

    // 探针端信息缓存，来源于 CDC_CLIENT 表
    private volatile ActiveClientCache activeClientCache = ActiveClientCache.empty();
    private volatile boolean activeClientCacheInitialized = false;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

//        String dataSourceIdSring;
        // 1. 检查配置库中，客户端是否存在，如果存在，使用库中的信息填充ClientInfo
        try (Connection conn = dataSource.getConnection();
             PreparedStatement checkStmt = conn.prepareStatement(CdcSqlConstants.FETCH_CLIENT_SQL)) {

            checkStmt.setString(1, clientInfo.getClientId());
            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next()) {
                    // 如果客户端已经注册，读取需要 同步的业务库ids，可能需要同步个业务库
                    clientInfo.setDataSourceIds(rs.getString("DATA_SOURCE_ID"));
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
//            insertStmt.setString(4, "TABLE_MERGE");
            insertStmt.executeUpdate();
        }
    }



    // 向 配置库 注册中心库
    public void registerServerIfAbsent(ServerInfo serverInfo) throws Exception {
        this.serverInfo = serverInfo;
        String serverId =  serverInfo.getServerId();

        // 1.先查询
        try (Connection conn = dataSource.getConnection();
             PreparedStatement checkStmt = conn.prepareStatement(CdcSqlConstants.FETCH_SERVER_SQL)) {
            checkStmt.setString(1, serverId);
            try (ResultSet rs = checkStmt.executeQuery()) {
                if (rs.next() && rs.getInt(1) == 0) {
                    // 2.不存在则注册
                    String ipDesc = String.join(", ", getLocalIPAddresses());
                    try (PreparedStatement insertStmt = conn.prepareStatement(CdcSqlConstants.REGISTER_SERVER_SQL)) {
                        insertStmt.setString(1, serverId);
                        insertStmt.setString(2, "服务端注册自: " + serverId + " IP: " + ipDesc);
                        insertStmt.setString(3, "1");
                        insertStmt.executeUpdate();
                    }
                }
            }
        }

    }

    // 向 配置库 注册中心库
    public void registerPipeLineIfAbsent(PipeLineInfo pipeLineInfo) throws Exception {
        this.pipeLineInfo = pipeLineInfo;


    }

    // 读取当前客户端 需要采集的所有数据源，每个源封装成一个BusinessDataSourceConfig
    public BusinessDataSourceConfigRegistry getClientBasicBusinessDataSourceConfig() throws Exception {
        // 解析当前 客户端配置的业务库数据源id，并去重，形成List
        List<String> dataSourceIdList = parseClientDataSourceIds(trim(clientInfo.getDataSourceIds()));
        BusinessDataSourceConfigRegistry registry = new BusinessDataSourceConfigRegistry();

        if (dataSourceIdList.isEmpty()) {
            return registry;
        }

        String placeholders = dataSourceIdList.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));
        String sql = CdcSqlConstants.FETCH_DATA_SOURCE_BY_IDS_SQL.replace("{}", placeholders);

        Map<String, BusinessDataSourceConfig> configById = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int index = 1;
            for (String dataSourceId : dataSourceIdList) {
                ps.setString(index++, dataSourceId);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    BusinessDataSourceConfig config = buildBusinessDataSourceConfig(rs);
                    configById.put(config.getDataSourceId(), config);
                }
            }
        }


        for (String dataSourceId : dataSourceIdList) {
            registry.register(configById.get(dataSourceId));
        }

        this.businessDataSourceConfigRegistry = registry;
        return registry;
    }


    // 解析当前 客户端配置的业务库数据源id，并去重
    private List<String> parseClientDataSourceIds(String dataSourceIds) {
        if (dataSourceIds.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(dataSourceIds.split(","))
                .map(String::trim)
                .filter(id -> !id.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }












    public Map<String, Set<String>> findUnreadableSourceTables(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry,
                                                               Map<String, Set<String>> sourceTablesByDataSourceId) throws Exception {
        // 返回值：key 是业务库 dataSourceId，value 是当前业务库下不可访问的 SCHEMA.TABLE 集合
        Map<String, Set<String>> unreadableTables = new LinkedHashMap<>();

        // 入参为空时直接返回空结果
        if (businessDataSourceConfigRegistry == null || businessDataSourceConfigRegistry.isEmpty()
                || sourceTablesByDataSourceId == null || sourceTablesByDataSourceId.isEmpty()) {
            return unreadableTables;
        }

        // 逐个业务库检查表访问权限
        for (Map.Entry<String, Set<String>> entry : sourceTablesByDataSourceId.entrySet()) {
            // 规范化业务库 ID
            String dataSourceId = trim(entry.getKey());
            if (dataSourceId.isEmpty()) {
                continue;
            }

            // 通过 registry 按 dataSourceId 查找业务库配置
            BusinessDataSourceConfig businessDataSourceConfig =
                    businessDataSourceConfigRegistry.findBusinessDataSourceConfigByDataSourceId(dataSourceId);
            if (businessDataSourceConfig == null) {
                continue;
            }

            // 当前业务库下需要检查的所有源表
            Set<String> fullyQualifiedTables = entry.getValue();
            if (fullyQualifiedTables == null || fullyQualifiedTables.isEmpty()) {
                continue;
            }

            // 每个业务库只创建一个 DataSource，并复用一个连接完成全部表检查
            javax.sql.DataSource businessDataSource = null;
            try {
                // 创建当前业务库的连接池
                businessDataSource = businessDataSourceConfig.getDataSource();

                // 使用一个连接循环检查该业务库下所有待采集表
                try (Connection conn = businessDataSource.getConnection()) {
                    for (String fullyQualifiedTableName : fullyQualifiedTables) {
                        // 规范化表名
                        String normalizedTableName = trim(fullyQualifiedTableName);
                        if (normalizedTableName.isEmpty()) {
                            continue;
                        }

                        // 如果当前账号无法访问该表，则记录为不可访问表
                        if (!canSelectSourceTable(conn, normalizedTableName)) {
                            unreadableTables
                                    .computeIfAbsent(dataSourceId, k -> new LinkedHashSet<>())
                                    .add(normalizedTableName);
                        }
                    }
                }
            } finally {
                // 当前 DataSource 是运行时临时创建的 HikariDataSource，需要主动关闭
                if (businessDataSource instanceof com.zaxxer.hikari.HikariDataSource) {
                    ((com.zaxxer.hikari.HikariDataSource) businessDataSource).close();
                }
            }
        }

        // 返回所有不可访问表
        return unreadableTables;
    }



    /**
     * 使用轻量查询检查当前账号是否可访问指定业务库表。
     *
     * @param conn 业务库连接
     * @param fullyQualifiedTableName 格式要求：SCHEMA.TABLE
     */
    private boolean canSelectSourceTable(Connection conn, String fullyQualifiedTableName) {
        String normalizedTableName = trim(fullyQualifiedTableName);
        if (conn == null || normalizedTableName.isEmpty()) {
            return false;
        }

        String sql = String.format(
                CdcSqlConstants.ORACLE_CHECK_SOURCE_TABLE_SELECT_PRIVILEGE_SQL,
                normalizedTableName
        );

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return true;
        } catch (Exception e) {
            logger.warn("业务库表访问权限检查失败，table={}, msg={}", normalizedTableName, e.getMessage());
            return false;
        }
    }
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

//    //  读取 业务库数据源 中的基本配置，比如url,用户名,密码
//    public BusinessDataSourceConfig getBusinessDataSourceBasicConfig() throws Exception {
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_DATA_SOURCE_SQL)) {
//            ps.setString(1, clientInfo.getClientId());
//            try (ResultSet rs = ps.executeQuery()) {
//                if (rs.next()) {
//                    this.businessDataSourceConfig = buildBusinessDataSourceConfig(rs);
//                }
//            }
//        }
//        return this.businessDataSourceConfig;
//    }


    // CDC_DATA_SOURCE中的单条记录封装成一个 BusinessDataSourceConfig
    private BusinessDataSourceConfig buildBusinessDataSourceConfig(ResultSet rs) throws Exception {
        return BusinessDataSourceConfig.builder()
                .dataSourceId(rs.getString("DATA_SOURCE_ID"))
                .dataSourceOrg(rs.getString("DATA_SOURCE_ORG"))
                .dataSourceHost(rs.getString("DATA_SOURCE_HOST"))
                .dataSourcePort(rs.getInt("DATA_SOURCE_PORT"))
                .dataSourceUserName(rs.getString("DATA_SOURCE_USER_NAME"))
                .dataSourcePassword(rs.getString("DATA_SOURCE_PASSWORD"))
                .dataSourceType(DatabaseType.fromCode(rs.getString("DATA_SOURCE_TYPE")))
                .dataSourceServiceName(rs.getString("DATA_SOURCE_SERVICE_NAME"))
                .fgActive(rs.getString("FG_ACTIVE"))
                .clientId(this.clientInfo == null ? null : this.clientInfo.getClientId())
                .build();
    }



    // 读取 当前客户端 对应的 目标库数据源 配置
    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry() throws Exception {
        // 当前客户端 使用到的目标库数据源id
        Set<String> targetDataSourceIdSet = collectTargetDataSourceIds(this.businessDataSourceConfigRegistry.getBusinessDataSourceConfigList());
        return getTargetDataSourceConfig(targetDataSourceIdSet);
    }



    private Set<String> collectTargetDataSourceIds(List<BusinessDataSourceConfig> businessDataSourceConfigList) throws Exception {
        // 保存当前客户端 使用到的目标库数据源id
        Set<String> targetDataSourceIdSet = new LinkedHashSet<>();

        try (Connection conn = dataSource.getConnection();
             // 根据业务库数据源id，从CDC_DATA_SUBSCRIBE中查询目标库数据源id
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_TARGET_DATA_SOURCE_ID_BY_BUSINESS_DATA_SOURCE_ID_SQL)) {

            for (BusinessDataSourceConfig businessDataSourceConfig : businessDataSourceConfigList) {

                String businessDataSourceId = trim(businessDataSourceConfig.getDataSourceId());
                if (businessDataSourceId.isEmpty()) {
                    continue;
                }

                ps.clearParameters();
                ps.setString(1, "%," + businessDataSourceId + ",%");

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        //
                        targetDataSourceIdSet.addAll(parseCommaSeparatedIds(rs.getString("DATA_TO_SOURCE_ID")));
                    }
                }
            }
        }

        return targetDataSourceIdSet;
    }


    /**
     * 查询当前业务库列表是否已经被其他 sync-client 占用执行计划。
     *
     * 返回值说明：
     * 1. key: 发生冲突的 FROM_DATA_SOURCE_ID
     * 2. value: 占用该业务库的其他 CLIENT_ID 集合
     * 3. 如果返回空 Map，表示没有冲突
     */
    public Map<String, Set<String>> findExecutionPlanSourceOwnershipConflicts(String currentClientId,
                                                                              List<String> dataSourceIdList) throws Exception {
        Map<String, Set<String>> conflicts = new LinkedHashMap<>();

        String normalizedClientId = trim(currentClientId);
        if (normalizedClientId.isEmpty() || dataSourceIdList == null || dataSourceIdList.isEmpty()) {
            return conflicts;
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.CHECK_EXECUTION_PLAN_SOURCE_OWNERSHIP_SQL)) {

            for (String dataSourceId : dataSourceIdList) {
                String normalizedDataSourceId = trim(dataSourceId);
                if (normalizedDataSourceId.isEmpty()) {
                    continue;
                }

                ps.clearParameters();
                ps.setString(1, normalizedDataSourceId);
                ps.setString(2, normalizedClientId);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String ownerClientId = trim(rs.getString("CLIENT_ID"));
                        String fromDataSourceId = trim(rs.getString("FROM_DATA_SOURCE_ID"));

                        if (ownerClientId.isEmpty() || fromDataSourceId.isEmpty()) {
                            continue;
                        }

                        conflicts
                                .computeIfAbsent(fromDataSourceId, k -> new LinkedHashSet<>())
                                .add(ownerClientId);
                    }
                }
            }
        }

        return conflicts;
    }

    // 将逗号分隔的 ID 字符串解析为有序去重列表。
    public List<String> parseCommaSeparatedIds(String rawIds) {
        if (rawIds == null || rawIds.trim().isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(rawIds.split(","))
                .map(String::trim)
                .filter(id -> !id.isEmpty())
                .distinct()
                .collect(Collectors.toList());
    }

    // 读取当前业务库的订阅关系(专供客户端使用)
    public SubscriberMappingRegistry getSubscriberMapping() throws Exception {

        // 所有业务库的订阅，都集中到 SubscriberMappingRegistry的tempSubscriberMappingList，稍后会详细解析成一对一的关系
        SubscriberMappingRegistry subscriberMappingRegistry = new SubscriberMappingRegistry();
        subscriberMappingRegistry.setClientInfo(this.clientInfo);

        for (BusinessDataSourceConfig businessDataSourceConfig : this.businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()) {

            // 读取一个业务库的订阅
            // 为了安全起见，把 当前业务库 所有订阅都读出来，因为搞不好一个业务库会有多个订阅关系
            // CDC_DATA_SUBSCRIBE表中的单条记录 构建一个 SubscriberMapping 对象
            List<SubscriberMapping> tempSubscriberMappingList = getSubscriberMappingList(businessDataSourceConfig);

            for (SubscriberMapping subscriberMapping : tempSubscriberMappingList) {

                // 移除当前业务库的 FROM_SOURCE_ID
                subscriberMapping.getTempFromDataSourceIdList()
                        .removeIf(fromDataSourceId -> !fromDataSourceId.equals(businessDataSourceConfig.getDataSourceId()));

                // 移除当前业务库的表
                subscriberMapping.getTempSourceTableNameList()
                        .removeIf(table -> !table.getDataSourceId().equals(businessDataSourceConfig.getDataSourceId()));
            }

            subscriberMappingRegistry.addTempSubscriberMappingList(tempSubscriberMappingList);

        }


        // 这里处理一对一，多对一，一对多，多对多的所有关系，都转换成一对多的关系
        subscriberMappingRegistry.afterPropertiesSet();
        return subscriberMappingRegistry;
    }

    // 读取 一个业务库 的订阅关系列表
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


    // CDC_DATA_SUBSCRIBE表中的单条记录 构建一个 SubscriberMapping 对象
    private SubscriberMapping buildSubscriberMapping(ResultSet rs) throws SQLException {
        SubscriberMapping subscriberMapping = new SubscriberMapping();
        subscriberMapping.addTempFromDataSourceId(rs.getString("DATA_FROM_SOURCE_ID"));
        subscriberMapping.addTempToDataSourceId(rs.getString("DATA_TO_SOURCE_ID"));
        subscriberMapping.addTempSourceTableName(rs.getString("DATA_SOURCE_TABLE"));
        return subscriberMapping;
    }


    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------
    //-------------------------------------------中心端方法-----------------------------------

    //  读取 业务库数据源 中的基本配置，比如url,用户名,密码
    public void loadServerInfoConfig(ServerInfo serverInfo) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_SERVER_CONFIG_SQL)) {
//            ps.setString(1, serverInfo.getServerId());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String key = rs.getString("CONFIG_KEY");
                    String value = rs.getString("CONFIG_VALUE");
                    if (key == null || value == null) {
                        continue;
                    }

                    switch (key) {
                        case "tableRowDeleteStrategy":
                            serverInfo.setTableRowDeleteStrategy(TableRowDeleteStrategy.fromCode(value));
                            break;
                        case "raw-message-storage-strategy":
                            serverInfo.setRawMessageStorageStrategy(RawMessageStorageStrategy.valueOf(value));
                            break;
                        case "snapshotBatchSize":
                            serverInfo.setSnapshotBatchSize(Integer.parseInt(value));
                            break;
                        case "auto-create-table":
                            serverInfo.setAutoCreateTable(Boolean.parseBoolean(value));
                            break;
                        case "auto-expand-column-length":
                            serverInfo.setAutoExpandColumnLength(Boolean.parseBoolean(value));
                            break;
                        case "server-log-topic-name":
                            serverInfo.setServerLogTopic(value);
                            break;
                        case "monitor-metric-topic-name":
                            serverInfo.setMonitorMetricTopic(value);
                            break;
                        case "realtime-insert-batch-enabled-database-types":

                            List<DatabaseType> realtimeInsertBatchEnabledDatabaseTypes = Arrays.stream(value.split(","))
                                    // 在传入 fromCode 之前，直接处理好字符串，减少 Stream 阶段
                                    .map(str -> DatabaseType.fromCode(str.trim().toUpperCase()))
                                    .collect(Collectors.toList());

                            serverInfo.setRealtimeInsertBatchEnabledDatabaseTypes(realtimeInsertBatchEnabledDatabaseTypes);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
    }


    /**
     * 按 key 读取 CDC_SERVER_CONFIG 配置值。
     */
    public String loadServerConfigValue(String configKey) throws Exception {
        // 校验配置 key 不能为空。
        if (configKey == null || configKey.trim().isEmpty()) {
            throw new IllegalArgumentException("configKey is blank");
        }

        // 查询 CDC_SERVER_CONFIG。
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_SERVER_CONFIG_VALUE_SQL)) {

            // 绑定配置 key。
            ps.setString(1, configKey.trim());

            // 执行查询。
            try (ResultSet rs = ps.executeQuery()) {
                // 如果存在配置，则返回配置值。
                if (rs.next()) {
                    return rs.getString("CONFIG_VALUE");
                }
            }
        }

        // 没查到则返回 null。
        return null;
    }

    // 读取 目标库数据源 配置,目前一个中心端，向所有目标库写入
    public TargetDataSourceConfigRegistry generateTargetDataSourceConfigRegistry() throws Exception {
        // 读取 当前服务端 对应的 目标库数据源 配置
        // 创建TargetDataSourceConfig，直接注册到TargetDataSourceConfigRegistry中
        TargetDataSourceConfigRegistry targetDataSourceConfigRegistry = getTargetDataSourceConfig();

        if(this.serverInfo != null) {
            // 向serverInfo设置 目标库数据源
            this.serverInfo.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 向serverInfo设置 配置库数据源
            this.serverInfo.setConfigDataSourceConfig(configDataSourceConfig);
        }

        if(this.pipeLineInfo != null) {
            // 向serverInfo设置 目标库数据源
            this.pipeLineInfo.setTargetDataSourceConfigRegistry(targetDataSourceConfigRegistry);
            // 向serverInfo设置 配置库数据源
            this.pipeLineInfo.setConfigDataSourceConfig(configDataSourceConfig);
        }

        return targetDataSourceConfigRegistry;
    }


    // 读取 当前服务端 对应的 目标库数据源 配置
    // 创建TargetDataSourceConfig，直接注册到TargetDataSourceConfigRegistry中
    public TargetDataSourceConfigRegistry getTargetDataSourceConfig() throws Exception {
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
                    if(this.serverInfo != null){  // 服务端需要 目标库的详细信息
                        targetDataSourceConfig.setDatabaseInfo(buildDatabaseInfo(targetDataSourceConfig));
                    }
                    targetDataSourceConfigRegistry.register(targetDataSourceConfig);
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
                .dataSourceBizAttr(rs.getString("DATA_SOURCE_BIZ_ATTR"))
                .build();
    }


    public DatabaseInfo buildDatabaseInfo(TargetDataSourceConfig targetDataSourceConfig) {
        // (目标)数据库信息，包括版本号等
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
                        dbInfo = new DatabaseInfo(DatabaseType.MYSQL, fullVersion);
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







    // 仅服务端使用
    // 读取所有有效的 订阅关系-
    public SubscriberMappingRegistry generateSubscriberMappingRegistry() throws Exception {
        // 所有的订阅关系，都集中到 SubscriberMappingRegistry
        SubscriberMappingRegistry subscriberMappingRegistry = new SubscriberMappingRegistry();
        if(this.serverInfo != null){
            subscriberMappingRegistry.setServerInfo(this.serverInfo);
        }

        if(this.pipeLineInfo != null){
            subscriberMappingRegistry.setPipeLineInfo(this.pipeLineInfo);
        }


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
        if(this.serverInfo != null){
            this.serverInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);
        }

        if(this.pipeLineInfo != null){
            this.pipeLineInfo.setSubscriberMappingRegistry(subscriberMappingRegistry);
        }
        return subscriberMappingRegistry;
    }







    //  优先复用已初始化缓存，未初始化时再查库并填充缓存
    public List<SimpleClientInfo> getSimpleClientInfoList() throws Exception {
        if (activeClientCacheInitialized) {
            return new ArrayList<>(activeClientCache.byClientIdView().values());
        }

        synchronized (this) {
            if (activeClientCacheInitialized) {
                return new ArrayList<>(activeClientCache.byClientIdView().values());
            }

            List<SimpleClientInfo> activeClients = loadActiveSimpleClientInfos();
            this.activeClientCache = ActiveClientCache.from(activeClients);
            this.activeClientCacheInitialized = true;
            logger.info("Active client cache initialized lazily, total={}", this.activeClientCache.size());

            return activeClients;
        }
    }





    // 强制刷新,刷新 CDC_CLIENT_MULTIPLE 激活客户端缓存（FG_ACTIVE='1'）。
    public void refreshActiveClientCache() throws Exception {
        List<SimpleClientInfo> activeClients = loadActiveSimpleClientInfos();
        this.activeClientCache = ActiveClientCache.from(activeClients);
        this.activeClientCacheInitialized = true;
        logger.info("Active client cache refreshed, total={}", this.activeClientCache.size());
    }


    // 读取 CDC_CLIENT_MULTIPLE 表 FG_ACTIVE = '1'，为 每条记录 创建单个 SimpleClientInfo
    public List<SimpleClientInfo> loadActiveSimpleClientInfos() throws Exception {
        List<SimpleClientInfo> result = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_SIMPLE_CLIENT_SQL);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                SimpleClientInfo info = buildSimpleClientInfo(rs);
                if (info != null) {
                    result.add(info);
                }
            }
        }

        return result;
    }

    // 为  CDC_CLIENT_MULTIPLE 表 FG_ACTIVE = '1' 每条记录 创建单个 SimpleClientInfo
    private SimpleClientInfo buildSimpleClientInfo(ResultSet rs) throws SQLException {
        // 只规范化索引键，避免缓存 key 因脏数据失配
        String clientId = trim(rs.getString("CLIENT_ID"));
        String dataSourceIds = trim(rs.getString("DATA_SOURCE_ID"));

        if (clientId.isEmpty() || dataSourceIds.isEmpty()) {
            return null;
        }

        SimpleClientInfo info = new SimpleClientInfo();
        info.setClientId(clientId);
        info.setDataSourceIds(dataSourceIds);

        return info;
    }





    // 中心端读取所有业务库配置，目前只有一个中心端
    public BusinessDataSourceConfigRegistry generateBusinessDataSourceConfigRegistry() throws Exception {

        // 2.读取所有 业务库数据源 的基本配置，包括url，用户名等，读取已经与客户端绑定的源
        BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry = getAllBasicBusinessDataSourceConfig();
        businessDataSourceConfigRegistry.afterPropertiesSet();

        this.businessDataSourceConfigRegistry = businessDataSourceConfigRegistry;
        return businessDataSourceConfigRegistry;
    }



    // 读取所有源数据源的基础配置：
    // 1. CDC_CLIENT_MULTIPLE.FG_ACTIVE='1' -> DATA_SOURCE_ID
    // 2. CDC_DATA_SUBSCRIBE.FG_ACTIVE='1' -> DATA_FROM_SOURCE_ID
    // 3. 取交集
    // 4. 用交集去查 CDC_DATA_SOURCE + CDC_DATA_SOURCE_EXTEND，并注册到 registry
    /**
     * 读取所有有效业务库基础配置，并装配目标表命名规则。
     */
    public BusinessDataSourceConfigRegistry getAllBasicBusinessDataSourceConfig() throws Exception {
        // 创建业务库注册表
        BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry = new BusinessDataSourceConfigRegistry();

        // 从 CDC_CLIENT_MULTIPLE 中读取所有 DATA_SOURCE_ID
        Set<String> clientMultipleDataSourceIds =
                loadCommaSeparatedIds(CdcSqlConstants.FETCH_ACTIVE_CLIENT_MULTIPLE_DATA_SOURCE_IDS_SQL, "DATA_SOURCE_ID");

        // 从 CDC_DATA_SUBSCRIBE 中读取所有 DATA_FROM_SOURCE_ID
        Set<String> subscribeFromDataSourceIds =
                loadCommaSeparatedIds(CdcSqlConstants.FETCH_ACTIVE_DATA_SUBSCRIBE_FROM_SOURCE_IDS_SQL, "DATA_FROM_SOURCE_ID");

        // 取两类来源的交集
        clientMultipleDataSourceIds.retainAll(subscribeFromDataSourceIds);

        // 没有交集时直接返回空注册表
        if (clientMultipleDataSourceIds.isEmpty()) {
            return businessDataSourceConfigRegistry;
        }

        // 为 IN 查询构造占位符
        String placeholders = clientMultipleDataSourceIds.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));

        // 生成查询 CDC_DATA_SOURCE 的 SQL
        String sql = CdcSqlConstants.FETCH_DATA_SOURCE_BY_IDS_SQL.replace("{}", placeholders);

        // 按数据源 ID 暂存基础配置
        Map<String, BusinessDataSourceConfig> configMap = new LinkedHashMap<>();

        // 获取配置库连接并创建查询
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 初始化参数下标
            int index = 1;

            // 绑定所有数据源 ID 参数
            for (String dataSourceId : clientMultipleDataSourceIds) {
                ps.setString(index++, dataSourceId);
            }

            // 执行查询并遍历结果
            try (ResultSet rs = ps.executeQuery()) {
                // 逐行构建基础业务库配置
                while (rs.next()) {
                    // 从结果集构建 BusinessDataSourceConfig
                    BusinessDataSourceConfig config = buildBusinessDataSourceConfig(rs);

                    // 按 DATA_SOURCE_ID 放入 map
                    configMap.put(config.getDataSourceId(), config);
                }
            }
        }

        // 批量读取目标表命名规则，索引结构为：业务库 ID -> 目标库 ID -> 命名规则
        Map<String, Map<String, TargetTableNamingRule>> namingRuleMap = loadTargetTableNamingRuleMap(configMap.keySet());

        // 从 activeClientCache 中取 dataSourceId -> clientId 映射
        Map<String, SimpleClientInfo> activeClientByDataSourceId = activeClientCache.byDataSourceIdView();

        // 遍历所有业务库基础配置
        for (BusinessDataSourceConfig config : configMap.values()) {
            String dataSourceId = config.getDataSourceId();
            // 为当前业务库装配命名规则
            config.setTargetTableNamingRule(namingRuleMap.get(config.getDataSourceId()));

            // 按 dataSourceId 回填所属 clientId
            SimpleClientInfo simpleClientInfo = activeClientByDataSourceId.get(dataSourceId);
            if (simpleClientInfo != null) {
                config.setClientId(simpleClientInfo.getClientId());
            }

            // 将装配后的业务库配置注册到注册表
            businessDataSourceConfigRegistry.register(config);
        }

        // 返回装配完成的注册表
        return businessDataSourceConfigRegistry;
    }

    /**
     * 执行指定 SQL，读取逗号分隔字段并展开为去重后的 ID 集合。
     */
    private Set<String> loadCommaSeparatedIds(String sql, String columnName) throws Exception {
        // 创建有序去重集合
        Set<String> result = new LinkedHashSet<>();

        // 获取配置库连接并执行查询
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            // 遍历查询结果
            while (rs.next()) {
                // 读取当前列原始字符串
                String rawIds = rs.getString(columnName);

                // 解析并加入结果集合
                result.addAll(parseCommaSeparatedIds(rawIds));
            }
        }

        // 返回展开后的 ID 集合
        return result;
    }

    // 按业务库 ID 批量读取目标表命名规则，返回结构为：业务库 ID -> 目标库 ID -> 命名规则
    private Map<String, Map<String, TargetTableNamingRule>> loadTargetTableNamingRuleMap(Collection<String> dataSourceIds) throws Exception {
        // 入参为空时直接返回空 map
        if (dataSourceIds == null || dataSourceIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // 为 IN 查询构造占位符
        String placeholders = dataSourceIds.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));

        // 生成查询 CDC_DATA_SOURCE_EXTEND 的 SQL
        String sql = CdcSqlConstants.FETCH_DATA_SOURCE_EXTEND_BY_IDS_SQL.replace("{}", placeholders);

        // 创建按 DATA_SOURCE_ID 索引的规则 map
        Map<String, Map<String, TargetTableNamingRule>> ruleMap = new LinkedHashMap<>();

        // 获取配置库连接并创建查询
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            // 初始化参数下标
            int index = 1;

            // 绑定所有数据源 ID 参数
            for (String dataSourceId : dataSourceIds) {
                ps.setString(index++, dataSourceId);
            }

            // 执行查询并遍历结果
            try (ResultSet rs = ps.executeQuery()) {
                // 逐行构建命名规则对象
                while (rs.next()) {
                    // 从结果集构建命名规则
                    TargetTableNamingRule rule = buildTargetTableNamingRule(rs);

                    // 规范化业务库 ID
                    String dataSourceId = trim(rule.getDataSourceId());

                    // 规范化目标库 ID
                    String targetDataSourceId = trim(rule.getTargetDataSourceId());

                    // 数据源 ID 为空时跳过
                    if (dataSourceId.isEmpty() || targetDataSourceId.isEmpty()) {
                        continue;
                    }


                    // 获取当前业务库下的目标库规则 Map
                    Map<String, TargetTableNamingRule> rulesByTargetDataSourceId =
                            ruleMap.computeIfAbsent(dataSourceId, key -> new LinkedHashMap<>());

                    // 同一个业务库 + 目标库只允许使用第一条规则
                    if (rulesByTargetDataSourceId.containsKey(targetDataSourceId)) {
                        logger.warn(
                                "CDC_DATA_SOURCE_EXTEND中存在多条 DATA_SOURCE_ID 与 TARGET_DATA_SOURCE_ID 相同的记录，本次启动会使用第一条记录，忽略其他记录，dataSourceId={}, targetDataSourceId={}",
                                dataSourceId,
                                targetDataSourceId
                        );
                        continue;
                    }

                    // 写入命名规则索引
                    rulesByTargetDataSourceId.put(targetDataSourceId, rule);
                }
            }
        }

        // 返回命名规则索引
        return ruleMap;
    }

    private  String buildKey(String str1, String str2) {
        return str1 + "|" + str2;
    }

    /**
     * 从 CDC_DATA_SOURCE_EXTEND 的结果集构建目标表命名规则对象。
     */
    private TargetTableNamingRule buildTargetTableNamingRule(ResultSet rs) throws SQLException {
        // 读取并规整命名策略原始值
        String strategyRaw = trim(rs.getString("TABLE_NAMING_STRATEGY"));

        // 默认命名策略为空
        TableNamingStrategy strategy = null;

        // 原始值非空时转换为枚举
        if (!strategyRaw.isEmpty()) {
            strategy = TableNamingStrategy.fromCode(strategyRaw);
        }

        // 按字段构建目标表命名规则对象
        return TargetTableNamingRule.builder()
                .dataSourceId(trim(rs.getString("DATA_SOURCE_ID")))        //  设置数据源 ID
                .targetDataSourceId(rs.getString("TARGET_DATA_SOURCE_ID"))  //  目标数据源 ID
                .tableNamingStrategy(strategy) // 设置命名策略
                .tableNamePrefix(rs.getString("TABLE_NAME_PREFIX")) // 设置表名前缀
                .tableNameSuffix(rs.getString("TABLE_NAME_SUFFIX")) // 设置表名后缀
                .build(); // 返回构建结果
    }

    // 服务端：根据订阅关系，批量生成表的执行计划
    public ExecutionPlan batchGenerateTableExecutionPlan(SubscriberMappingRegistry subscriberMappingRegistry){
        ExecutionPlan currentExecutionPlan = new ExecutionPlan();
        // 根据订阅关系，生成表级执行计划
        for (SubscriberMapping subscriberMapping : subscriberMappingRegistry.getSubscriberMappingList()) {
            batchGenerateTableExecutionPlan(currentExecutionPlan,subscriberMapping, TableStatus.RECEIVE);
        }

        // 应用执行计划的变更，
        // 如果是服务端的执行计划，此时，deletedTableExecutionPlan和tableExecutionPlans都是null
        // 基本上就是把addedTableExecutionPlan都加到tableExecutionPlans
        currentExecutionPlan.applyChanges();
        return currentExecutionPlan;
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




    // 获取执行计划，目前没什么实际作用,弃用CDC_EXECUTION_PLAN表
    public ExecutionPlan buildClientExecutionPlan(SubscriberMappingRegistry subscriberMappingRegistry) throws Exception {

        // 从 订阅关系中 中生成当前的一次执行计划
        ExecutionPlan lastExecutionPlan = batchGenerateTableExecutionPlan(subscriberMappingRegistry, TableStatus.NORMAL);


        // 应用执行计划的变更，并判断是否需要执行快照
        // 全部执行计划，放到ExecutionPlan.tableExecutionPlans<List>
        lastExecutionPlan.applyChanges();

        return lastExecutionPlan;
    }

//    // 决定当前作业的启动模式
//    public ExecutionPlan buildClientExecutionPlan(SubscriberMappingRegistry subscriberMappingRegistry) throws Exception {
////        // job的默认启动模式
////        JobStartupMode jobStartupMode = JobStartupMode.NORMAL;
//
//        // 从 CDC_EXECUTION_PLAN 中获取上一次执行计划
//        ExecutionPlan lastExecutionPlan = getLastExecutionPlan();
//
//
//        // 当前数据源第一次启动，没有任何执行计划
//        if(lastExecutionPlan.hasNoPlanItems()){
//            // 为所有的表生成执行计划，job的为启动模式：NORMAL
//            // ExecutionPlan.addedTableExecutionPlan<List>
//            batchGenerateTableExecutionPlan(lastExecutionPlan,subscriberMappingRegistry, TableStatus.NORMAL);
//        }else{  // 当前数据源不是第一次启动了，那得检查一下执行计划了
//
//            // 处理 删除表 级执行计划
//            handleRemovedTableExecutionPlan(lastExecutionPlan);
//
//            // 处理 新增表 级执行计划d
//            handleNewTableExecutionPlan(lastExecutionPlan);
//        }
//
//        // 执行计划持久化
//        // 持久化新增的执行计划
//        batchSaveExecutionPlan(lastExecutionPlan);
//        // 持久化删除的执行计划
//        batchDeleteExecutionPlans(lastExecutionPlan);
//
//        // 应用执行计划的变更，并判断是否需要执行快照
//        // 全部执行计划，放到ExecutionPlan.tableExecutionPlans<List>
//        lastExecutionPlan.applyChanges();
//
////        // 如果表的状态是SNAPSHOT，一般是作业的SNAPSHOT模式被中断了，需要继续执行快照
////        if(lastExecutionPlan.hasTablesInSnapshotState()){
////            jobStartupMode = JobStartupMode.SNAPSHOT;
////        }
////        // 设置当前作业的启动模式
////        this.clientInfo.setJobStartupMode(jobStartupMode);
//
//        return lastExecutionPlan;
//    }

    // for client
    // 批量生成表的执行计划，根据订阅关系，通常是第一次启动的时候
    public ExecutionPlan batchGenerateTableExecutionPlan(SubscriberMappingRegistry subscriberMappingRegistry,
                                                TableStatus tableStatus){

        ExecutionPlan executionPlan = new ExecutionPlan();
        // 根据订阅关系，生成表级执行计划
        for (SubscriberMapping subscriberMapping : subscriberMappingRegistry.getSubscriberMappingList()) {
            batchGenerateTableExecutionPlan(executionPlan,subscriberMapping, tableStatus);

        }

        return executionPlan;

    }

    // 根据单个订阅关系，批量生成表的执行计划
    public void batchGenerateTableExecutionPlan(ExecutionPlan executionPlan,SubscriberMapping subscriberMapping,TableStatus tableStatus){
        // 根据订阅关系中的Table生成订阅关系
        for (Table table : subscriberMapping.getSourceTableNameList()) {
            executionPlan.getAddedTableExecutionPlan().add(generateTableExecutionPlan(table,
                                                                                          subscriberMapping,
                                                                                          tableStatus));
        }

    }

    // 生成单表的执行计划
    public ExecutionPlan.TableExecutionPlan generateTableExecutionPlan(Table table,
                                                                       SubscriberMapping subscriberMapping,
                                                                       TableStatus tableStatus){
        return new ExecutionPlan.TableExecutionPlan(table.getDataSourceId(),                  // 业务库 数据源
                                                    subscriberMapping.getToDataSourceId(),    // 目标库 数据源
                                                    table.getSchemaName(),                    // schema
                                                    table.getTableName(),                     // 表名
                                                    tableStatus,
                                                    subscriberMapping.getClientInfo());

    }

    // 处理需要删除的 表级执行计划
    public void handleRemovedTableExecutionPlan(ExecutionPlan lastExecutionPlan){
        // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName
        List<ExecutionPlan.TableExecutionPlan> lastTableExecutionPlans = lastExecutionPlan.getTableExecutionPlans();
        // 遍历上一次的执行计划，判断是否有被删除的表级执行计划
        for (ExecutionPlan.TableExecutionPlan lastTableExecutionPlan : lastTableExecutionPlans) {
            String tableExecutionPlanKey = lastTableExecutionPlan.getKey();
            // 如果已经不存在于当前订阅关系中，则认为是被删除的表级执行计划，该主题不会再新加数据
//            if(!businessDataSourceConfig.getSubscriberMappingRegistry().getMappingSetByToDataSourceIdSchemaNameTableName().contains(tableExecutionPlanKey)){
//                lastExecutionPlan.getDeletedTableExecutionPlan().add(lastTableExecutionPlan);
//            }
        }

    }

    // 处理新增的 表级执行计划
    public void handleNewTableExecutionPlan(ExecutionPlan lastExecutionPlan){
        // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName
        // 新的执行计划，向里面添加，移除旧的执行计划
        Map<String, ExecutionPlan.TableExecutionPlan> newTableExecutionPlanMap = lastExecutionPlan.toMap();
        // 从订阅关系中生成所有的表级执行计划，判断是否有新增的表级执行计划
//        for (SubscriberMapping subscriberMapping : businessDataSourceConfig.getSubscriberMappingRegistry().getSubscriberMappingList()) {
//            handleNewTableExecutionPlan(subscriberMapping, newTableExecutionPlanMap);
//
//        }
    }
//
//    // 处理单个订阅中，新增的 表级 执行计划
//    public void handleNewTableExecutionPlan(SubscriberMapping subscriberMapping,
//                                            Map<String, ExecutionPlan.TableExecutionPlan> newTableExecutionPlanMap){ // 上一次的执行计划，保存在表中，转换成map格式，key:toDataSourceId.schemaName.tableName
//
//        String toDataSourceId = subscriberMapping.getToDataSourceId();
//
//        for (Table table : subscriberMapping.getSourceTableNameList()) {
//            String executionPlanKey = toDataSourceId + "." + table.getSchemaName() + "." + table.getTableName();
//            // 是否已经存在执行计划
//            ExecutionPlan.TableExecutionPlan tableExecutionPlan = newTableExecutionPlanMap.get(executionPlanKey);
//            // tableExecutionPlan为null，通常表示该表是新增的，需要执行快照
//            if(tableExecutionPlan == null){
//                // 新增的执行计划，需要被持久化
//                lastExecutionPlan.getAddedTableExecutionPlan().add(generateTableExecutionPlan(table,
//                                                                                              subscriberMapping,
//                                                                                              TableStatus.SNAPSHOT));
//
//            }
//
//        }
//
//    }
//








    // 读取当前客户端负责的所有源库的上一次执行计划
    public ExecutionPlan getLastExecutionPlan() throws Exception {
        // 上一次运行时的执行计划 / 当前运行时 的执行计划
        ExecutionPlan lastExecutionPlan = new ExecutionPlan();

        List<String> dataSourceIdList = getBusinessDataSourceIdList();

        String placeholders = dataSourceIdList.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));

        String sql = CdcSqlConstants.FETCH_EXECUTION_PLAN_BY_SOURCE_IDS_SQL.replace("{}", placeholders);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, this.clientInfo.getClientId());
            int index = 2;
            for (String dataSourceId : dataSourceIdList) {
                ps.setString(index++, dataSourceId);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    lastExecutionPlan.addPlanItem(
                            rs.getString("CLIENT_ID"),
                            rs.getString("FROM_DATA_SOURCE_ID"),
                            rs.getString("TO_DATA_SOURCE_ID"),
                            rs.getString("SCHEMA_NAME"),
                            rs.getString("TABLE_NAME"),
                            rs.getString("TABLE_STATUS"),
                            this.clientInfo
                    );
                }
            }
        }

        return lastExecutionPlan;
    }

    private List<String> getBusinessDataSourceIdList() {
        return this.businessDataSourceConfigRegistry.getBusinessDataSourceConfigList()
                .stream()
                .map(BusinessDataSourceConfig::getDataSourceId)
                .distinct()
                .collect(Collectors.toList());
    }



    // 批量保存 数据源的 执行计划
    public void batchSaveExecutionPlan(ExecutionPlan lastExecutionPlan) throws Exception {
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
    public void batchDeleteExecutionPlans(ExecutionPlan lastExecutionPlan) throws Exception {
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
        this.businessDataSourceConfigRegistry = null;
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



    // 读取表：CDC_PIPELINE，相同 PLAN_ID 的Node，组成一个RulePlan
    public List<RulePlan> loadRulePlans() throws Exception {
        // key: planId
        // value: 当前plan的所有节点，每个节点对应一个PlanNodeSpec
        Map<String, List<PlanNodeSpec>> planNodesByPlanId = new LinkedHashMap<>();

        // key: planId
        // value: 当前plan最终输出的目标表名列表
        Map<String, List<String>> outputTableNameByPlanId = new HashMap<>();

        // key: planId
        // value: 当前plan中，上一个节点的输出id,由于同一个plan中的node是按序获取的，通过该map,可以获取当前plan中，上一个节点的输出id，即当前节点的输入
        Map<String, String> prevOutputByPlanId = new HashMap<>();

        // key: planId
        // value: 当前 plan 的数据来源clientid
        Map<String, String> sqlSourceClientIdByPlanId = new HashMap<>();

        try (Connection conn = dataSource.getConnection();
             // 查询 CDC_PIPELINE 表，根据PLAN_ID, SN排序
             PreparedStatement ps = conn.prepareStatement(CdcSqlConstants.FETCH_CDC_PIPELINE_SQL);
             ResultSet rs = ps.executeQuery()) {

            // 处理 CDC_PIPELINE表 中的每行记录
            while (rs.next()) {
                // plan id
                String planId = trim(rs.getString("PLAN_ID"));
                if (planId.isEmpty()) {
                    continue;
                }

                // 执行序号
                int sn = rs.getInt("SN");
                String typeRaw = trim(rs.getString("PLAN_NODE_TYPE")).toUpperCase(Locale.ROOT);
                // 节点类型
                PlanNodeType type = PlanNodeType.valueOf(typeRaw);

                // 当前节点的所有相关配置
                Map<String, Object> params = parseJsonMap(rs.getString("PLAN_NODE_VALUE"));
                params.put("planNodeDesc", rs.getString("PLAN_NODE_DESC"));
                params.put("sn", sn);


                // 当前节点id
                String nodeId = "p" + planId + "_n" + sn;
                // 当 前节点输出值 的id，后继节点可以通过该id，来获取前驱节点的输出值
                String output = "p" + planId + "_out_" + sn;

                // 当前节点的输入，目前只有一个输入，今后可能有多个，因此直接使用List
                List<String> inputs = new ArrayList<>();
                String prevOutput = prevOutputByPlanId.get(planId);
                if (prevOutput != null) {
                    inputs.add(prevOutput);
                }

                // 创建节点对象
                PlanNodeSpec spec = new PlanNodeSpec();
                // 节点id
                spec.setId(nodeId);
                // 节点类型
                spec.setType(type);
                // 节点的输入id
                spec.setInputs(inputs);
                // 节点的输出id
                spec.setOutput(output);
                // 节点的参数
                spec.setParams(params);

                planNodesByPlanId.computeIfAbsent(planId, k -> new ArrayList<>()).add(spec);
                prevOutputByPlanId.put(planId, output);

                if (type == PlanNodeType.SQL) {
                    sqlSourceClientIdByPlanId.put(planId, resolveClientIdByDataSourceId(params.get("data_source_id").toString()));
                }

                if (type == PlanNodeType.SINK_TABLE) {
                    params.put("client_id",sqlSourceClientIdByPlanId.get(planId));
                    List<String> outputTableNames = resolveSinkTableOutputName(params);
                    if (!outputTableNames.isEmpty()) {
                        outputTableNameByPlanId
                                .computeIfAbsent(planId, k -> new ArrayList<>())
                                .addAll(outputTableNames);
                    }
                }
            } // CDC_PIPELINE表 单行记录处理完成
        }


        List<RulePlan> result = new ArrayList<>();
        for (Map.Entry<String, List<PlanNodeSpec>> e : planNodesByPlanId.entrySet()) {
            RulePlan plan = new RulePlan();
            plan.setPlanId(e.getKey());
            plan.setOutputTableNames(distinctPreserveOrder(
                    outputTableNameByPlanId.getOrDefault(e.getKey(), Collections.emptyList())
            ));
            plan.setNodes(e.getValue());
            result.add(plan);
        }
        return result;
    }

    private static Map<String, Object> parseJsonMap(String json) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            return new HashMap<>();
        }
        return OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
    }

    private static String trim(String s) {
        return s == null ? "" : s.trim();
    }


    // 用于解析sink table的输出表名
    @SuppressWarnings("unchecked")
    private List<String> resolveSinkTableOutputName(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return Collections.emptyList();
        }

        Object targetObj = params.get("target");
        if (!(targetObj instanceof List)) {
            return Collections.emptyList();
        }

        List<String> outputTableNames = new ArrayList<>();
        for (Object item : (List<?>) targetObj) {
            if (!(item instanceof Map)) {
                continue;
            }

            Map<String, Object> targetMap = (Map<String, Object>) item;
            Object tableNameObj = targetMap.get("table_name");
            if (tableNameObj == null) {
                continue;
            }

            String tableName = String.valueOf(tableNameObj).trim();
            if (!tableName.isEmpty()) {
                outputTableNames.add(tableName);
            }
        }

        return distinctPreserveOrder(outputTableNames);
    }

    private List<String> distinctPreserveOrder(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> result = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String value : values) {
            String v = trim(value);
            if (!v.isEmpty() && seen.add(v)) {
                result.add(v);
            }
        }
        return result;
    }



    public String resolveClientIdByDataSourceId(String dataSourceId) {
        String key = trim(dataSourceId);
        if (key.isEmpty()) {
            throw new IllegalArgumentException("dataSourceId 不能为空");
        }

        SimpleClientInfo info = activeClientCache.requireByDataSourceId(key);
        return info.getClientId();
    }


    public SimpleClientInfo requireActiveClientInfoByDataSourceId(String dataSourceId) {
        String key = trim(dataSourceId);
        if (key.isEmpty()) {
            throw new IllegalArgumentException("dataSourceId 不能为空");
        }

        return activeClientCache.requireByDataSourceId(key);
    }


    public Map<String, SimpleClientInfo> getActiveClientByDataSourceId() {
        return activeClientCache.byDataSourceIdView();
    }

    public Map<String, SimpleClientInfo> getActiveClientByClientId() {
        return activeClientCache.byClientIdView();
    }

    /**
     * 返回配置库 DataSource，供运行态仓储类复用。
     */
    public DataSource getConfigDataSource() {
        // 配置库连接应在 @PostConstruct 的 initConnection() 中初始化完成
        if (dataSource == null) {
            throw new IllegalStateException("config dataSource is not initialized");
        }
        return dataSource;
    }






//    // 读取 目标库数据源 for KafkaPipeline
//    public TargetDataSourceConfigRegistry getTargetDataSourceConfigRegistry(BusinessDataSourceConfigRegistry businessDataSourceConfigRegistry) throws Exception {
//        // 当前客户端 使用到的目标库数据源id
//        Set<String> targetDataSourceIdSet = collectTargetDataSourceIds(businessDataSourceConfigRegistry.getBusinessDataSourceConfigList());
//        return getTargetDataSourceConfig(targetDataSourceIdSet);
//    }

}
