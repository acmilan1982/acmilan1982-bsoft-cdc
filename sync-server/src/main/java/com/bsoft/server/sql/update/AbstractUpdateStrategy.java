package com.bsoft.server.sql.update;


import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.constants.CdcColumnConstants;
import com.bsoft.common.constants.CdcSqlConstants;

import com.bsoft.common.enums.DatabaseType;
import com.bsoft.server.sql.adapter.DataSourceHolder;
import com.bsoft.server.sql.adapter.DatabaseAdapter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

// 更新策略基类，封装更新执行流程，并向具体数据库策略传递日期字段单位
public abstract class AbstractUpdateStrategy  implements UpdateStrategy {

    private static final Logger logger = LogManager.getLogger(AbstractUpdateStrategy.class);

    protected DataSourceHolder dataSourceHolder;

    protected DatabaseAdapter adapter;

    public AbstractUpdateStrategy() {
    }

    protected AbstractUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        this.dataSourceHolder = new DataSourceHolder(targetDataSourceConfig) ;
    }

    // Oracle、MySQL 使用本类的 JDBC 批量更新实现。
    // Doris 已在自己的策略类中重写该方法及批量更新入口。
    @Override
    public boolean supportsBatch() {
        // 未初始化时不声明支持批量执行。
        if (dataSourceHolder == null
                || dataSourceHolder.getTargetDataSourceConfig() == null) {
            return false;
        }

        // 本类的 JDBC 实现仅适用于 Oracle 和 MySQL。
        DatabaseType databaseType = dataSourceHolder
                .getTargetDataSourceConfig().getDataSourceType();
        return databaseType == DatabaseType.ORACLE
                || databaseType == DatabaseType.MYSQL;
    }


    // 在一个 JDBC 事务中按消息顺序执行更新。
// 先确认目标记录存在，再执行连续 SQL 分组，全部成功后统一提交。
    @Override
    public void executeBatchUpdate(String tableName,
                                   List<KafkaPayload> payloads,
                                   List<ColumnInfo> allColumns) throws Exception {
        // 校验批次，空批次无需访问数据库。
        Objects.requireNonNull(payloads, "更新批次不能为 null");
        if (payloads.isEmpty()) {
            return;
        }

        // 校验目标表及策略初始化状态。
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("更新的目标表名不能为空");
        }
        if (!supportsBatch() || adapter == null) {
            throw new IllegalStateException(
                    "当前策略未初始化或不支持 JDBC 批量更新");
        }

        // 防止其他子类仅覆盖能力声明后意外进入本 JDBC 实现。
        DatabaseType databaseType = dataSourceHolder
                .getTargetDataSourceConfig().getDataSourceType();
        if (databaseType != DatabaseType.ORACLE
                && databaseType != DatabaseType.MYSQL) {
            throw new UnsupportedOperationException(
                    "当前 JDBC 批量更新入口不支持: " + databaseType);
        }

        // 按目标字段准备每条更新的数据、日期元数据及 SQL。
        // 保持输入顺序，不修改原始消息，不丢弃无效消息。
        List<JdbcUpdateRow> rows =
                prepareJdbcUpdateRows(tableName, payloads, allColumns);

        // 每条输入消息都必须对应一条待执行更新。
        if (rows == null || rows.size() != payloads.size()) {
            throw new IllegalStateException("更新数据准备结果数量与输入数量不一致");
        }

        // 当前 500 条业务批次共用一个连接和事务。
        try (Connection connection = dataSourceHolder.getDataSource().getConnection()) {
            // 禁止每条命令自动提交。
            connection.setAutoCommit(false);

            try {
                // 检查并锁定目标记录，任一主键不存在时整批失败。
                // 重复主键只需检查一次，但后续更新仍逐条按顺序执行。
                lockExistingUpdateRows(connection, tableName, rows);

                // 仅合并连续相同 SQL 的消息，避免改变同一主键的更新顺序。
                int groupStart = 0;
                while (groupStart < rows.size()) {
                    // 当前分组使用第一条记录的 SQL。
                    String currentSql = rows.get(groupStart).sql;
                    int groupEnd = groupStart + 1;

                    // SQL 改变时结束当前分组。
                    while (groupEnd < rows.size()
                            && currentSql.equals(rows.get(groupEnd).sql)) {
                        groupEnd++;
                    }

                    // 同一 SQL 使用 PreparedStatement.addBatch 和 executeBatch。
                    // 每条记录绑定自己的日期单位，组内不提交事务。
                    executeJdbcUpdateGroup(
                            connection,
                            currentSql,
                            rows.subList(groupStart, groupEnd));

                    // 前一组执行完成后，再执行下一组。
                    groupStart = groupEnd;
                }

                // 所有分组执行成功后，统一提交整个业务批次。
                connection.commit();
            } catch (Exception exception) {
                // 校验、绑定、执行或提交异常时尝试回滚。
                try {
                    connection.rollback();
                } catch (Exception rollbackException) {
                    // 保留原始异常，附加回滚失败原因。
                    exception.addSuppressed(rollbackException);
                }

                // 上层负责逐条记录整批失败并推进 offset。
                throw exception;
            }
        }
    }
    // 根据消息执行更新，SQL 构建和参数绑定使用同一份日期单位映射。
    @Override
    public void executeUpdate(String tableName, KafkaPayload payload) throws Exception {
        Map<String, Object> afterData = payload.getAfterData();
        // 被更新的字段列表
        List<String> keys = new ArrayList<>(afterData.keySet());
        Set<String> primaryKeySet = payload.getPrimaryKeySet();


        // 用源表的主键更新，可能存在以下问题:1.源表没有主键 2.多表合并，主键冲突，因此统一用ROWID来更新
        primaryKeySet.clear();
        primaryKeySet.add(CdcColumnConstants.CDC_ROWID_COLUMN_LOWER);



        // 过滤掉主键字段
        List<String> updateKeys = keys.stream()
                .filter(key -> !primaryKeySet.contains(key))
                .collect(Collectors.toList());

        // 检查是否有可更新的字段
        if (updateKeys.isEmpty()) {
            logger.warn("⚠️ No update columns in afterData for table: {}", tableName);
            return;
        }

        // 添加CDC字段
        keys.add("cdc_op");
        keys.add("cdc_update_time");
        afterData.put("cdc_op", payload.getOp());
        afterData.put("cdc_update_time", LocalDateTime.now());

        // 复制日期字段集合，补充 CDC 字段时不修改原消息元数据。
        Set<String> dateFieldSet = new HashSet<>(payload.getDateFieldSet());
        appendCdcTimeFields(dateFieldSet);

       // 获取消息中的明确单位，没有源日期字段时允许为空 Map。
        Map<String, String> dateFieldUnits = payload.getDateFieldUnits();

        // 根据日期字段单位构建更新 SQL，CDC 时间字段使用秒级格式。
        String sql = buildUpdateSql(
                tableName,
                keys,
                primaryKeySet,
                dateFieldSet,
                dateFieldUnits
        );

        // 执行更新
        try (Connection conn = dataSourceHolder.getDataSource().getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

// 使用与 SQL 构建相同的日期元数据绑定 SET 和 WHERE 参数。
            setParameters(
                    ps,
                    afterData,
                    keys,
                    primaryKeySet,
                    dateFieldSet,
                    dateFieldUnits
            );

            // 执行更新
            int affectedRows = ps.executeUpdate();

            if (affectedRows == 0) {
                logger.warn("⚠️ No rows affected for table: {}, primaryKeys: {}",
                        tableName, primaryKeySet);
            } else {
                logger.debug("✅ Updated {} rows in table: {}", affectedRows, tableName);
            }
        }catch (Exception e) {
            e.printStackTrace();
            throw  e;
        }
    }



    /**
     * 校验更新操作的必要参数
     * @param tableName 表名
     * @param keys 所有字段列表
     * @param primaryKeySet 主键字段集合
     * @throws IllegalArgumentException 参数不合法时抛出异常
     */
    protected void validateUpdateParameters(String tableName, List<String> keys, Set<String> primaryKeySet) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName is empty");
        }

        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("keys is empty for table: " + tableName);
        }

        if (primaryKeySet == null || primaryKeySet.isEmpty()) {
            throw new IllegalArgumentException("primaryKeys is empty, unsafe to update: " + tableName);
        }
    }

    // 将服务端生成的更新时间加入日期字段集合。
    private void appendCdcTimeFields(Set<String> dateFieldSet) {
        // cdc_update_time 使用日期对象，不依赖源字段单位。
        dateFieldSet.add("cdc_update_time");
    }

    // 以目标字段为准准备 JDBC 更新数据，保持消息顺序。
// 忽略多余源字段，缺失业务字段补 null，仅维护约定的 CDC 更新字段。
    private List<JdbcUpdateRow> prepareJdbcUpdateRows(
            String tableName,
            List<KafkaPayload> payloads,
            List<ColumnInfo> allColumns) {
        // 必须提供目标字段元数据，不能根据某个源表推测目标结构。
        if (allColumns == null || allColumns.isEmpty()) {
            throw new IllegalArgumentException("目标字段列表不能为空");
        }

        // 建立规范字段名到实际目标字段名的映射，并保留目标字段顺序。
        Map<String, String> targetColumns = new LinkedHashMap<>();
        for (ColumnInfo column : allColumns) {
            // 拒绝不完整的字段元数据。
            if (column == null || column.getColumnName() == null
                    || column.getColumnName().trim().isEmpty()) {
                throw new IllegalArgumentException("目标字段元数据存在空字段名");
            }

            // 实际字段名用于 SQL 和绑定，小写名称用于消息字段匹配。
            String actualName = column.getColumnName().trim();
            String normalizedName = actualName.toLowerCase(Locale.ROOT);
            if (targetColumns.containsKey(normalizedName)) {
                throw new IllegalArgumentException(
                        "目标字段存在大小写重复: " + actualName);
            }
            targetColumns.put(normalizedName, actualName);
        }

        // 检查更新定位和 CDC 维护必需字段。
        for (String required : Arrays.asList(
                "cdc_rowid", "cdc_op", "cdc_update_time")) {
            if (!targetColumns.containsKey(required)) {
                throw new IllegalArgumentException(
                        "目标表缺少更新必需字段: " + required);
            }
        }

        // 以下字段保留目标记录原值，不参与本次 SET 赋值。
        Set<String> preservedFields = new HashSet<>(Arrays.asList(
                "cdc_dml_time",
                "cdc_kafka_enqueue_time",
                "cdc_insert_time",
                "cdc_delete_time",
                "cdc_delete_flag"));

        // 当前业务批次共用服务端本地更新时间。
        LocalDateTime updateTime = LocalDateTime.now();
        List<JdbcUpdateRow> rows = new ArrayList<>(payloads.size());

        // 按输入顺序逐条准备，不根据主键或字段结构重新排序。
        for (KafkaPayload payload : payloads) {
            // 仅接受更新消息。
            if (payload == null || !"u".equals(payload.getOp())) {
                throw new IllegalArgumentException("更新批次包含空消息或非更新消息");
            }

            // 复制并规范化原始数据和日期单位字段名。
            Map<String, Object> afterData =
                    normalizeJdbcFieldMap(payload.getAfterData(), "after");
            Map<String, String> sourceUnits =
                    normalizeJdbcFieldMap(payload.getDateFieldUnits(), "dateFieldUnits");

            // 主键必须提供，不允许通过补 null 生成。
            Object rowIdValue = afterData.get("cdc_rowid");
            if (!(rowIdValue instanceof String)
                    || ((String) rowIdValue).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "更新消息必须提供非空字符串 cdc_rowid");
            }
            String rowId = (String) rowIdValue;

            // 复制源日期字段集合，不修改原始元数据。
            Set<String> sourceDateFields = new HashSet<>();
            Objects.requireNonNull(
                    payload.getDateFieldSet(), "dateFieldSet 不能为 null");
            for (String fieldName : payload.getDateFieldSet()) {
                // 日期字段名必须有效。
                if (fieldName == null || fieldName.trim().isEmpty()) {
                    throw new IllegalArgumentException("日期字段名不能为空");
                }
                sourceDateFields.add(fieldName.toLowerCase(Locale.ROOT));
            }

            // 每条记录使用独立集合，避免不同源表的日期单位互相影响。
            Map<String, Object> data = new LinkedHashMap<>();
            List<String> keys = new ArrayList<>();
            Set<String> primaryKeys = new LinkedHashSet<>();
            Set<String> dateFields = new HashSet<>();
            Map<String, String> dateFieldUnits = new LinkedHashMap<>();

            // 定位字段使用目标元数据中的实际名称。
            primaryKeys.add(targetColumns.get("cdc_rowid"));

            // 只遍历目标字段，多余源字段不会进入 SQL 或日期转换。
            for (Map.Entry<String, String> column : targetColumns.entrySet()) {
                String normalizedName = column.getKey();
                String actualName = column.getValue();

                // 保留不属于本次更新范围的 CDC 字段。
                if (preservedFields.contains(normalizedName)) {
                    continue;
                }

                // 原始主键保持不变，CDC 操作和更新时间由服务端确定。
                Object value;
                if ("cdc_rowid".equals(normalizedName)) {
                    value = rowId;
                } else if ("cdc_op".equals(normalizedName)) {
                    value = "u";
                } else if ("cdc_update_time".equals(normalizedName)) {
                    value = updateTime;
                    dateFields.add(actualName);
                } else {
                    // 显式 null 和消息中缺失的业务字段均写入 SQL NULL。
                    value = afterData.get(normalizedName);

                    // 仅非空源日期值需要日期转换和有效单位。
                    if (value != null && sourceDateFields.contains(normalizedName)) {
                        String unit = sourceUnits.get(normalizedName);
                        if (!"ms".equals(unit)
                                && !"us".equals(unit)
                                && !"ns".equals(unit)) {
                            throw new IllegalArgumentException(
                                    "日期字段缺少有效时间单位，字段="
                                            + actualName + "，单位=" + unit);
                        }

                        // 参数绑定识别实际字段名，单位查询使用小写名称。
                        dateFields.add(actualName);
                        dateFieldUnits.put(normalizedName, unit);
                    }
                }

                // 按目标字段顺序保存，供 SQL 构建和参数绑定共同使用。
                data.put(actualName, value);
                keys.add(actualName);
            }

            // 使用具体数据库现有的明确单位 SQL 构建逻辑。
            // null 日期不加入转换集合，直接通过普通占位符绑定 SQL NULL。
            String sql = buildUpdateSql(
                    tableName, keys, primaryKeys, dateFields, dateFieldUnits);

            // 保存本条记录，不覆盖或去重相同主键的后续更新。
            rows.add(new JdbcUpdateRow(
                    sql,
                    rowId,
                    data,
                    keys,
                    primaryKeys,
                    dateFields,
                    dateFieldUnits));
        }

        // 返回与输入消息一一对应、顺序一致的准备结果。
        return rows;
    }

    // 在更新事务内检查并锁定目标记录，禁止把不存在的目标记录视为更新成功。
// 相同主键只查询一次，原始更新列表保持不变，后续仍逐条按顺序执行。
    private void lockExistingUpdateRows(Connection connection,
                                        String tableName,
                                        List<JdbcUpdateRow> rows) throws SQLException {
        // 空列表无需查询。
        if (rows.isEmpty()) {
            return;
        }

        // 必须使用更新事务的同一连接，确保锁保持到提交或回滚。
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("更新存在性检查必须在显式事务中执行");
        }

        // 使用准备阶段确定的目标定位字段名称。
        Set<String> primaryKeys = rows.get(0).primaryKeys;
        if (primaryKeys.size() != 1) {
            throw new IllegalStateException("批量更新必须且只能使用 cdc_rowid 定位");
        }
        String primaryKeyColumn = primaryKeys.iterator().next();
        if (!"cdc_rowid".equalsIgnoreCase(primaryKeyColumn)) {
            throw new IllegalStateException("批量更新定位字段必须为 cdc_rowid");
        }

        // 去重并排序仅用于存在性检查，不改变更新消息的执行顺序。
        Set<String> uniqueRowIds = new TreeSet<>();
        for (JdbcUpdateRow row : rows) {
            // 所有记录必须使用同一个目标定位字段。
            if (!primaryKeys.equals(row.primaryKeys)) {
                throw new IllegalStateException("更新批次中的定位字段不一致");
            }
            uniqueRowIds.add(row.rowId);
        }
        List<String> rowIds = new ArrayList<>(uniqueRowIds);

        // 每次查询最多携带 500 个主键，限制 IN 参数数量。
        // 这是检查查询的分段大小，不改变更新业务批次或事务边界。
        int lookupBatchSize = 500;
        for (int startIndex = 0; startIndex < rowIds.size();
             startIndex += lookupBatchSize) {
            // 取本次需要检查的主键范围。
            int endIndex = Math.min(startIndex + lookupBatchSize, rowIds.size());
            List<String> lookupRowIds = rowIds.subList(startIndex, endIndex);

            // 主键值使用占位符，不拼接到 SQL 中。
            String placeholders = String.join(
                    ", ", Collections.nCopies(lookupRowIds.size(), "?"));

            // 沿用现有策略的表名使用方式，列名来自目标字段元数据。
            // FOR UPDATE 锁定查到的记录，避免检查后被其他事务删除。
            String sql = String.format(
                    "SELECT %s FROM %s WHERE %s IN (%s) ORDER BY %s FOR UPDATE",
                    primaryKeyColumn,
                    tableName,
                    primaryKeyColumn,
                    placeholders,
                    primaryKeyColumn);

            // 保存待确认的主键，同时用于发现目标表中的重复行身份。
            Set<String> remainingRowIds = new HashSet<>(lookupRowIds);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                // 按占位符顺序绑定完整主键值，不裁剪或转换大小写。
                for (int parameterIndex = 0;
                     parameterIndex < lookupRowIds.size();
                     parameterIndex++) {
                    statement.setString(
                            parameterIndex + 1, lookupRowIds.get(parameterIndex));
                }

                // 查询和更新使用同一个事务连接。
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        // 读取目标记录的实际行身份。
                        String foundRowId = resultSet.getString(1);

                        // 重复返回同一主键或返回非预期身份均明确失败。
                        if (!remainingRowIds.remove(foundRowId)) {
                            throw new SQLException(
                                    "目标表存在重复或不匹配的 cdc_rowid，table="
                                            + tableName);
                        }
                    }
                }
            }

            // 有主键未找到时，在执行任何 UPDATE 前终止整个批次。
            if (!remainingRowIds.isEmpty()) {
                throw new SQLException(
                        "批量更新的目标记录不存在，table=" + tableName
                                + "，当前检查分组缺失数量=" + remainingRowIds.size());
            }
        }

        // 此处不提交或回滚，记录锁由外层更新事务统一释放。
    }

    // 使用同一个 PreparedStatement 按顺序执行连续相同 SQL 的更新。
// 每条记录使用自己的日期元数据，事务由外层统一管理。
    private void executeJdbcUpdateGroup(Connection connection,
                                        String sql,
                                        List<JdbcUpdateRow> rows) throws SQLException {
        // 空分组无需执行。
        if (rows.isEmpty()) {
            return;
        }

        // 必须沿用外层事务，禁止分组自动提交。
        if (connection.getAutoCommit()) {
            throw new IllegalStateException("批量更新必须在显式事务中执行");
        }

        // 同一个 SQL 分组共用一个 PreparedStatement。
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            // 按原消息顺序绑定，不按主键去重或重新排序。
            for (JdbcUpdateRow row : rows) {
                // 防止不同 SQL 的记录误入同一 JDBC 分组。
                if (!sql.equals(row.sql)) {
                    throw new IllegalArgumentException("更新分组包含不同 SQL");
                }

                // 清除上一条参数，避免残留绑定影响当前记录。
                statement.clearParameters();

                // 调用具体数据库的明确单位绑定入口。
                // SQL 和绑定共用当前记录的字段顺序、日期集合及单位映射。
                setParameters(
                        statement,
                        row.data,
                        row.keys,
                        row.primaryKeys,
                        row.dateFields,
                        row.dateFieldUnits);

                // 当前参数加入批次，相同主键的多条更新仍逐条保留。
                statement.addBatch();
            }

            // 执行整个分组，异常直接向外传播，由外层回滚整个业务批次。
            int[] results = statement.executeBatch();

            // 每条命令必须有对应结果，否则不能确认整个分组成功。
            if (results == null || results.length != rows.size()) {
                throw new SQLException(
                        "批量更新返回结果数量与提交数量不一致，submittedRows="
                                + rows.size()
                                + "，resultCount="
                                + (results == null ? "null" : results.length));
            }

            // 按输入命令顺序检查返回结果。
            for (int resultIndex = 0; resultIndex < results.length; resultIndex++) {
                int affectedRows = results[resultIndex];

                // 驱动确认执行成功但未提供影响行数时，接受其成功状态。
                if (affectedRows == Statement.SUCCESS_NO_INFO) {
                    continue;
                }

                // EXECUTE_FAILED 及其他未知负数都视为失败。
                if (affectedRows < 0) {
                    throw new SQLException(
                            "批量更新命令失败，分组索引="
                                    + resultIndex + "，返回值=" + affectedRows);
                }

                // cdc_rowid 应定位唯一记录，多行受影响说明目标行身份异常。
                if (affectedRows > 1) {
                    throw new SQLException(
                            "单条更新影响多条目标记录，分组索引="
                                    + resultIndex + "，affectedRows=" + affectedRows);
                }

                // 接受零行结果：此前已在同一事务内确认并锁定目标记录。
                // MySQL 写入相同值时可能报告零条实际变更。
            }
        }

        // 不在分组内提交，后续分组失败时仍可回滚此前执行的更新。
    }
    // 复制字段映射并将字段名统一为小写，拒绝大小写重复字段。
    private <T> Map<String, T> normalizeJdbcFieldMap(
            Map<String, T> source,
            String description) {
        // 协议集合必须存在，允许内容为空。
        Objects.requireNonNull(source, description + " 不能为 null");

        // 创建独立副本，不修改原始消息。
        Map<String, T> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, T> entry : source.entrySet()) {
            // 空字段名不能参与匹配。
            String fieldName = entry.getKey();
            if (fieldName == null || fieldName.trim().isEmpty()) {
                throw new IllegalArgumentException(description + " 包含空字段名");
            }

            // 只统一大小写，不改变字段名中的其他字符。
            String normalizedName = fieldName.toLowerCase(Locale.ROOT);
            if (normalized.containsKey(normalizedName)) {
                throw new IllegalArgumentException(
                        description + " 存在大小写重复字段: " + fieldName);
            }

            // containsKey 检查能够识别原值为 null 的重复字段。
            normalized.put(normalizedName, entry.getValue());
        }

        // 返回可独立使用的字段映射。
        return normalized;
    }
    // 保存一条 JDBC 更新所需的数据及日期元数据，仅用于当前批次执行。
    private static final class JdbcUpdateRow {
        // 当前记录对应的 SQL，用于连续相同 SQL 分组。
        private final String sql;

        // 原始行身份，用于事务内检查目标记录是否存在。
        private final String rowId;

        // 使用目标字段名称的数据，包含补齐的 null。
        private final Map<String, Object> data;

        // SQL 和参数绑定共用的字段顺序。
        private final List<String> keys;

        // 定位字段集合，固定只包含目标 cdc_rowid 字段。
        private final Set<String> primaryKeys;

        // 本条记录需要进行日期格式化的字段。
        private final Set<String> dateFields;

        // 本条记录实际使用的明确日期单位。
        private final Map<String, String> dateFieldUnits;

        // 保存准备完成的独立数据，不引用原始消息中的集合。
        private JdbcUpdateRow(String sql,
                              String rowId,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeys,
                              Set<String> dateFields,
                              Map<String, String> dateFieldUnits) {
            // 所有参数均由准备方法构造，后续执行阶段只读取。
            this.sql = sql;
            this.rowId = rowId;
            this.data = data;
            this.keys = keys;
            this.primaryKeys = primaryKeys;
            this.dateFields = dateFields;
            this.dateFieldUnits = dateFieldUnits;
        }
    }






}
