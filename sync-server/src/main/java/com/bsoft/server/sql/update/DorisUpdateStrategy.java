package com.bsoft.server.sql.update;

import com.bsoft.common.bean.ColumnInfo;
import com.bsoft.common.bean.KafkaPayload;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.DorisTableModel;
import com.bsoft.server.sql.doris.DorisCustomColumnMappingSupport;
import com.bsoft.server.sql.doris.http.DorisStreamLoadHelper;
import com.bsoft.server.sql.doris.http.StreamLoadResult;
import com.bsoft.server.sql.factory.DatabaseAdapterFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// Doris UNIQUE KEY 更新策略，本轮仅处理已开启 Merge-on-Write 的 HTTP 更新路径。
// 以目标字段为准补齐业务列，只更新已有 cdc_rowid 对应的记录。
public class DorisUpdateStrategy extends AbstractUpdateStrategy {

    // 记录请求处理结果，不打印待更新的业务字段值。
    private static final Logger logger =
            LogManager.getLogger(DorisUpdateStrategy.class);

    // 本项目固定使用 cdc_rowid 定位目标记录。
    private static final String ROW_ID = "cdc_rowid";

    // 本次更新维护的 CDC 字段。
    private static final String OP = "cdc_op";
    private static final String UPDATE_TIME = "cdc_update_time";

    // 以下 CDC 字段不参与本次更新，保留目标记录中的原值。
    private static final Set<String> PRESERVED_CDC_FIELDS =
            new HashSet<>(Arrays.asList(
                    "cdc_dml_time",
                    "cdc_kafka_enqueue_time",
                    "cdc_insert_time",
                    "cdc_delete_time",
                    "cdc_delete_flag"
            ));

    // 复用已有 Stream Load 请求、序列化及日期转换能力。
    private DorisStreamLoadHelper streamLoadHelper;

    // 保留工厂中的无参构造入口，未初始化时禁止执行更新。
    public DorisUpdateStrategy() {
        // 此入口不创建数据源，实际执行必须使用已初始化的策略。
        super();
    }

    // 根据目标库配置初始化更新策略。
    public DorisUpdateStrategy(TargetDataSourceConfig targetDataSourceConfig) {
        // 沿用项目现有的数据源初始化方式。
        super(Objects.requireNonNull(
                targetDataSourceConfig,
                "targetDataSourceConfig 不能为 null"
        ));

        // 使用父类适配器字段，避免子类声明同名字段。
        this.adapter =
                DatabaseAdapterFactory.createAdapter(targetDataSourceConfig);
        this.streamLoadHelper =
                new DorisStreamLoadHelper(targetDataSourceConfig, this.adapter);
    }

    // 原单条入口没有目标字段元数据，不能可靠完成过滤和补齐。
    @Override
    public void executeUpdate(String tableName,
                              KafkaPayload payload) throws Exception {
        // 明确拒绝旧调用，避免退回仅更新 after 已出现字段的处理方式。
        throw new IllegalStateException(
                "Doris 更新必须传入目标字段列表，请使用 "
                        + "executeUpdate(tableName, payload, allColumns)"
        );
    }

    // 单条更新复用批量入口，确保处理规则完全一致。
    @Override
    public void executeUpdate(String tableName,
                              KafkaPayload payload,
                              List<ColumnInfo> allColumns) throws Exception {
        // 空 payload 直接失败，不记录为成功。
        Objects.requireNonNull(payload, "payload 不能为 null");

        // 单条记录同样经过字段补齐、单位转换及结果校验。
        executeBatchUpdate(
                tableName,
                Collections.singletonList(payload),
                allColumns
        );
    }

    // 一次 HTTP 请求更新一批不同 cdc_rowid 的记录。
    @Override
    public void executeBatchUpdate(String tableName,
                                   List<KafkaPayload> payloads,
                                   List<ColumnInfo> allColumns) throws Exception {
        // 校验调用参数，空批次无需请求 Doris。
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("更新的目标表名不能为空");
        }
        Objects.requireNonNull(payloads, "payloads 不能为 null");
        if (payloads.isEmpty()) {
            return;
        }

        // 未初始化的策略不能进入任何写入流程。
        if (this.streamLoadHelper == null || this.dataSourceHolder == null) {
            throw new IllegalStateException("Doris 更新策略尚未初始化");
        }
        TargetDataSourceConfig targetConfig =
                this.dataSourceHolder.getTargetDataSourceConfig();
        Objects.requireNonNull(
                targetConfig,
                "Doris 更新策略缺少目标库配置"
        );

        // DUPLICATE 表明确跳过，其他未确认的模型不允许执行更新。
        DorisTableModel tableModel = targetConfig.getTableModel(tableName);
        if (tableModel == DorisTableModel.DUPLICATE) {
            logger.info(
                    "Doris DUPLICATE 表跳过 UPDATE，未执行实际写库，table={}, rows={}",
                    tableName,
                    payloads.size()
            );
            return;
        }
        if (tableModel != DorisTableModel.UNIQUE) {
            throw new IllegalStateException(
                    "仅允许更新 Doris UNIQUE 表，table=" + tableName
                            + "，tableModel=" + tableModel
            );
        }

        // 根据目标元数据建立大小写映射，不在更新时重新查询数据库。
        Map<String, String> targetColumns =
                buildTargetColumnMap(allColumns);

        // 解析现有的自定义字段映射，映射方向为目标字段到源字段。
        Map<String, String> customMapping =
                DorisCustomColumnMappingSupport.resolveNormalizedCustomMapping(
                        targetConfig,
                        tableName
                );

        // 确定本次统一写入的目标列，排除需要保留的 CDC 字段。
        List<String> writeColumns =
                buildWriteColumns(targetColumns, customMapping);

        // 仅应用本次实际写入的业务列映射，不允许映射改写系统定位字段。
        Map<String, String> effectiveMapping = new LinkedHashMap<>();
        for (String column : writeColumns) {
            String normalizedColumn = column.toLowerCase(Locale.ROOT);
            if (isManagedCdcField(normalizedColumn)) {
                continue;
            }
            if (customMapping.containsKey(normalizedColumn)) {
                effectiveMapping.put(
                        normalizedColumn,
                        customMapping.get(normalizedColumn)
                );
            }
        }

        // 全部记录准备完毕后才发送请求，校验失败时不提交本批数据。
        long startTime = System.currentTimeMillis();
        LocalDateTime updateTime = LocalDateTime.now();
        List<Map<String, Object>> rows =
                new ArrayList<>(payloads.size());
        Set<String> rowIds = new HashSet<>();

        for (KafkaPayload payload : payloads) {
            // 按每条消息自己的日期单位生成完整的本次更新数据。
            Map<String, Object> row = prepareUpdateRow(
                    payload,
                    writeColumns,
                    effectiveMapping,
                    updateTime
            );

            // 不依赖单次导入内部对重复 Key 的排序，上游需要按顺序拆批。
            String rowId = (String) row.get(targetColumns.get(ROW_ID));
            if (!rowIds.add(rowId)) {
                throw new IllegalArgumentException(
                        "同一 Doris 更新批次包含重复 cdc_rowid，"
                                + "请上游按事件顺序拆批，table=" + tableName
                );
            }

            // 保留调用方传入的记录顺序。
            rows.add(row);
        }

        // 复用已设置 ERROR 新 Key 策略和零过滤比例的 HTTP 更新方法。
        String jsonArrayData = this.streamLoadHelper.toJson(rows);
        StreamLoadResult result =
                this.streamLoadHelper.executePartialUpdate(
                        tableName,
                        jsonArrayData,
                        writeColumns
                );

        // 防御性校验返回状态，不把响应异常或过滤记录视为整批成功。
        if (result == null || !result.isSuccess()
                || result.getNumberFilteredRows() != 0) {
            throw new IllegalStateException(
                    "Doris 批量更新失败或结果无法确认，table=" + tableName
                            + "，detail="
                            + (result == null
                            ? "未返回 Stream Load 结果"
                            : result.getResponseJson())
            );
        }

        // 本批 Key 不重复，加载行数应与提交的记录数一致。
        if (result.getNumberLoadedRows() != rows.size()) {
            throw new IllegalStateException(
                    "Doris 更新加载行数不符合预期，table=" + tableName
                            + "，submittedRows=" + rows.size()
                            + "，loadedRows=" + result.getNumberLoadedRows()
            );
        }

        // 只有返回结果通过全部校验后才记录确认成功。
        logger.info(
                "Doris Stream Load 更新成功，table={}, submittedRows={}, "
                        + "loadedRows={}, elapsedMs={}",
                tableName,
                rows.size(),
                result.getNumberLoadedRows(),
                System.currentTimeMillis() - startTime
        );
    }

    // 建立目标字段小写名称到实际名称的映射。
    private Map<String, String> buildTargetColumnMap(
            List<ColumnInfo> allColumns) {
        // 没有目标元数据时禁止推测字段范围。
        if (allColumns == null || allColumns.isEmpty()) {
            throw new IllegalArgumentException("目标字段列表不能为空");
        }

        // 保持元数据中的字段顺序，并拒绝大小写冲突。
        Map<String, String> targetColumns = new LinkedHashMap<>();
        for (ColumnInfo column : allColumns) {
            if (column == null || column.getColumnName() == null
                    || column.getColumnName().trim().isEmpty()) {
                throw new IllegalArgumentException("目标字段元数据存在空字段名");
            }

            // 请求使用目标字段的实际名称，匹配时统一小写。
            String actualName = column.getColumnName().trim();
            String normalizedName = actualName.toLowerCase(Locale.ROOT);
            if (targetColumns.putIfAbsent(normalizedName, actualName) != null) {
                throw new IllegalArgumentException(
                        "目标字段存在大小写重复: " + actualName
                );
            }
        }

        // 本实现必须具有定位字段和约定维护的 CDC 字段。
        for (String required : Arrays.asList(ROW_ID, OP, UPDATE_TIME)) {
            if (!targetColumns.containsKey(required)) {
                throw new IllegalArgumentException(
                        "目标表缺少更新必需字段: " + required
                );
            }
        }

        // 返回独立元数据映射，不修改调用方的字段列表。
        return targetColumns;
    }

    // 确定本批写入列，目标业务字段均参与更新，缺失值稍后补 null。
    private List<String> buildWriteColumns(
            Map<String, String> targetColumns,
            Map<String, String> customMapping) {
        // 定位列固定放在请求字段列表首位。
        List<String> writeColumns = new ArrayList<>();
        writeColumns.add(targetColumns.get(ROW_ID));

        // 遍历目标字段，保留不应由本次更新覆盖的 CDC 字段。
        for (Map.Entry<String, String> entry : targetColumns.entrySet()) {
            String normalizedName = entry.getKey();
            if (ROW_ID.equals(normalizedName)
                    || PRESERVED_CDC_FIELDS.contains(normalizedName)) {
                continue;
            }

            // 映射自保留 CDC 字段的业务列同样保留，避免因本次无值被清空。
            String mappedSource = customMapping.get(normalizedName);
            if (!isManagedCdcField(normalizedName)
                    && mappedSource != null
                    && PRESERVED_CDC_FIELDS.contains(mappedSource)) {
                continue;
            }

            // 其他目标列均写入，after 缺失时按已确认规则补 null。
            writeColumns.add(entry.getValue());
        }

        // 所有记录使用同一字段列表，适配固定列的局部更新请求。
        return writeColumns;
    }

    // 为单条消息生成独立更新行，不修改原始数据或日期元数据。
    private Map<String, Object> prepareUpdateRow(
            KafkaPayload payload,
            List<String> writeColumns,
            Map<String, String> effectiveMapping,
            LocalDateTime updateTime) {
        // 仅接受更新消息，避免把其他操作错误装入更新批次。
        Objects.requireNonNull(payload, "更新批次中不能包含 null payload");
        if (!"u".equalsIgnoreCase(payload.getOp())) {
            throw new IllegalArgumentException("更新批次只能包含 u 操作");
        }

        // 复制并统一 after 字段名，保留显式 null。
        Map<String, Object> afterData =
                normalizeAfterData(payload.getAfterData());

        // cdc_rowid 不允许缺失、为空或被补成 null。
        Object rowId = afterData.get(ROW_ID);
        if (!(rowId instanceof String)
                || ((String) rowId).trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "更新记录必须提供非空字符串 cdc_rowid"
            );
        }

        // 保留目标业务字段，以及自定义映射需要读取的源字段。
        Set<String> requiredSourceFields = new LinkedHashSet<>();
        for (String column : writeColumns) {
            String normalizedColumn = column.toLowerCase(Locale.ROOT);
            if (!isManagedCdcField(normalizedColumn)) {
                requiredSourceFields.add(normalizedColumn);
            }
        }
        requiredSourceFields.addAll(effectiveMapping.values());

        // 无关源字段不进入日期转换，也不进入最终更新请求。
        Map<String, Object> workingData = new LinkedHashMap<>();
        for (String sourceField : requiredSourceFields) {
            if (!PRESERVED_CDC_FIELDS.contains(sourceField)) {
                workingData.put(sourceField, afterData.get(sourceField));
            }
        }

        // CDC 字段由服务端确定，忽略 after 中可能携带的同名值。
        workingData.put(ROW_ID, rowId);
        workingData.put(OP, "u");
        workingData.put(UPDATE_TIME, updateTime);

        // 复制日期字段集合，统一小写后补充服务端更新时间。
        Set<String> sourceDateFields = Objects.requireNonNull(
                payload.getDateFieldSet(),
                "dateFieldSet 不能为 null"
        );
        Set<String> dateFields = new HashSet<>();
        for (String field : sourceDateFields) {
            if (field == null || field.trim().isEmpty()) {
                throw new IllegalArgumentException("日期字段名不能为空");
            }
            dateFields.add(field.toLowerCase(Locale.ROOT));
        }
        dateFields.add(UPDATE_TIME);

        // 复制明确单位映射，供日期转换使用，不猜测缺失单位。
        Map<String, String> dateFieldUnits = new LinkedHashMap<>(
                Objects.requireNonNull(
                        payload.getDateFieldUnits(),
                        "dateFieldUnits 不能为 null"
                )
        );

        // 先按源日期单位转换，再执行字段映射，防止目标别名漏转换。
        Map<String, Object> processedData =
                this.streamLoadHelper.processDataForStreamLoad(
                        workingData,
                        dateFields,
                        dateFieldUnits
                );
        DorisCustomColumnMappingSupport.applyCustomMapping(
                processedData,
                effectiveMapping
        );

        // 以目标字段为准投影，同时补齐缺失业务值并恢复实际大小写。
        Map<String, Object> row = new LinkedHashMap<>();
        for (String column : writeColumns) {
            String normalizedColumn = column.toLowerCase(Locale.ROOT);
            row.put(column, processedData.get(normalizedColumn));
        }

        // 返回仅包含本次写入列的独立更新行。
        return row;
    }

    // 复制 after 数据并规范字段名，不修改 payload 中的原 Map。
    private Map<String, Object> normalizeAfterData(
            Map<String, Object> afterData) {
        // 无数据时明确失败，不按更新成功处理。
        if (afterData == null || afterData.isEmpty()) {
            throw new IllegalArgumentException("更新消息的 after 不能为空");
        }

        // 避免同一逻辑字段因大小写差异出现不同值。
        Map<String, Object> normalizedData = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : afterData.entrySet()) {
            String field = entry.getKey();
            if (field == null || field.trim().isEmpty()) {
                throw new IllegalArgumentException("after 字段名不能为空");
            }

            // 使用 containsKey 判断重复，兼容字段原值为 null 的情况。
            String normalizedField = field.toLowerCase(Locale.ROOT);
            if (normalizedData.containsKey(normalizedField)) {
                throw new IllegalArgumentException(
                        "after 存在大小写重复字段: " + field
                );
            }
            normalizedData.put(normalizedField, entry.getValue());
        }

        // 返回规范化副本，后续处理不会改写原始消息。
        return normalizedData;
    }

    // 判断本次由策略直接控制的定位字段和 CDC 更新字段。
    private boolean isManagedCdcField(String fieldName) {
        // 自定义映射不能覆盖行身份、操作类型或服务端更新时间。
        return ROW_ID.equals(fieldName)
                || OP.equals(fieldName)
                || UPDATE_TIME.equals(fieldName);
    }

    // 显式声明已经实现真正的 HTTP 批量更新入口。
    @Override
    public boolean supportsBatch() {
        // 上游仍需负责构造有序批次，不能跨删除或 DDL 边界随意重排。
        return true;
    }

    // 本轮 HTTP 更新路径不生成 JDBC SQL。
    @Override
    public String buildUpdateSql(String tableName,
                                 List<String> keys,
                                 Set<String> primaryKeySet,
                                 Set<String> dateFieldSet) {
        // 防止误用旧 JDBC 模板时生成空 SQL。
        throw new UnsupportedOperationException(
                "当前 Doris 更新策略使用 HTTP Stream Load"
        );
    }

    // 本轮 HTTP 更新路径不绑定 JDBC 参数。
    @Override
    public void setParameters(PreparedStatement statement,
                              Map<String, Object> data,
                              List<String> keys,
                              Set<String> primaryKeySet,
                              Set<String> dateFieldSet) {
        // 防止未接入的 JDBC 路径被当作正常更新执行。
        throw new UnsupportedOperationException(
                "当前 Doris 更新策略使用 HTTP Stream Load"
        );
    }
}