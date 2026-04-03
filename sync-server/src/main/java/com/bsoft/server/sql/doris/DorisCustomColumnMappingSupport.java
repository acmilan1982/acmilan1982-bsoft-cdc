package com.bsoft.server.sql.doris;

import com.bsoft.common.bean.DdlBizAttr;
import com.bsoft.common.bean.DorisBizAttr;
import com.bsoft.common.bean.TargetDataSourceConfig;
import com.bsoft.common.enums.DatabaseType;

import java.util.*;

/**
 * Doris custom_column_mapping 公共处理：
 * 1. 解析并规范化 custom_column_mapping
 * 2. 校验重复 key / 循环映射
 * 3. 将映射应用到单条待写入数据
 */
public final class DorisCustomColumnMappingSupport {

    private DorisCustomColumnMappingSupport() {
    }

    /**
     * 内置 CDC 字段白名单。
     * 注意：
     * 1. executeSingleInsert 当前不会显式写入 cdc_delete_time / cdc_delete_flag
     * 2. 但 custom_column_mapping 若引用它们，按 null 处理即可
     * 3. cdc_rowid 暂不纳入这里的处理范围
     */
    private static final Set<String> BUILTIN_CDC_COLUMNS =
            Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
                    "cdc_op",
                    "cdc_dml_time",
                    "cdc_kafka_enqueue_time",
                    "cdc_insert_time",
                    "cdc_update_time",
                    "cdc_delete_time",
                    "cdc_delete_flag"
            )));

    public static Map<String, String> resolveNormalizedCustomMapping(TargetDataSourceConfig targetConfig,
                                                                     String tableName) {
        if (targetConfig == null || targetConfig.getDataSourceType() != DatabaseType.DORIS) {
            return Collections.emptyMap();
        }

        DdlBizAttr ddlBizAttr = targetConfig.getEffectiveDdlBizAttr(tableName);
        if (!(ddlBizAttr instanceof DorisBizAttr)) {
            return Collections.emptyMap();
        }

        DorisBizAttr dorisBizAttr = (DorisBizAttr) ddlBizAttr;
        Map<String, String> raw = dorisBizAttr.getCustomColumnMapping();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> normalized = normalizeMapping(raw);
        validateNoCycle(normalized);
        return normalized;
    }

    /**
     * 将 custom_column_mapping 应用到单条待写入数据。
     * 语义：
     * 1. target 列不存在时新增
     * 2. target 列已存在时覆盖写入
     * 3. source 来自当前数据中的源字段或内置 CDC 字段
     * 4. source 字段当前未出现在数据中时，按 null 处理
     */
    public static void applyCustomMapping(Map<String, Object> rowData,
                                          Map<String, String> normalizedMapping) {
        if (rowData == null || rowData.isEmpty()) {
            return;
        }
        if (normalizedMapping == null || normalizedMapping.isEmpty()) {
            return;
        }

        Map<String, String> actualKeyByLower = new LinkedHashMap<>();
        Map<String, Object> sourceValueByLower = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            String normalizedKey = normalize(entry.getKey());
            actualKeyByLower.put(normalizedKey, entry.getKey());
            sourceValueByLower.put(normalizedKey, entry.getValue());
        }

        for (Map.Entry<String, String> entry : normalizedMapping.entrySet()) {
            String target = entry.getKey();
            String source = entry.getValue();

            Object sourceValue;
            if (sourceValueByLower.containsKey(source)) {
                sourceValue = sourceValueByLower.get(source);
            } else if (BUILTIN_CDC_COLUMNS.contains(source)) {
                // 对 executeSingleInsert 尚未显式写入的数据型 CDC 字段，默认按 null 处理
                sourceValue = null;
            } else {
                // 理论上 DDL 阶段已经校验过；这里保守回退为 null，避免影响单条写入
                sourceValue = null;
            }

            String actualTargetKey = actualKeyByLower.getOrDefault(target, target);
            rowData.put(actualTargetKey, sourceValue);
        }
    }

    public static Set<String> builtinCdcColumnNames() {
        return BUILTIN_CDC_COLUMNS;
    }

    public static Map<String, String> normalizeMapping(Map<String, String> raw) {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            String k = normalize(entry.getKey());
            String v = normalize(entry.getValue());

            if (k.isEmpty() || v.isEmpty()) {
                throw new IllegalArgumentException("custom_column_mapping 存在空 key/value");
            }
            if (out.containsKey(k)) {
                throw new IllegalArgumentException("custom_column_mapping 存在重复 key(忽略大小写): " + k);
            }

            out.put(k, v);
        }
        return out;
    }

    public static void validateNoCycle(Map<String, String> mapping) {
        for (String start : mapping.keySet()) {
            Set<String> seen = new HashSet<>();
            String current = start;

            while (mapping.containsKey(current)) {
                if (!seen.add(current)) {
                    throw new IllegalArgumentException("custom_column_mapping 存在循环映射: " + start);
                }
                current = mapping.get(current);
            }
        }
    }

    public static String normalize(String raw) {
        return raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
    }
}