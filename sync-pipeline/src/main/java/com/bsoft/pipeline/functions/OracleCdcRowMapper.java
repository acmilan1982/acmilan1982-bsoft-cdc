package com.bsoft.pipeline.functions;

import com.bsoft.common.dbmeta.OracleColumnMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;

public class OracleCdcRowMapper {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Row parseKafkaMessage(String value, List<OracleColumnMetadata> columns) throws Exception {
        JsonNode root = OBJECT_MAPPER.readTree(value);
        if (root == null || root.get("op") == null) {
            return null;
        }

        String op = root.get("op").asText();
        if (!"r".equals(op) && !"c".equals(op) && !"u".equals(op) && !"d".equals(op) && !"ddl".equals(op)) {
            return null;
        }
        if ("ddl".equals(op)) {
            return null;
        }

        JsonNode dataNode;
        RowKind rowKind;
        switch (op) {
            case "d":
                dataNode = root.get("before");
                rowKind = RowKind.DELETE;
                break;
            case "u":
                dataNode = root.get("after");
                rowKind = RowKind.UPDATE_AFTER;
                break;
            case "r":
            case "c":
            default:
                dataNode = root.get("after");
                rowKind = RowKind.INSERT;
                break;
        }

        if (dataNode == null || dataNode.isNull()) {
            return null;
        }

        Row row = new Row(rowKind, columns.size());
        for (int i = 0; i < columns.size(); i++) {
            OracleColumnMetadata col = columns.get(i);
            String jsonField = col.getColumnName().toLowerCase(Locale.ROOT);
            JsonNode node = dataNode.path(jsonField);
            row.setField(i, convertByOracleType(node, col));
        }
        return row;
    }

    private static Object convertByOracleType(JsonNode node, OracleColumnMetadata col) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }

        String type = col.getDataType() == null ? "" : col.getDataType().trim().toUpperCase(Locale.ROOT);
        String txt = node.asText();

        try {
            switch (type) {
                case "NUMBER":
                case "DECIMAL":
                case "NUMERIC":
                    Integer p = col.getDataPrecision();
                    Integer s = col.getDataScale();

                    if (txt == null || txt.trim().isEmpty()) {
                        return null;
                    }

                    if (s != null && s > 0) {
                        return new BigDecimal(txt.trim());
                    }
                    if (p == null) {
                        return new BigDecimal(txt.trim());
                    }
                    if (p <= 9) {
                        return Integer.parseInt(txt.trim());
                    }
                    if (p <= 18) {
                        return Long.parseLong(txt.trim());
                    }
                    return new BigDecimal(txt.trim());

                case "FLOAT":
                case "BINARY_FLOAT":
                    return node.isNumber() ? node.floatValue() : Float.parseFloat(txt.trim());

                case "BINARY_DOUBLE":
                    return node.isNumber() ? node.doubleValue() : Double.parseDouble(txt.trim());

                case "DATE":
                case "TIMESTAMP":
                case "TIMESTAMP WITH TIME ZONE":
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                    if (node.isNumber()) {
                        return new Timestamp(node.asLong());
                    }
                    if (txt != null && txt.matches("^\\d+$")) {
                        return new Timestamp(Long.parseLong(txt));
                    }
                    // 不要返回 String，避免和 TIMESTAMP 列类型冲突
                    return null;

                default:
                    return txt;
            }
        } catch (Exception ex) {
            // 兜底：单字段失败不让整条崩
            return txt;
        }
    }
}