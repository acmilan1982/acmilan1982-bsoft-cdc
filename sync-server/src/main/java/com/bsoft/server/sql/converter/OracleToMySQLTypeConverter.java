package com.bsoft.server.sql.converter;

/**
 * ClassName: OracleToMySQLTypeConverter
 * Package:   com.bsoft.server.sql.converter
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/29 - 13:57
 */


import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: OracleToMySQLTypeConverter
 * Package:   com.bsoft.server.sql.converter
 * Description: Oracle到MySQL字段类型转换工具类
 *
 * @Author: snoopy
 * @Create: 2025/8/29 - 14:30
 */
public class OracleToMySQLTypeConverter {

    // Oracle到MySQL类型映射表
    private static final Map<String, String> TYPE_MAPPING = new HashMap<>();

    static {
        // 数字类型
        TYPE_MAPPING.put("NUMBER", "DECIMAL");
        TYPE_MAPPING.put("INTEGER", "INT");
        TYPE_MAPPING.put("INT", "INT");
        TYPE_MAPPING.put("SMALLINT", "SMALLINT");
        TYPE_MAPPING.put("FLOAT", "FLOAT");
        TYPE_MAPPING.put("DOUBLE", "DOUBLE");
        TYPE_MAPPING.put("BINARY_DOUBLE", "DOUBLE");
        TYPE_MAPPING.put("BINARY_FLOAT", "FLOAT");

        // 字符串类型
        TYPE_MAPPING.put("VARCHAR2", "VARCHAR");
        TYPE_MAPPING.put("NVARCHAR2", "VARCHAR");
        TYPE_MAPPING.put("CHAR", "CHAR");
        TYPE_MAPPING.put("NCHAR", "CHAR");
        TYPE_MAPPING.put("CLOB", "LONGTEXT");
        TYPE_MAPPING.put("NCLOB", "LONGTEXT");
        TYPE_MAPPING.put("LONG", "LONGTEXT");

        // 日期时间类型
        TYPE_MAPPING.put("DATE", "DATETIME");
        TYPE_MAPPING.put("TIMESTAMP", "TIMESTAMP");
        TYPE_MAPPING.put("TIMESTAMP WITH TIME ZONE", "TIMESTAMP");
        TYPE_MAPPING.put("TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP");

        // 二进制类型
        TYPE_MAPPING.put("RAW", "VARBINARY");
        TYPE_MAPPING.put("LONG RAW", "LONGBLOB");
        TYPE_MAPPING.put("BLOB", "LONGBLOB");
        TYPE_MAPPING.put("BFILE", "LONGBLOB");

        // 其他类型
        TYPE_MAPPING.put("ROWID", "VARCHAR(18)");
        TYPE_MAPPING.put("UROWID", "VARCHAR(4000)");
    }

    // 匹配带参数的数据类型 如: NUMBER(10,2), VARCHAR2(100)
    private static final Pattern TYPE_WITH_PARAMS = Pattern.compile("([A-Za-z_]+)\\s*\\(([^)]+)\\)");

    /**
     * 将Oracle数据类型转换为MySQL数据类型
     * @param oracleType Oracle数据类型
     * @return MySQL数据类型
     */
    public static String convertType(String oracleType) {
        if (oracleType == null || oracleType.trim().isEmpty()) {
            return "TEXT";
        }

        String upperType = oracleType.trim().toUpperCase();

        // 处理带参数的类型
        Matcher matcher = TYPE_WITH_PARAMS.matcher(upperType);
        if (matcher.matches()) {
            String baseType = matcher.group(1);
            String params = matcher.group(2);

            return convertTypeWithParams(baseType, params);
        }

        // 处理简单类型
        return TYPE_MAPPING.getOrDefault(upperType, "TEXT");
    }

    /**
     * 处理带参数的类型转换
     * @param baseType 基础类型
     * @param params 参数部分
     * @return 转换后的MySQL类型
     */
    private static String convertTypeWithParams(String baseType, String params) {
        switch (baseType) {
            case "NUMBER":
                return convertNumberType(params);
            case "VARCHAR2":
            case "NVARCHAR2":
                return "VARCHAR(" + params + ")";
            case "CHAR":
            case "NCHAR":
                return "CHAR(" + params + ")";
            case "RAW":
                return "VARBINARY(" + params + ")";
            case "TIMESTAMP":
                // Oracle TIMESTAMP(6) -> MySQL TIMESTAMP(6)
                return "TIMESTAMP(" + params + ")";
            default:
                String mysqlBaseType = TYPE_MAPPING.get(baseType);
                if (mysqlBaseType != null) {
                    return mysqlBaseType + "(" + params + ")";
                }
                return "TEXT";
        }
    }

    /**
     * 转换Oracle NUMBER类型到MySQL类型
     * @param params NUMBER的参数，如: "10,2" 或 "10"
     * @return MySQL对应类型
     */
    private static String convertNumberType(String params) {
        if (params == null || params.trim().isEmpty()) {
            return "DECIMAL(65,30)"; // MySQL DECIMAL最大精度
        }

        String[] parts = params.split(",");
        if (parts.length == 1) {
            // 只有精度，没有小数位数 NUMBER(10)
            int precision = Integer.parseInt(parts[0].trim());
            if (precision <= 3) {
                return "TINYINT";
            } else if (precision <= 5) {
                return "SMALLINT";
            } else if (precision <= 9) {
                return "INT";
            } else if (precision <= 19) {
                return "BIGINT";
            } else {
                return "DECIMAL(" + precision + ",0)";
            }
        } else if (parts.length == 2) {
            // 有精度和小数位数 NUMBER(10,2)
            int precision = Integer.parseInt(parts[0].trim());
            int scale = Integer.parseInt(parts[1].trim());

            if (scale == 0) {
                // 整数类型
                if (precision <= 3) {
                    return "TINYINT";
                } else if (precision <= 5) {
                    return "SMALLINT";
                } else if (precision <= 9) {
                    return "INT";
                } else if (precision <= 19) {
                    return "BIGINT";
                }
            }

            // MySQL DECIMAL最大精度是65，最大小数位数是30
            precision = Math.min(precision, 65);
            scale = Math.min(scale, 30);
            return "DECIMAL(" + precision + "," + scale + ")";
        }

        return "DECIMAL(10,0)";
    }

    /**
     * 获取默认值转换
     * @param oracleDefaultValue Oracle默认值
     * @param mysqlType MySQL类型
     * @return MySQL默认值
     */
    public static String convertDefaultValue(String oracleDefaultValue, String mysqlType) {
        if (oracleDefaultValue == null || oracleDefaultValue.trim().isEmpty()) {
            return null;
        }

        String defaultValue = oracleDefaultValue.trim();

        // Oracle SYSDATE -> MySQL NOW()
        if ("SYSDATE".equalsIgnoreCase(defaultValue)) {
            return "NOW()";
        }

        // Oracle SYSTIMESTAMP -> MySQL CURRENT_TIMESTAMP
        if ("SYSTIMESTAMP".equalsIgnoreCase(defaultValue)) {
            return "CURRENT_TIMESTAMP";
        }

        // Oracle USER -> MySQL USER()
        if ("USER".equalsIgnoreCase(defaultValue)) {
            return "USER()";
        }

        return defaultValue;
    }
}