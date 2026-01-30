package com.bsoft;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleToMySQLDDLConverter {

    public static String convertOracleToMySQL(String oracleDDL) {
        // 1. 提取表名
        Pattern tableNamePattern = Pattern.compile("CREATE TABLE\\s+\"[^\"]+\"\\.\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
        Matcher tableNameMatcher = tableNamePattern.matcher(oracleDDL);
        String tableName = null;
        if (tableNameMatcher.find()) {
            tableName = tableNameMatcher.group(1);
        } else {
            throw new RuntimeException("未能识别表名");
        }

        // 2. 提取主键字段
        String pkColumn = null;
        Pattern pkPattern = Pattern.compile("PRIMARY KEY\\s*\\(([^\\)]+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher pkMatcher = pkPattern.matcher(oracleDDL);
        if (pkMatcher.find()) {
            pkColumn = pkMatcher.group(1).replaceAll("\"", "").trim().toLowerCase();
        }

        // 3. 提取括号中的字段定义内容
        int left = oracleDDL.indexOf('(');
        int right = oracleDDL.lastIndexOf(')');
        if (left == -1 || right == -1 || right <= left) {
            throw new RuntimeException("未能解析字段定义区段");
        }
        String fieldBlock = oracleDDL.substring(left + 1, right);

        // 4. 处理每一行字段
        List<String> fields = new ArrayList<>();
        for (String rawLine : fieldBlock.split("\n")) {
            String line = rawLine.trim();
            // 跳过空行、约束和SUPPLEMENTAL等
            if (line.isEmpty() || line.startsWith("CONSTRAINT") || line.toUpperCase().contains("PRIMARY KEY") || line.toUpperCase().startsWith("SUPPLEMENTAL")) {
                continue;
            }
            // 去掉行尾逗号
            if (line.endsWith(",")) line = line.substring(0, line.length() - 1).trim();
            // 解析字段
            Matcher fieldMatcher = Pattern.compile("^\"([^\"]+)\"\\s+([A-Z0-9_()]+)(?:\\s+NOT NULL ENABLE)?", Pattern.CASE_INSENSITIVE).matcher(line);
            if (fieldMatcher.find()) {
                String colName = fieldMatcher.group(1).toLowerCase();
                String colType = fieldMatcher.group(2);
                boolean notNull = line.toUpperCase().contains("NOT NULL ENABLE");
                String mysqlType = convertType(colType);
                fields.add("  `" + colName + "` " + mysqlType + (notNull ? " NOT NULL" : ""));
            }
        }

        // 5. 拼接MySQL建表语句
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE `").append(tableName.toLowerCase()).append("` (\n");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i));
            if (i < fields.size() - 1 || pkColumn != null) sb.append(",");
            sb.append("\n");
        }
        if (pkColumn != null) {
            sb.append("  PRIMARY KEY (`").append(pkColumn).append("`)\n");
        }
        sb.append(");");
        return sb.toString();
    }

    private static String convertType(String oracleType) {
        oracleType = oracleType.trim().toUpperCase();
        if (oracleType.startsWith("VARCHAR2")) {
            Matcher m = Pattern.compile("VARCHAR2\\((\\d+)\\)").matcher(oracleType);
            if (m.find()) {
                return "varchar(" + m.group(1) + ")";
            } else {
                return "varchar(255)";
            }
        } else if (oracleType.startsWith("NUMBER")) {
            Matcher m = Pattern.compile("NUMBER\\((\\d+)(,(\\d+))?\\)").matcher(oracleType);
            if (m.find()) {
                if (m.group(3) != null) {
                    return "decimal(" + m.group(1) + "," + m.group(3) + ")";
                } else {
                    int len = Integer.parseInt(m.group(1));
                    if (len <= 9) return "int";
                    if (len <= 18) return "bigint";
                    return "decimal(" + len + ")";
                }
            } else {
                return "int";
            }
        } else if (oracleType.startsWith("DATE")) {
            return "datetime";
        } else if (oracleType.startsWith("CHAR")) {
            Matcher m = Pattern.compile("CHAR\\((\\d+)\\)").matcher(oracleType);
            if (m.find()) {
                return "char(" + m.group(1) + ")";
            } else {
                return "char(1)";
            }
        }
        // 默认类型
        return "varchar(255)";
    }

    // 测试主方法
    public static void main(String[] args) {
        String oracleDDL = "CREATE TABLE \"DEV_EHRVIEW\".\"OPT_FEE\" \n" +
                "   (   \"DCID\" VARCHAR2(64) NOT NULL ENABLE, \n" +
                "    \"EVENT_ID\" VARCHAR2(64), \n" +
                "    \"MPI_ID\" VARCHAR2(64), \n" +
                "    \"EFFECTIVE_TIME\" DATE, \n" +
                "    \"AUTHOR_ORGANIZATION\" VARCHAR2(64), \n" +
                "    \"AUTHOR_ORGANIZATION_TEXT\" VARCHAR2(64), \n" +
                "    \"SOURCE_ID\" VARCHAR2(64), \n" +
                "    \"VERSION_NUMBER\" VARCHAR2(20), \n" +
                "    \"SYSTEM_TIME\" DATE DEFAULT SYSDATE, \n" +
                "    \"JZLSH\" VARCHAR2(50), \n" +
                "    \"FYLSH\" VARCHAR2(50) NOT NULL ENABLE, \n" +
                "    \"TFBZ\" VARCHAR2(5), \n" +
                "    \"TFBZ_CODE\" VARCHAR2(5), \n" +
                "    \"TFBZ_TEXT\" VARCHAR2(50), \n" +
                "    \"JSLY\" VARCHAR2(5), \n" +
                "    \"JSLY_CODE\" VARCHAR2(5), \n" +
                "    \"JSLY_TEXT\" VARCHAR2(50), \n" +
                "    \"FPHM\" VARCHAR2(50), \n" +
                "    \"STFSJ\" DATE, \n" +
                "    \"FYJE\" VARCHAR2(25), \n" +
                "    \"JCF\" VARCHAR2(25), \n" +
                "    \"ZLF\" VARCHAR2(25), \n" +
                "    \"QTF\" VARCHAR2(25), \n" +
                "    \"SSF\" VARCHAR2(25), \n" +
                "    \"FSF\" VARCHAR2(25), \n" +
                "    \"HYF\" VARCHAR2(25), \n" +
                "    \"ZCF\" VARCHAR2(25), \n" +
                "    \"MZZCYF\" VARCHAR2(25), \n" +
                "    \"ZCYF\" VARCHAR2(25), \n" +
                "    \"GHF\" VARCHAR2(25), \n" +
                "    \"XYF\" VARCHAR2(25), \n" +
                "    \"YLFFFS\" VARCHAR2(5), \n" +
                "    \"YLFFFS_CODE\" VARCHAR2(5), \n" +
                "    \"YLFFFS_TEXT\" VARCHAR2(50), \n" +
                "    \"YBZFJE\" VARCHAR2(25), \n" +
                "    \"ZFJE\" VARCHAR2(25), \n" +
                "     CONSTRAINT \"OPT_FEE_PK\" PRIMARY KEY (\"DCID\")\n" +
                "  );";
        String mysqlDDL = convertOracleToMySQL(oracleDDL);
        System.out.println(mysqlDDL);
    }
}
