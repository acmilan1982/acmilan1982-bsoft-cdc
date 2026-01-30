package com.bsoft.client.o2k;

import java.util.Properties;

/**
 * ClassName: DebeziumProperties
 * Package:   com.bsoft.client.o2k
 * Description:
 *
 * @Author: snoopy
 * @Create: 2025/8/6 - 14:08
 */
public class DebeziumProperties {
    public static Properties getDebeziumProperties() {
        Properties debeziumProps = new Properties();

        // 启用DDL捕获的关键配置
        debeziumProps.setProperty("include.schema.changes", "true");
        debeziumProps.setProperty("schema.history.internal.ddl.filter", ".*");

        // ★★★ 关键：确认用 LogMiner，并让事件产出 ROWID ★★★
        debeziumProps.setProperty("database.connection.adapter", "logminer"); // 必须是 logminer 才有 rowid
        debeziumProps.setProperty("include.row_id", "true");                   // 让 rowid 出现在 payload.source 里

        return debeziumProps;
    }
}
