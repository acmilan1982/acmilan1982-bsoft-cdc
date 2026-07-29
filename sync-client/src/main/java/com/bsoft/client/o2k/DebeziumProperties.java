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
    /**
     * 构造传递给 Debezium 的配置
     *
     * @param logicalServerName 当前 job 对应的 Debezium logical server name
     *
     * 说明：
     * 1. 在一个 JVM 内同时运行多个 Debezium job 时，database.server.name 必须唯一
     * 2. 该值会影响 Debezium connectorName，进而影响 metrics MBean 的命名
     * 3. 如果多个 job 使用相同的 logicalServerName，会导致：
     *    Unable to register metrics as an old set with the same name exists
     */
    public static Properties getDebeziumProperties(String logicalServerName) {
        Properties debeziumProps = new Properties();

        // 启用 DDL 捕获的关键配置
        debeziumProps.setProperty("include.schema.changes", "true");
        debeziumProps.setProperty("schema.history.internal.ddl.filter", ".*");

        // 关键：确认使用 LogMiner，并让事件产出 ROWID
        debeziumProps.setProperty("database.connection.adapter", "logminer");
        debeziumProps.setProperty("include.row_id", "true");

        // 关键：为每个 job 指定唯一的 logical server name
        //
        // 说明：
        // 1. Flink CDC OracleSource 内部默认会把 database.server.name 设为固定值 oracle_logminer
        // 2. 但在 OracleSource.Builder.build() 里，后续 props.putAll(dbzProperties) 会覆盖这个默认值
        // 3. 因此这里传入的 logicalServerName 会成为最终生效值
        debeziumProps.setProperty("database.server.name", logicalServerName);

        return debeziumProps;
    }
}