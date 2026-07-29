package com.bsoft.client.sink;

import com.bsoft.client.function.OracleSourceLogRichFunction;
import com.bsoft.client.state.DataSourceRunStateRepository;
import com.bsoft.client.state.JdbcDataSourceRunStateRepository;
import com.bsoft.common.bean.ConfigDataSourceConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.time.Duration;

// snapshot 完成控制信号写库 Sink
public class SnapshotCompleteJdbcSink extends RichSinkFunction<String> {

    private static final Logger logger = LogManager.getLogger(SnapshotCompleteJdbcSink.class);
    // JSON 解析器
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // 配置库连接配置
    private final ConfigDataSourceConfig configDataSourceConfig;

    // 当前客户端 ID
    private final String clientId;

    // 当前业务库 ID
    private final String dataSourceId;

    // 收到 snapshot 完成信号后的延迟秒数
    private final long delaySeconds;

    // 业务库运行状态仓库
    private transient DataSourceRunStateRepository repository;

    // 构造 snapshot 完成写库 Sink
    public SnapshotCompleteJdbcSink(ConfigDataSourceConfig configDataSourceConfig,
                                    String clientId,
                                    String dataSourceId,
                                    long delaySeconds) {
        // 保存配置库连接配置
        this.configDataSourceConfig = configDataSourceConfig;
        // 保存当前客户端 ID
        this.clientId = clientId;
        // 保存当前业务库 ID
        this.dataSourceId = dataSourceId;
        // 延迟秒数不允许小于 0
        this.delaySeconds = Math.max(delaySeconds, 0L);
    }

    // 初始化 JDBC 仓库
    @Override
    public void open(Configuration parameters) throws Exception {
        // 执行父类初始化
        super.open(parameters);
        // 初始化配置库数据源
        DataSource dataSource = configDataSourceConfig.initDataSource();
        // 创建运行状态仓库
        this.repository = new JdbcDataSourceRunStateRepository(dataSource);
    }

    // 处理 snapshot 完成控制信号
    @Override
    public void invoke(String value, Context context) throws Exception {
        // 解析控制信号 JSON
        JsonNode root = OBJECT_MAPPER.readTree(value);

        // 只处理 SNAPSHOT_COMPLETE 控制信号
        if (!"SNAPSHOT_COMPLETE".equalsIgnoreCase(root.path("control_type").asText())) {
            return;
        }

        // 读取控制信号中的业务库 ID
        String signalDataSourceId = root.path("datasource_id").asText(null);

        // 防止错误业务库的控制信号误更新当前业务库状态
        if (!dataSourceId.equals(signalDataSourceId)) {
            return;
        }

        // 延迟一段时间，给 Kafka sink 留出写入窗口
        if (delaySeconds > 0L) {
            Thread.sleep(Duration.ofSeconds(delaySeconds).toMillis());
        }

        // 标记当前业务库 snapshot 已完成，这里只做 UPDATE
        repository.markSnapshotCompleted(clientId, dataSourceId);

        logger.info("数据源： {} 快照完成标志已经被持久化",signalDataSourceId);
    }
}