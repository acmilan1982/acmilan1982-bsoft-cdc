package com.bsoft.monitor.k2zk;

import com.bsoft.common.metric.MonitorMetricEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class MonitorMetricJsonParser {
    private static final Logger logger = LogManager.getLogger(MonitorMetricJsonParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private MonitorMetricJsonParser() {
    }

    public static MonitorMetricEvent parse(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(value, MonitorMetricEvent.class);
        } catch (Exception e) {
            logger.warn("Failed to parse monitor metric json: {}", e.getMessage());
            return null;
        }
    }
}
