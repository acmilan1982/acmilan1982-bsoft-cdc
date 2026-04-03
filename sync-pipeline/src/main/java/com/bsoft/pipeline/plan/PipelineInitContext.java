package com.bsoft.pipeline.plan;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PipelineInitContext implements Serializable {
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrapServers";

    private static final PipelineInitContext EMPTY =
            new PipelineInitContext(Collections.emptyMap());

    private final Map<String, Object> attributes;

    private PipelineInitContext(Map<String, Object> attributes) {
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
    }

    public static PipelineInitContext empty() {
        return EMPTY;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Object get(String key) {
        return attributes.get(key);
    }

    public String getString(String key) {
        Object v = attributes.get(key);
        if (v == null) return null;
        String s = String.valueOf(v).trim();
        return s.isEmpty() ? null : s;
    }

    public String getRequiredString(String key) {
        String s = getString(key);
        if (s == null) {
            throw new IllegalArgumentException("PipelineInitContext missing key: " + key);
        }
        return s;
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public static final class Builder {
        private final Map<String, Object> attrs = new LinkedHashMap<>();

        public Builder put(String key, Object value) {
            if (key != null && !key.trim().isEmpty()) {
                attrs.put(key.trim(), value);
            }
            return this;
        }

        public Builder kafkaBootstrapServers(String servers) {
            return put(KAFKA_BOOTSTRAP_SERVERS, servers);
        }

        public PipelineInitContext build() {
            return new PipelineInitContext(attrs);
        }
    }
}
