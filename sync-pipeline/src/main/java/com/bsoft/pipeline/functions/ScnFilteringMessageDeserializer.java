package com.bsoft.pipeline.functions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Kafka deserializer that enriches the raw CDC message with kafka metadata
 * and filters records whose source.scn is not greater than the configured boundary.
 */
public class ScnFilteringMessageDeserializer implements KafkaRecordDeserializationSchema<String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String topic;
    private final Long startAfterScn;
    private final boolean dropWhenScnMissing;

    public ScnFilteringMessageDeserializer(String topic, Long startAfterScn) {
        this(topic, startAfterScn, true);
    }

    public ScnFilteringMessageDeserializer(String topic, Long startAfterScn, boolean dropWhenScnMissing) {
        this.topic = topic == null ? "" : topic.trim();
        this.startAfterScn = startAfterScn;
        this.dropWhenScnMissing = dropWhenScnMissing;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws JsonProcessingException {
        if (record == null || record.value() == null) {
            return;
        }

        String msgJson = new String(record.value(), StandardCharsets.UTF_8);
        JsonNode root = MAPPER.readTree(msgJson);
        if (!(root instanceof ObjectNode)) {
            return;
        }

        ObjectNode msgNode = (ObjectNode) root;
        Long scn = extractScn(msgNode);
        if (!shouldKeep(scn)) {
            return;
        }

        msgNode.put("kafka_offset", record.offset());
        msgNode.put("kafka_topic", topic.isEmpty() ? record.topic() : topic);
        msgNode.put("raw_message", msgJson);

        out.collect(MAPPER.writeValueAsString(msgNode));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }

    private boolean shouldKeep(Long scn) {
        if (startAfterScn == null) {
            return true;
        }
        if (scn == null) {
            return !dropWhenScnMissing;
        }
        return scn > startAfterScn;
    }

    private Long extractScn(ObjectNode msgNode) {
        JsonNode scnNode = msgNode.path("source").path("scn");
        if (scnNode.isMissingNode() || scnNode.isNull()) {
            return null;
        }

        String text = scnNode.asText();
        if (text == null || text.trim().isEmpty()) {
            return null;
        }

        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
