package com.datafibers.flinknext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.JsonRowDeserializationSchema;

import java.util.Properties;

/**
 * A version-agnostic Kafka Avro {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the JSON file and so are the types.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource {

    /**
     * Creates a generic Kafka JSON {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    KafkaAvroTableSource(
            String topic,
            Properties properties,
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes), fieldNames, fieldTypes);
    }

    /**
     * Creates a generic Kafka JSON {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    KafkaAvroTableSource(
            String topic,
            Properties properties,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes) {

        super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes), fieldNames, fieldTypes);
    }

    /**
     * Configures the failure behaviour if a JSON field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        JsonRowDeserializationSchema deserializationSchema = (JsonRowDeserializationSchema) getDeserializationSchema();
        deserializationSchema.setFailOnMissingField(failOnMissingField);
    }

    private static JsonRowDeserializationSchema createDeserializationSchema(
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes) {

        return new JsonRowDeserializationSchema(fieldNames, fieldTypes);
    }

    private static JsonRowDeserializationSchema createDeserializationSchema(
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        return new JsonRowDeserializationSchema(fieldNames, fieldTypes);
    }
}
