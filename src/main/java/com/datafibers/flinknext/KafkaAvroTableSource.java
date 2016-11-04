package com.datafibers.flinknext;

import com.datafibers.util.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

/**
 * A version-agnostic Kafka AVRO {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the JSON file and so are the types.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource {

    /**
     * Creates a generic Kafka AVRO {@link StreamTableSource}.
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

        super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes, properties), fieldNames, fieldTypes);
    }

    /**
     * Creates a generic Kafka AVRO with fields and types derived from Schema Registry
     * @param topic
     * @param properties
     */
    KafkaAvroTableSource(String topic, Properties properties) {
        super(topic, properties,
                createDeserializationSchema(
                        SchemaRegistryClient.getFieldNamesFromProperty(properties),
                        SchemaRegistryClient.getFieldTypesFromProperty(properties),
                        properties
                ),
                SchemaRegistryClient.getFieldNamesFromProperty(properties),
                SchemaRegistryClient.getFieldTypesFromProperty(properties)
        );
    }

    /**
     * Creates a generic Kafka AVRO with fields and types derived from Avro Schema
     * @param topic
     * @param properties
     */
    KafkaAvroTableSource(String topic, Properties properties, Schema schema) {
        super(topic, properties,
                createDeserializationSchema(
                        SchemaRegistryClient.getFieldNames(schema),
                        SchemaRegistryClient.getFieldTypes(schema),
                        properties
                ),
                SchemaRegistryClient.getFieldNames(schema),
                SchemaRegistryClient.getFieldTypes(schema)
        );
    }

    /**
     * Creates a generic Kafka AVRO {@link StreamTableSource}.
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

        super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes, properties), fieldNames, fieldTypes);
    }

    /**
     * Configures the failure behaviour if a AVRO field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        AvroRowDeserializationSchema deserializationSchema = (AvroRowDeserializationSchema) getDeserializationSchema();
        deserializationSchema.setFailOnMissingField(failOnMissingField);
    }

    private static AvroRowDeserializationSchema createDeserializationSchema(
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes, Properties properties) {

        return new AvroRowDeserializationSchema(fieldNames, fieldTypes, properties);
    }

    private static AvroRowDeserializationSchema createDeserializationSchema(
            String[] fieldNames,
            Class<?>[] fieldTypes, Properties properties) {

        return new AvroRowDeserializationSchema(fieldNames, fieldTypes, properties);
    }
}
