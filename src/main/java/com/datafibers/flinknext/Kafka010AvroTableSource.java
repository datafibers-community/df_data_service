package com.datafibers.flinknext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.10.
 */
public class Kafka010AvroTableSource extends KafkaAvroTableSource {

    /**
     * Creates a Kafka 0.10 AVRO {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    public Kafka010AvroTableSource(
            String topic,
            Properties properties,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes) {

        super(topic, properties, fieldNames, fieldTypes);
    }

    /**
     * Creates a Kafka 0.10 AVRO fields name and type are derived from schema info in properties
     * @param topic - topic in Kafka to map the table source
     * @param properties - list of properties to connect Kafka, etc
     *
     * <p>Following property need to set ahead of using.
     * <p>properties.setProperty("schema.subject", "test-value"); // Subject Name for the SchemaRegistry
     * <p>properties.setProperty("schema.registry", "localhost:8081"); // Host and port for the SchemaRegistry
     * <p>properties.setProperty("static.avro.schema", STATIC_USER_SCHEMA); // Schema string when Schema is static. With Static Schema, SchemaRegistry does not have to be used.
     */
    public Kafka010AvroTableSource(String topic, Properties properties) {

        super(topic, properties);
    }

    /**
     * Creates a Kafka 0.10 AVRO {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    public Kafka010AvroTableSource(
            String topic,
            Properties properties,
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        super(topic, properties, fieldNames, fieldTypes);
    }

    @Override
    FlinkKafkaConsumerBase<Row> getKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
        return new FlinkKafkaConsumer010<>(topic, deserializationSchema, properties);
    }


}
