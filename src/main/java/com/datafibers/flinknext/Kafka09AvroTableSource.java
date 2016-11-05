package com.datafibers.flinknext;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;
/**
 * Kafka {@link StreamTableSource} for Kafka 0.9.
 */
public class Kafka09AvroTableSource extends KafkaAvroTableSource {

    /**
     * Creates a Kafka 0.9 AVRO {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    public Kafka09AvroTableSource(
            String topic,
            Properties properties, String schemaUri, String schemaSubject,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes) {

        super(topic, properties, schemaUri, schemaSubject, fieldNames, fieldTypes);
    }

    /**
     * Creates a Kafka 0.9 AVRO fields name and type are derived from schema info in properties
     * @param topic
     * @param properties
     */
    public Kafka09AvroTableSource(String topic, Properties properties, String schemaUri, String schemaSubject) {

        super(topic, properties, schemaUri, schemaSubject);
    }

    /**
     * Creates a Kafka 0.9 AVRO {@link StreamTableSource}.
     *
     * @param topic      Kafka topic to consume.
     * @param properties Properties for the Kafka consumer.
     * @param fieldNames Row field names.
     * @param fieldTypes Row field types.
     */
    public Kafka09AvroTableSource(
            String topic,
            Properties properties, String schemaUri, String schemaSubject,
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        super(topic, properties, schemaUri, schemaSubject, fieldNames, fieldTypes);
    }

    @Override
    FlinkKafkaConsumerBase<Row> getKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
        return new FlinkKafkaConsumer09<>(topic, deserializationSchema, properties);
    }
}
