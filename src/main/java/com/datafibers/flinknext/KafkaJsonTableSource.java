package com.datafibers.flinknext;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * A version-agnostic Kafka JSON {@link StreamTableSource}.
 *
 * <p>
 * The version-specific Kafka consumers need to extend this class and override
 * {@link #getKafkaConsumer(String, Properties, DeserializationSchema)} .
 *
 * <p>
 * The field names are used to parse the JSON file and so are the types.
 */
public abstract class KafkaJsonTableSource extends KafkaTableSource {

	/**
	 * Creates a generic Kafka JSON {@link StreamTableSource}.
	 *
	 * @param topic
	 *            Kafka topic to consume.
	 * @param properties
	 *            Properties for the Kafka consumer.
	 * @param typeInfo
	 *            Type information describing the result type. The field names
	 *            are used to parse the JSON file and so are the types.
	 */
	KafkaJsonTableSource(String topic, Properties properties,
			TypeInformation<Row> typeInfo) {

		super(topic, properties, createDeserializationSchema(typeInfo),
				typeInfo);
	}
	KafkaJsonTableSource(
            String topic,
            Properties properties,
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes, properties), fieldNames, fieldTypes);
    }

	private static DeserializationSchema<Row> createDeserializationSchema(
			String[] fieldNames, Class<?>[] fieldTypes, Properties properties) {
		return new JsonRowDeserializationSchema(fieldNames, fieldTypes, properties);
	
	}
	/**
	 * Configures the failure behaviour if a JSON field is missing.
	 *
	 * <p>
	 * By default, a missing field is ignored and the field is set to null.
	 *
	 * @param failOnMissingField
	 *            Flag indicating whether to fail or not on a missing field.
	 */
	public void setFailOnMissingField(boolean failOnMissingField) {
		JsonRowDeserializationSchema deserializationSchema = (JsonRowDeserializationSchema) getDeserializationSchema();
		deserializationSchema.setFailOnMissingField(failOnMissingField);
	}

	private static JsonRowDeserializationSchema createDeserializationSchema(
			TypeInformation<Row> typeInfo) {

		return new JsonRowDeserializationSchema(typeInfo);
	}
	

}
