package com.datafibers.flinknext;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.9.
 */
public class Kafka09JsonTableSource extends KafkaJsonTableSource {

		/**
		 * Creates a Kafka 0.9 JSON {@link StreamTableSource}.
		 *
		 * @param topic      Kafka topic to consume.
		 * @param properties Properties for the Kafka consumer.
		 * @param typeInfo   Type information describing the result type. The field names are used
		 *                   to parse the JSON file and so are the types.
		 */
		public Kafka09JsonTableSource(
				String topic,
				Properties properties,
				TypeInformation<Row> typeInfo) {

			super(topic, properties, typeInfo);
		}
		
	    public Kafka09JsonTableSource(
	            String topic,
	            Properties properties,
	            String[] fieldNames,
	            Class<?>[] fieldTypes) {

	        super(topic, properties, fieldNames, fieldTypes);
	    }
		@Override
		FlinkKafkaConsumerBase<Row> getKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
			return new FlinkKafkaConsumer09<>(topic, deserializationSchema, properties);
		}


	}