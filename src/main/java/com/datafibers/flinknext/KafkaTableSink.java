/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datafibers.flinknext;

import java.util.Properties;

import com.datafibers.util.ConstantApp;
import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.datafibers.util.SchemaRegistryClient;

/**
 * A version-agnostic Kafka {@link UpsertStreamTableSink}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaProducer(String, Properties, SerializationSchema, FlinkKafkaPartitioner)}}.
 */
public abstract class KafkaTableSink implements UpsertStreamTableSink<Row> {

	protected final String topic;
	protected final Properties properties;
	protected SerializationSchema<Tuple2<Boolean, Row>> serializationSchema;
	protected final FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner;
	protected String[] fieldNames;
	protected TypeInformation[] fieldTypes;
	protected Schema schema;
	/**
	 * Creates KafkaTableSink
	 *
	 * @param topic                 Kafka topic to write to.
	 * @param properties            Properties for the Kafka consumer.
	 * @param partitioner           Partitioner to select Kafka partition for each item
	 */
	public KafkaTableSink(
			String topic,
			Properties properties,
			FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner) {

		this.topic = Preconditions.checkNotNull(topic, "topic");
		this.properties = Preconditions.checkNotNull(properties, "properties");
		this.partitioner = Preconditions.checkNotNull(partitioner, "partitioner");
		this.schema = SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT);
		//setIsAppendOnly(false);
		setIsAppendOnly(true);
		setKeyFields(properties.getProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS).split(","));
	}

	/**
	 * Returns the version-specifid Kafka producer.
	 *
	 * @param topic               Kafka topic to produce to.
	 * @param properties          Properties for the Kafka producer.
	 * @param serializationSchema Serialization schema to use to create Kafka records.
	 * @param partitioner         Partitioner to select Kafka partition.
	 * @return The version-specific Kafka producer
	 */
	protected abstract FlinkKafkaProducerBase<Tuple2<Boolean, Row>> createKafkaProducer(
		String topic, Properties properties,
		SerializationSchema<Tuple2<Boolean, Row>> serializationSchema,
		FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner);

	/**
	 * Create serialization schema for converting table rows into bytes.
	 *
	 * @param properties
	 * @return
     */

	protected abstract SerializationSchema<Tuple2<Boolean, Row>> createSerializationSchema(Properties properties) ;

	/**
	 * Create a deep copy of this sink.
	 *
	 * @return Deep copy of this sink
	 */
	protected abstract KafkaTableSink createCopy();

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		dataStream.addSink(createKafkaProducer(topic, properties, serializationSchema, partitioner));
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	//@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(fieldTypes);
	}

	@Override
	public KafkaTableSink configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		KafkaTableSink copy = createCopy();
		copy.fieldNames = SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT);
		copy.fieldTypes = SchemaRegistryClient.getFieldTypesInfoFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT);
		copy.serializationSchema = createSerializationSchema(properties);
		return copy;
	}

}
