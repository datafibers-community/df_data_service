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

import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.11.
 */
public class Kafka011AvroTableSource extends KafkaAvroTableSource {

	/**
	 * Creates a Kafka 0.11 Avro {@link StreamTableSource} using a given {@link SpecificRecord}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 */
	public Kafka011AvroTableSource(String topic, Properties properties) {
		super(topic, properties);
	}

	/**
	 * Declares a field of the schema to be a processing time attribute.
	 *
	 * @param proctimeAttribute The name of the field that becomes the processing time field.
	 */
	@Override
	public void setProctimeAttribute(String proctimeAttribute) {
		super.setProctimeAttribute(proctimeAttribute);
	}

	/**
	 * Declares a field of the schema to be a rowtime attribute.
	 *
	 * @param rowtimeAttributeDescriptor The descriptor of the rowtime attribute.
	 */
	public void setRowtimeAttributeDescriptor(RowtimeAttributeDescriptor rowtimeAttributeDescriptor) {
		Preconditions.checkNotNull(rowtimeAttributeDescriptor, "Rowtime attribute descriptor must not be null.");
		super.setRowtimeAttributeDescriptors(Collections.singletonList(rowtimeAttributeDescriptor));
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer011<>(topic, deserializationSchema, properties);
	}
}

