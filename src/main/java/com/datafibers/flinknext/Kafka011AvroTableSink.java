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

import com.datafibers.util.ConstantApp;
import com.datafibers.util.SchemaRegistryClient;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Kafka 0.9 {@link KafkaTableSink} that serializes data in JSON format.
 */
public class Kafka011AvroTableSink extends KafkaAvroTableSink {
	/**
	 * Creates {@link KafkaTableSink} for Kafka 0.9
	 *
	 * @param topic topic in Kafka to which table is written
	 * @param properties properties to connect to Kafka
	 * @param partitioner Kafka partitioner
	 */

	private Boolean isAppendOnly;

	public Kafka011AvroTableSink(String topic, Properties properties, FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner) {
		super(topic, properties, partitioner);
	}

/*	@Override
	protected FlinkKafkaProducerBase<Tuple2<Boolean, Row>> createKafkaProducer(String topic, Properties properties, SerializationSchema<Tuple2<Boolean, Row>> serializationSchema, FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner) {
		return new FlinkKafkaProducer09<>(topic, serializationSchema, properties, partitioner);
	}*/

	@Override
	protected FlinkKafkaProducer011<Tuple2<Boolean, Row>> createKafkaProducer(String topic, Properties properties, SerializationSchema<Tuple2<Boolean, Row>> serializationSchema, FlinkKafkaPartitioner<Tuple2<Boolean, Row>> partitioner) {
		return new FlinkKafkaProducer011<>(topic, serializationSchema, properties, Optional.ofNullable(partitioner));
	}

	@Override
	protected Kafka011AvroTableSink createCopy() {
		return new Kafka011AvroTableSink(topic, properties, partitioner);
	}

	
	@Override
	protected SerializationSchema<Tuple2<Boolean, Row>> createSerializationSchema(Properties properties) {
		// TODO Auto-generated method stub
		return new AvroRowSerializationSchema(properties);
	}

	@Override
	public void setKeyFields(String[] keys) {

	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		this.isAppendOnly = isAppendOnly;
	}

	@Override
	public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo(Types.BOOLEAN(),
				new RowTypeInfo(SchemaRegistryClient.getFieldTypesInfoFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT)));
	}
}