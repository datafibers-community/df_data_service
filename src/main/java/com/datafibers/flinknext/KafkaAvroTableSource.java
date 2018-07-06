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
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.Map;
import java.util.Properties;

/**
 * A version-agnostic Kafka Avro {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #createKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource implements DefinedFieldMapping {

	private Map<String, String> fieldMapping;
	private TableSchema schema;
	private Properties properties;

	/**
	 * Creates a generic Kafka Avro {@link StreamTableSource} using a given {@link SpecificRecord}.
	 *
	 * @param topic            Kafka topic to consume.
	 * @param properties       Properties for the Kafka consumer.
	 */
	protected KafkaAvroTableSource(String topic, Properties properties) {
		super(
				topic,
				properties,
				TableSchema.fromTypeInfo(
						createDeserializationSchema(
								SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
								SchemaRegistryClient.getFieldTypesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
								properties
						).getProducedType()
				),
				createDeserializationSchema(
						SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
						SchemaRegistryClient.getFieldTypesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
						properties
				).getProducedType()
		);
		this.properties = properties;
	}

	@Override
	public Map<String, String> getFieldMapping() {
		return fieldMapping;
	}

	@Override
	public String explainSource() {
		return "KafkaAvroTableSource(" + this.schema.toString() + ")";
	}

	@Override
	protected AvroRowDeserializationSchema getDeserializationSchema() {

		return createDeserializationSchema(
				SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
				SchemaRegistryClient.getFieldTypesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
				properties
		);
	}

	private static AvroRowDeserializationSchema createDeserializationSchema(
			String[] fieldNames,
			Class<?>[] fieldTypes, Properties properties) {

		return new AvroRowDeserializationSchema(fieldNames, fieldTypes, properties);
	}
}
