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
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.StreamTableSource;

import java.util.Map;
import java.util.Objects;
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
						new AvroRowDeserializationSchema (
								SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
								SchemaRegistryClient.getFieldTypesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
								properties
						).getProducedType()
				),
				new AvroRowDeserializationSchema (
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
		return "KafkaAvroTableSource()";
	}

	@Override
	protected AvroRowDeserializationSchema getDeserializationSchema() {

		return new AvroRowDeserializationSchema(
				SchemaRegistryClient.getFieldNamesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
				SchemaRegistryClient.getFieldTypesFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT),
				properties
		);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof org.apache.flink.streaming.connectors.kafka.KafkaAvroTableSource)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		final KafkaAvroTableSource that = (KafkaAvroTableSource) o;
		return Objects.equals(fieldMapping, that.fieldMapping);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), fieldMapping);
	}

	//////// SETTERS FOR OPTIONAL PARAMETERS

	/**
	 * Configures a field mapping for this TableSource.
	 *
	 * @param fieldMapping The field mapping.
	 */
	protected void setFieldMapping(Map<String, String> fieldMapping) {
		this.fieldMapping = fieldMapping;
	}

	//////// HELPER METHODS

	/**
	 * Abstract builder for a {@link org.apache.flink.streaming.connectors.kafka.KafkaAvroTableSource} to be extended by builders of subclasses of
	 * KafkaAvroTableSource.
	 *
	 * @param <T> Type of the KafkaAvroTableSource produced by the builder.
	 * @param <B> Type of the KafkaAvroTableSource.Builder subclass.
	 */
	protected abstract static class Builder<T extends KafkaAvroTableSource, B extends KafkaAvroTableSource.Builder>
			extends KafkaTableSource.Builder<T, B> {

		private Class<? extends SpecificRecordBase> avroClass;

		private Map<String, String> fieldMapping;

		/**
		 * Sets the class of the Avro records that are read from the Kafka topic.
		 *
		 * @param avroClass The class of the Avro records that are read from the Kafka topic.
		 * @return The builder.
		 */
		public B forAvroRecordClass(Class<? extends SpecificRecordBase> avroClass) {
			this.avroClass = avroClass;
			return builder();
		}

		/**
		 * Sets a mapping from schema fields to fields of the produced Avro record.
		 *
		 * <p>A field mapping is required if the fields of produced tables should be named different than
		 * the fields of the Avro record.
		 * The key of the provided Map refers to the field of the table schema,
		 * the value to the field of the Avro record.</p>
		 *
		 * @param schemaToAvroMapping A mapping from schema fields to Avro fields.
		 * @return The builder.
		 */
		public B withTableToAvroMapping(Map<String, String> schemaToAvroMapping) {
			this.fieldMapping = schemaToAvroMapping;
			return builder();
		}

		/**
		 * Returns the configured Avro class.
		 *
		 * @return The configured Avro class.
		 */
		protected Class<? extends SpecificRecordBase> getAvroRecordClass() {
			return this.avroClass;
		}

		@Override
		protected void configureTableSource(T source) {
			super.configureTableSource(source);
			source.setFieldMapping(this.fieldMapping);
		}
	}
}
