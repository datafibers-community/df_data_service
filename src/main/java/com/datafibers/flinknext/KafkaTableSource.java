package com.datafibers.flinknext;

import java.util.Properties;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

/**
 * A version-agnostic Kafka {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 */
abstract class KafkaTableSource implements StreamTableSource<Row> {

    /** The Kafka topic to consume. */
	protected final String topic;

    /** Properties for the Kafka consumer. */
	protected final Properties properties;

    /** Deserialization schema to use for Kafka records. */
	protected final DeserializationSchema<Row> deserializationSchema;

    /** Row field names. */
	protected final String[] fieldNames;

    /** Row field types. */
    protected final TypeInformation<?>[] fieldTypes;

	/** Type information describing the result type. */
    protected final TypeInformation<Row> typeInfo;

	/**
	 * Creates a generic Kafka {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @param typeInfo              Type information describing the result type.
	 */
	KafkaTableSource(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			TypeInformation<Row> typeInfo) {

		this.topic = Preconditions.checkNotNull(topic, "Topic");
		this.properties = Preconditions.checkNotNull(properties, "Properties");
		this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "Deserialization schema");
		this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information");
		this.fieldTypes = null;
		this.fieldNames = null;
	}
	
	/**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param topic                 Kafka topic to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @param fieldNames            Row field names.
     * @param fieldTypes            Row field types.
     */
    KafkaTableSource(
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            String[] fieldNames,
            Class<?>[] fieldTypes) {

        this(topic, properties, deserializationSchema, fieldNames, toTypeInfo(fieldTypes));
    }

    /**
     * Creates a generic Kafka {@link StreamTableSource}.
     *
     * @param topic                 Kafka topic to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @param fieldNames            Row field names.
     * @param fieldTypes            Row field types.
     */
    KafkaTableSource(
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema,
            String[] fieldNames,
            TypeInformation<?>[] fieldTypes) {

        this.topic = Preconditions.checkNotNull(topic, "Topic");
        this.properties = Preconditions.checkNotNull(properties, "Properties");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "Deserialization schema");
        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types");

        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");
        
        this.typeInfo = new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        // Version-specific Kafka consumer
        FlinkKafkaConsumerBase<Row> kafkaConsumer = getKafkaConsumer(topic, properties, deserializationSchema);
        DataStream<Row> kafkaSource = env.addSource(kafkaConsumer);
        return kafkaSource;
    }

  
    //@Override
    public int getNumberOfFields() {
        return fieldNames.length;
    }

   // @Override
    public String[] getFieldsNames() {
        return fieldNames;
    }

   // @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    //@Override
    public TypeInformation<Row> getReturnType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    /**
     * Returns the version-specific Kafka consumer.
     *
     * @param topic                 Kafka topic to consume.
     * @param properties            Properties for the Kafka consumer.
     * @param deserializationSchema Deserialization schema to use for Kafka records.
     * @return The version-specific Kafka consumer
     */
    abstract FlinkKafkaConsumerBase<Row> getKafkaConsumer(
            String topic,
            Properties properties,
            DeserializationSchema<Row> deserializationSchema);

    /**
     * Returns the deserialization schema.
     *
     * @return The deserialization schema
     */
    protected DeserializationSchema<Row> getDeserializationSchema() {
        return deserializationSchema;
    }

    /**
     * Creates TypeInformation array for an array of Classes.
     */
    protected static TypeInformation<?>[] toTypeInfo(Class<?>[] fieldTypes) {
        TypeInformation<?>[] typeInfos = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            typeInfos[i] = TypeExtractor.getForClass(fieldTypes[i]);
        }
        return typeInfos;
    }


	@Override
	public String explainSource() {
		return "";
	}
}
