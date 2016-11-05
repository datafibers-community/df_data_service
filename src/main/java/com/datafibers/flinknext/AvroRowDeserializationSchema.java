package com.datafibers.flinknext;

import com.datafibers.util.ConstantApp;
import com.datafibers.util.SchemaRegistryClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Deserialization schema from AVRO to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a AVROject and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class AvroRowDeserializationSchema implements DeserializationSchema<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroRowDeserializationSchema.class);
    private static final long serialVersionUID = 4330538776656642779L;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;
    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;
    /** Avro Schema for the row is in this properties. It has to be final. */
    private final Properties properties;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    /** Generic Avro Schema reader for the row */
    private transient GenericDatumReader<GenericRecord> reader;

    /** TODO - When schema changes, the Source table does not need to be recreated.*/

    /**
     * Creates a AVRO deserializtion schema for the given fields and type classes.
     *
     * @param fieldNames Names of JSON fields to parse.
     * @param fieldTypes Type classes to parse JSON fields as.
     */
    public AvroRowDeserializationSchema(String[] fieldNames, Class<?>[] fieldTypes, Properties properties) {

        this.properties = Preconditions.checkNotNull(properties, "properties");
        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
        this.fieldTypes = new TypeInformation[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            this.fieldTypes[i] = TypeExtractor.getForClass(fieldTypes[i]);
        }

        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");
    }

    /**
     * Creates a AVRO deserializtion schema for the given fields and types.
     *
     * @param fieldNames Names of AVRO fields to parse.
     * @param fieldTypes Types to parse AVRO fields as.
     */
    public AvroRowDeserializationSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes, Properties properties) {

        this.properties = Preconditions.checkNotNull(properties, "properties");
        this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
        this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types");

        Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
                "Number of provided field names and types does not match.");
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {

            ByteBuffer buffer = ByteBuffer.wrap(message);
            if (buffer.get() != ConstantApp.MAGIC_BYTE) {
                throw new SerializationException("Unknown magic byte!");
            }
            //int schema_id = buffer.getInt();

            int length = buffer.limit() - 1 - ConstantApp.idSize;
            int start = buffer.position() + buffer.arrayOffset();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);

            reader = new GenericDatumReader<>(SchemaRegistryClient.getLatestSchemaFromProperty(properties));
            GenericRecord gr = reader.read(null, decoder);

            JsonNode root = objectMapper.readTree(gr.toString());

            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = root.get(fieldNames[i]);

                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                    row.setField(i, value);
                }
            }

            return row;
        } catch (Throwable t) {
            throw new IOException("Failed to deserialize AVRO object.", t);
        }
    }

    @Override
    public boolean isEndOfStream(Row nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return new RowTypeInfo(fieldTypes);
    }

    /**
     * Configures the failure behaviour if a JSON field is missing.
     *
     * <p>By default, a missing field is ignored and the field is set to null.
     *
     * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
     */
    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

}
