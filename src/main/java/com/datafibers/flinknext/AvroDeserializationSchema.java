package com.datafibers.flinknext;

import com.datafibers.util.ConstantApp;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.kafka.common.errors.SerializationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;

public class AvroDeserializationSchema implements DeserializationSchema<GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroDeserializationSchema.class);

    private static final long serialVersionUID = 4330538776656642778L;

    private static Schema avroSchema;
    private static String schemaUri;
    private static String schemaSubject;
    private static boolean dynamicSchema = false;

    private transient GenericDatumReader<GenericRecord> reader;
    private transient BinaryDecoder decoder;

    public AvroDeserializationSchema(String schemaUri, String schemaSubject, Boolean dynamicSchema) {
        // get schema for each records
        this.dynamicSchema = dynamicSchema;
        this.schemaUri = schemaUri;
        this.schemaSubject = schemaSubject;
    }
    public AvroDeserializationSchema(String schemaUri, String schemaSubject) {
        //get latest schema once for all records
        this.avroSchema = getSchemaFromRegistry(schemaUri, schemaSubject, "latest");
        this.schemaUri = schemaUri;
        this.schemaSubject = schemaSubject;
    }

    @Override
    public GenericRecord  deserialize(byte[] message) {
        ByteBuffer buffer = ByteBuffer.wrap(message);
        if (buffer.get() != ConstantApp.MAGIC_BYTE) {
            throw new SerializationException("Unknown magic byte!");
        }
        int schema_id = buffer.getInt();

        if (dynamicSchema) {
            this.avroSchema = getSchemaFromRegistry(schemaUri, schemaSubject, schema_id + "");
        }

        reader = new GenericDatumReader<GenericRecord>(avroSchema);

        try {
            int length = buffer.limit() - 1 - ConstantApp.idSize;
            int start = buffer.position() + buffer.arrayOffset();
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, null);
            GenericRecord gr = reader.read(null, decoder);
            return gr;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean isEndOfStream(GenericRecord  nextElement) {
        return false;
    }

    @Override
    public TypeInformation<GenericRecord> getProducedType() {
        return TypeExtractor.getForClass(GenericRecord.class);
    }

    private Schema getSchemaFromRegistry(String schemaUri, String schemaSubject, String schemaVersion) {
        String fullUrl = String.format("%s/subjects/%s/versions/%s", schemaUri, schemaSubject, schemaVersion);

        String schemaString;
        BufferedReader br = null;
        try {
            StringBuilder response = new StringBuilder();
            String line;
            br = new BufferedReader(new InputStreamReader(new URL(fullUrl).openStream()));
            while ((line = br.readLine()) != null) {
                response.append(line);
            }

            JsonNode responseJson = new ObjectMapper().readValue(response.toString(), JsonNode.class);
            schemaString = responseJson.get("schema").getValueAsText();

            try {
                return new Schema.Parser().parse(schemaString);
            } catch (SchemaParseException ex) {
                LOG.error(String.format("Unable to successfully parse schema from: %s", schemaString), ex);
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
