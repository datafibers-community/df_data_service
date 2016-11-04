package com.datafibers.test_tool;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.avro.Schema.Type.RECORD;

public class SimpleAvroTest {

    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"symbol\", \"type\":\"string\" },"
            + "  { \"name\":\"name\", \"type\":\"string\" },"
            + "  { \"name\":\"exchange\", \"type\":\"string\" }"
            + "]}";

    public static void main(String[] args) throws InterruptedException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("symbol", "CHINA");
        user1.put("exchange", "TEST");

        try {
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder( out, null );
            writer.write(user1, encoder );
            encoder.flush();

            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
            GenericRecord gr = reader.read(null, decoder);
            System.out.println(gr.toString());

            System.out.println("******Get Fields Names");

            List<String> stringList = new ArrayList<String>();
            if (RECORD.equals(schema.getType()) && schema.getFields() != null
                    && !schema.getFields().isEmpty()) {
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    stringList.add(field.name());
                }
            }
            String[] fieldNames = stringList.toArray( new String[] {} );
            for ( String element : fieldNames ) {
                System.out.println( element );
            }

            System.out.println("******Get Fields Types");

            int fieldsLen = schema.getFields().size();

            Class<?>[] fieldTypes = new Class[fieldsLen];
            int index = 0;
            String typeName;

            try {
                if (RECORD.equals(schema.getType()) && schema.getFields() != null
                        && !schema.getFields().isEmpty()) {
                    for (org.apache.avro.Schema.Field field : schema.getFields()) {
                        typeName = field.schema().getType().getName().toLowerCase();
                        // Mapping Avro type to Java type - TODO Complex type is not supported yet
                        switch (typeName) {
                            case "boolean":
                            case "string":
                            case "long":
                            case "float":
                                fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
                                break;
                            case "bytes":
                                fieldTypes[index] = Class.forName("java.util.Byte");
                                break;
                            case "int":
                                fieldTypes[index] = Class.forName("java.lang.Integer");
                                break;
                            default:
                                fieldTypes[index] = Class.forName("java.lang." + StringUtils.capitalize(typeName));
                        }
                        index ++;
                    }
                }
            } catch (ClassNotFoundException cnf) {
                cnf.printStackTrace();
            }


            for ( Class<?> element : fieldTypes ) {
                System.out.println( element );
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }



    }
}