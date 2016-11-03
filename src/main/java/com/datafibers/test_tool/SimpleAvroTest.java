package com.datafibers.test_tool;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
            System.out.println(gr.get("name").toString());

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }



    }
}