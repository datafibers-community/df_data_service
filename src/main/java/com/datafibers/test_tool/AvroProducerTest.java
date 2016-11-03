package com.datafibers.test_tool;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.DecoderException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;


public class AvroProducerTest {
    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int idSize = 4;

    public void producer(Schema schema) throws IOException {

        Properties props = new Properties();
        props.put("metadata.broker.list", "0:9092");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");

        //props.put("serializer.class", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        //props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);
        GenericRecord payload1 = new GenericData.Record(schema);
        //Step2 : Put data in that genericrecord object

        //payload1.put("name", "अasa");
        payload1.put("name", "name_test");
        payload1.put("symbol", "symbol_test");
        payload1.put("exchange", "excahnge_test");

        System.out.println("Original Message : "+ payload1);

        //Step3 : Serialize the object to a bytearray
        DatumWriter<GenericRecord>writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(MAGIC_BYTE);
        out.write(ByteBuffer.allocate(idSize).putInt(1).array());

        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(payload1, encoder);
        encoder.flush();
        out.close();

        byte[] serializedBytes = out.toByteArray();

        System.out.println("Sending message in bytes : " + serializedBytes);
        //String serializedHex = Hex.encodeHexString(serializedBytes);
        //System.out.println("Serialized Hex String : " + serializedHex);
        KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>("test", serializedBytes);
        producer.send(message);
        producer.close();
    }


    public static void main(String[] args) throws IOException, DecoderException {
        System.out.println("TestCase_Simple Avro Producer");
        String USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"test\","
                + "\"fields\":["
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"exchange\", \"type\":\"string\" }"
                + "]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        AvroProducerTest test = new AvroProducerTest();
        test.producer(schema);
    }
}
