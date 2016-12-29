package com.datafibers.test_tool;

/**
 * Created by duw3 on 11/2/2016.
 */
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.File;

public class ConsumerTest implements Runnable{

    private KafkaStream m_stream;
    private int m_threadNumber;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run(){
        ConsumerIterator<byte[], byte[]>it = m_stream.iterator();
        while(it.hasNext())
        {
            try {
                //System.out.println("Encoded Message received : " + message_received);
                //byte[] input = Hex.decodeHex(it.next().message().toString().toCharArray());
                //System.out.println("Deserializied Byte array : " + input);
                byte[] received_message = it.next().message();
                System.out.println(received_message);
                Schema schema = null;
                schema = new Schema.Parser().parse(new File("src/test_schema.avsc"));
                DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);
                GenericRecord payload2 = null;
                payload2 = reader.read(null, decoder);
                System.out.println("Message received : " + payload2);
            }catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        }

    }


}