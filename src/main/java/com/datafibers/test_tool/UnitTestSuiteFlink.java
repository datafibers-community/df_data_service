package com.datafibers.test_tool;

import com.datafibers.flinknext.AvroDeserializationSchema;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.flinknext.Kafka09JsonTableSink;
import com.datafibers.processor.FlinkTransformProcessor;
import com.datafibers.service.DFInitService;
import com.datafibers.util.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.api.table.sinks.CsvTableSink;
import org.apache.flink.api.table.sinks.TableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * TC for Flink features
 */
public class UnitTestSuiteFlink {

    private static final Logger LOG = LoggerFactory.getLogger(UnitTestSuiteFlink.class);

    public static void testFlinkRun() {
        LOG.info("Only Unit Testing Function is enabled - Test Flink Run UDF");
        String jarFile = "/home/vagrant/quick-start-1.0-fat.jar";
        FlinkTransformProcessor.runFlinkJar(jarFile, "localhost:6123");
    }

    public static void testFlinkSQL() {

        LOG.info("Only Unit Testing Function is enabled");
        String resultFile = "/home/vagrant/test.txt";

        try {

            String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jarPath)
                    .setParallelism(1);
            String kafkaTopic = "finance";
            String kafkaTopic_stage = "df_trans_stage_finance";
            String kafkaTopic_out = "df_trans_out_finance";



            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "consumer3");

            // Internal covert Json String to Json - Begin
            DataStream<String> stream = env
                    .addSource(new FlinkKafkaConsumer09<>(kafkaTopic, new SimpleStringSchema(), properties));

            stream.map(new MapFunction<String, String>() {
                @Override
                public String map(String jsonString) throws Exception {
                    return jsonString.replaceAll("\\\\", "").replace("\"{", "{").replace("}\"","}");
                }
            }).addSink(new FlinkKafkaProducer09<String>("localhost:9092", kafkaTopic_stage, new SimpleStringSchema()));
            // Internal covert Json String to Json - End

            String[] fieldNames =  new String[] {"name"};
            Class<?>[] fieldTypes = new Class<?>[] {String.class};

            KafkaJsonTableSource kafkaTableSource = new Kafka09JsonTableSource(
                    kafkaTopic_stage,
                    properties,
                    fieldNames,
                    fieldTypes);

            //kafkaTableSource.setFailOnMissingField(true);

            tableEnv.registerTableSource("Orders", kafkaTableSource);

            Table result = tableEnv.sql("SELECT STREAM name FROM Orders");

            Files.deleteIfExists(Paths.get(resultFile));

            // create a TableSink
            TableSink sink = new CsvTableSink(resultFile, "|");
            // write the result Table to the TableSink
            result.writeToSink(sink);

            env.execute("FlinkConsumer");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testFlinkAvroSerDe(String schema_registery_host) {
        System.out.println("TestCase_Test Avro SerDe");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");

        try {
            AvroDeserializationSchema avroSchemaDecoder = new AvroDeserializationSchema(schema_registery_host, "test-value");
            FlinkKafkaConsumer09<GenericRecord> kafkaConsumer = new FlinkKafkaConsumer09<GenericRecord>("test", avroSchemaDecoder, properties);
            DataStream<GenericRecord> messageStream = env.addSource(kafkaConsumer);
            messageStream.rebalance().print();
            env.execute("Flink AVRO KAFKA Test SerDe");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testFlinkAvroSQL() {
        System.out.println("TestCase_Test Avro SQL");
        String resultFile = "/home/vagrant/test.txt";

        String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");

        try {
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            Table result = tableEnv.sql("SELECT STREAM name, symbol, exchange FROM Orders");

            Files.deleteIfExists(Paths.get(resultFile));

            // create a TableSink
            TableSink sink = new CsvTableSink(resultFile, "|");
            // write the result Table to the TableSink
            result.writeToSink(sink);
            env.execute("Flink AVRO SQL KAFKA Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testFlinkAvroSQLJson() {
        System.out.println("TestCase_Test Avro SQL to Json Sink");

        String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");

        try {
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            Table result = tableEnv.sql("SELECT STREAM name, symbol, exchange FROM Orders");
            Kafka09JsonTableSink json_sink = new Kafka09JsonTableSink ("test_json", properties, new FixedPartitioner());

            // write the result Table to the TableSink
            result.writeToSink(json_sink);
            env.execute("Flink AVRO SQL KAFKA Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testSchemaRegisterClient() {
        System.out.println("TestCase_Test Schema Register Client");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");

        try {
            Schema schema = SchemaRegistryClient.getLatestSchemaFromProperty(properties);
            System.out.println("raw schema1 for name is " + schema.getField("name"));

            String USER_SCHEMA = "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"test\","
                    + "\"fields\":["
                    + "  { \"name\":\"name\", \"type\":\"string\" },"
                    + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                    + "  { \"name\":\"exchange\", \"type\":\"string\" }"
                    + "]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema2 = parser.parse(USER_SCHEMA);
            System.out.println("raw schema2 for name is " + schema.getField("name"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, DecoderException {
        testFlinkAvroSerDe("http://localhost:18081");
    }


}
