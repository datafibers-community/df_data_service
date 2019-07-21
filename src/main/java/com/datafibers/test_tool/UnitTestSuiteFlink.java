package com.datafibers.test_tool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;

import net.openhft.compiler.CompilerUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.DecoderException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.log4j.Logger;
import com.datafibers.flinknext.DFRemoteStreamEnvironment;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.service.DFInitService;
import com.datafibers.util.DynamicRunner;
import com.datafibers.util.SchemaRegistryClient;

/**
 * TC for Flink features
 */
public class UnitTestSuiteFlink {

    private static final Logger LOG = Logger.getLogger(UnitTestSuiteFlink.class);

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

            Kafka09AvroTableSource kafkaTableSource = new Kafka09AvroTableSource(
                    kafkaTopic_stage,
                    properties,
                    fieldNames,
                    fieldTypes);

            //kafkaTableSource.setFailOnMissingField(true);

            tableEnv.registerTableSource("Orders", kafkaTableSource);

            //Table result = tableEnv.sql("SELECT STREAM name FROM Orders");
            Table result = tableEnv.sql("SELECT name FROM Orders");

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
        properties.setProperty("static.avro.schema", "empty_schema");

        try {
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            //Table result = tableEnv.sql("SELECT STREAM name, symbol, exchange FROM Orders");
            Table result = tableEnv.sql("SELECT name, symbol, exchangecode FROM Orders");

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

    public static void testFlinkAvroSQLWithStaticSchema() {
        System.out.println("TestCase_Test Avro SQL with static Schema");

        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";
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
        properties.setProperty("static.avro.schema", STATIC_USER_SCHEMA);

        try {
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            //Table result = tableEnv.sql("SELECT STREAM name, symbol, exchange FROM Orders");
            Table result = tableEnv.sql("SELECT name, symbol, exchangecode FROM Orders");

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
        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";

        String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        DFRemoteStreamEnvironment env = new DFRemoteStreamEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("useAvro", "avro");
        properties.setProperty("static.avro.schema",
                SchemaRegistryClient.getSchemaFromRegistry("http://localhost:8081", "test-value", "latest").toString());

        try {
            HashMap<String, String> hm = new HashMap<>();
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            Table result = tableEnv.sql("SELECT name, symbol, exchangecode FROM Orders");
            //Kafka09JsonTableSink json_sink = new Kafka09JsonTableSink ("test_json", properties, new FlinkFixedPartitioner());
            Kafka09AvroTableSink json_sink = new Kafka09AvroTableSink ("test_json", properties, new FlinkFixedPartitioner());

            // write the result Table to the TableSink
            result.writeToSink(json_sink);
            env.executeWithDFObj("Flink AVRO SQL KAFKA Test", new DFJobPOPJ().setJobConfig(hm) );
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
                    + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                    + "]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema2 = parser.parse(USER_SCHEMA);
            System.out.println("raw schema2 for name is " + schema.getField("name"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testFlinkAvroScriptWithStaticSchema() {
        System.out.println("TestCase_Test Avro Table API Script with static Schema");

        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";

        String jarPath = DFInitService.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer_test");
        properties.setProperty("schema.subject", "test-value");
        properties.setProperty("schema.registry", "localhost:8081");
        properties.setProperty("static.avro.schema", STATIC_USER_SCHEMA);

        try {
            Kafka09AvroTableSource kafkaAvroTableSource =  new Kafka09AvroTableSource("test", properties);
            tableEnv.registerTableSource("Orders", kafkaAvroTableSource);

            Table ingest = tableEnv.scan("Orders");

            String className = "dynamic.FlinkScript";

            String header = "package dynamic;\n" +
                    "import org.apache.flink.table.api.Table;\n" +
                    "import com.datafibers.util.*;\n";

            String transScript = "select(\"name\")";

            String javaCode = header +
                    "public class FlinkScript implements DynamicRunner {\n" +
                    "@Override \n" +
                    "    public Table transTableObj(Table tbl) {\n" +
                    "try {" +
                    "return tbl."+ transScript + ";" +
                    "} catch (Exception e) {" +
                    "};" +
                    "return null;}}";

            // Dynamic code generation
            Class aClass = CompilerUtils.CACHED_COMPILER.loadFromJava(className, javaCode);
            DynamicRunner runner = (DynamicRunner) aClass.newInstance();
            Table result = runner.transTableObj(ingest);

            Kafka09AvroTableSink sink =
                    new Kafka09AvroTableSink ("test_json", properties, new FlinkFixedPartitioner());
            // write the result Table to the TableSink
            result.writeToSink(sink);
            env.execute("Flink AVRO SQL KAFKA Test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, DecoderException {

        final String STATIC_USER_SCHEMA = "{"
                + "\"type\":\"record\","
                + "\"name\":\"myrecord\","
                + "\"fields\":["
                + "  { \"name\":\"symbol\", \"type\":\"string\" },"
                + "  { \"name\":\"name\", \"type\":\"string\" },"
                + "  { \"name\":\"exchangecode\", \"type\":\"string\" }"
                + "]}";

        System.out.println(SchemaRegistryClient.getSchemaFromRegistry ("http://localhost:8081", "test-value", "latest"));
        //testFlinkAvroSerDe("http://localhost:18081");
        //testFlinkAvroSerDe("http://localhost:8081");
        testFlinkAvroSQLJson();
        //testFlinkRun();
        //testFlinkSQL();
        //testFlinkAvroSQL();
        //testFlinkAvroSQLWithStaticSchema();
        //testFlinkAvroScriptWithStaticSchema();
    }

}
