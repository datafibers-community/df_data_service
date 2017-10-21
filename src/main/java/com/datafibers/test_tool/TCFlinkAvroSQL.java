package com.datafibers.test_tool;

import com.datafibers.flinknext.Kafka010AvroTableSource;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.flinknext.Kafka09AvroTableSource;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.SchemaRegistryClient;
import org.apache.commons.codec.DecoderException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * TC for Flink features
 */
public class TCFlinkAvroSQL {

    private static final Logger LOG = Logger.getLogger(TCFlinkAvroSQL.class);

    public static void tcFlinkAvroSQL(String SchemaRegistryHostPort, String srcTopic, String targetTopic, String sqlState) {
        System.out.println("tcFlinkAvroSQL");
        String resultFile = "testResult";

        //String jarPath = "C:/Users/dadu/Coding/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar";
        String jarPath = "/Users/will/Documents/Coding/GitHub/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, jarPath)
                .setParallelism(1);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty(ConstantApp.PK_KAFKA_HOST_PORT.replace("_", "."), "localhost:9092");
        properties.setProperty(ConstantApp.PK_KAFKA_CONSUMER_GROURP, "consumer_test");
        //properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, "test");
        properties.setProperty(ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT.replace("_", "."), SchemaRegistryHostPort);
        properties.setProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS, "symbol");

        String[] srcTopicList = srcTopic.split(",");
        for (int i = 0; i < srcTopicList.length; i++) {
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_INPUT, srcTopicList[i]);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_INPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT) + "");
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_INPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT).toString());
            tableEnv.registerTableSource(srcTopic, new Kafka010AvroTableSource(srcTopicList[i], properties));
        }

        try {
            Table result = tableEnv.sql(sqlState);
            result.printSchema();
            System.out.println("generated avro schema is = " + SchemaRegistryClient.tableAPIToAvroSchema(result, targetTopic));
            SchemaRegistryClient.addSchemaFromTableResult(SchemaRegistryHostPort, targetTopic, result);

            // delivered properties
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, targetTopic);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT) + "");
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());

            System.out.println(Paths.get(resultFile).toAbsolutePath());
            Kafka09AvroTableSink avro_sink =
                    new Kafka09AvroTableSink(targetTopic, properties, new FlinkFixedPartitioner());
            result.writeToSink(avro_sink);
            //result.writeToSink(new CsvTableSink(resultFile, "|", 1, FileSystem.WriteMode.OVERWRITE));
            env.execute("tcFlinkAvroSQL");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, DecoderException {
        String sqlState_select_01 =
                "SELECT symbol, company_name FROM test_stock";
        String sqlState_select_02 =
                "SELECT symbol, company_name, ask_size, bid_size, (ask_size + bid_size) as total FROM test_stock";
/* sqlState_select_03 output schema
curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{"schema": "{ \"type\": \"record\",\"name\": \"stock_out\",\"fields\":[{\"name\": \"symbol\", \"type\": \"string\"},{\"name\": \"company_name\", \"type\": \"string\"},{\"name\": \"bid_size\", \"type\": \"int\"}]}"}' \
http://localhost:8081/subjects/stock_out/versions
*/
        String sqlState_select_03 =
                "SELECT symbol, company_name, bid_size FROM test_stock where bid_size > 50";

/* sqlState_select_04 output schema
curl -X POST -i -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"schema": "{ \"type\": \"record\",\"name\": \"stock_int\",\"fields\":[{\"name\": \"symbol\", \"type\": \"string\"},{\"name\": \"total_bids\", \"type\": \"int\"}]}"}' \
http://localhost:8081/subjects/stock_int/versions
*/
        String sqlState_select_04 =
                "SELECT symbol, sum(bid_size) as total_bids FROM test_stock group by symbol";

        tcFlinkAvroSQL("localhost:8002", "test_stock", "stock_int_test", sqlState_select_04);
        // Test: kafka-avro-console-consumer --zookeeper localhost:2181 --topic stock_int_test --from-beginning
    }

}
