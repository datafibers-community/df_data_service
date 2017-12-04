package com.datafibers.test_tool;

import com.datafibers.flinknext.Kafka010AvroTableSource;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.SchemaRegistryClient;
import org.apache.commons.codec.DecoderException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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

        String jarPath = "C:/Users/dadu/Coding/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar";
        //String jarPath = "/Users/will/Documents/Coding/GitHub/df_data_service/target/df-data-service-1.1-SNAPSHOT-fat.jar";
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
            tableEnv.registerTableSource(srcTopicList[i], new Kafka010AvroTableSource(srcTopicList[i], properties));
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
        //TODO since sink avro only support primary type, we cannot select other unsupport type in the select statement

        final String SQLSTATE_PROJECT_01 =
                "SELECT symbol, company_name, ask_size, bid_size, (ask_size + bid_size) as total FROM test_stock";

        final String SQLSTATE_PROJECT_02 =
                "SELECT symbol, company_name, bid_size FROM test_stock where bid_size > 50";

        final String SQLSTATE_AGG_01 =
                "SELECT symbol, sum(bid_size) as total_bids FROM test_stock group by symbol";

        // TODO not support yet
        final String SQLSTATE_AGG_02 =
                "SELECT symbol, rowtime FROM test_stock";

        // TODO not support yet
        final String SQLSTATE_AGG_03 =
                "SELECT symbol, cast(refresh_time as timestamp) as source_time FROM test_stock";

        // TODO not support yet by table source
        final String SQLSTATE_AGG_04 =
                "SELECT symbol, sum(bid_size) as total_bids FROM test_stock group by TUMBLE(rowtime, INTERVAL '1' MINUTE), symbol";

        final String SQLSTATE_AGG_05 =
                "SELECT distinct symbol, bid_size FROM test_stock";

        // TODO not support yet by table source
        final String SQLSTATE_AGG_06 =
                "SELECT COUNT(bid_size) OVER (PARTITION BY symbol ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM test_stock";

        final String SQLSTATE_AGG_07 =
                "SELECT symbol, sum(bid_size) as total_bids FROM test_stock group by symbol having sum(bid_size) > 1000";

        final String SQLSTATE_UNION_01 =
                "SELECT symbol, bid_size FROM test_stock where symbol = 'FB' union all SELECT symbol, bid_size FROM test_stock where symbol = 'SAP'";

        final String SQLSTATE_UNION_HAVING_GROUPBY_01 =
                "SELECT symbol, sum(bid_size) as total_bids FROM (SELECT symbol, bid_size FROM test_stock where symbol = 'FB' union all SELECT symbol, bid_size FROM test_stock2 where symbol = 'SAP') group by symbol having sum(bid_size) > 1000";


        tcFlinkAvroSQL("localhost:8002", "test_stock", "SQLSTATE_UNION_01", SQLSTATE_UNION_01);
        // Test: kafka-avro-console-consumer --zookeeper localhost:2181 --topic stock_int_test --from-beginning
        // To test it locally, remove the jar from Flink Rest API.
    }

}
