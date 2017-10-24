package com.datafibers.util;

import com.datafibers.flinknext.Kafka010AvroTableSource;
import com.datafibers.flinknext.Kafka09AvroTableSink;
import org.apache.commons.codec.DecoderException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import java.io.IOException;
import java.util.Properties;

/**
 * Flink Client to Submit SQL job through Flink Rest API
 */
public class FlinkAvroSQLClient {

    public static void tcFlinkAvroSQL(String KafkaServerHostPort, String SchemaRegistryHostPort,
                                      String srcTopic, String targetTopic,
                                      String consumerGroupId, String sinkKeys, String sqlState) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        Properties properties = new Properties();
        properties.setProperty(ConstantApp.PK_KAFKA_HOST_PORT.replace("_", "."), KafkaServerHostPort);
        properties.setProperty(ConstantApp.PK_KAFKA_CONSUMER_GROURP, consumerGroupId);
        properties.setProperty(ConstantApp.PK_KAFKA_SCHEMA_REGISTRY_HOST_PORT.replace("_", "."), SchemaRegistryHostPort);
        properties.setProperty(ConstantApp.PK_FLINK_TABLE_SINK_KEYS, sinkKeys);

        String[] srcTopicList = srcTopic.split(",");
        for (int i = 0; i < srcTopicList.length; i++) {
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_INPUT, srcTopicList[i]);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_INPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT) + "");
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_INPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_INPUT).toString());
            tableEnv.registerTableSource(srcTopic, new Kafka010AvroTableSource(srcTopicList[i], properties));
        }

        try {
            Table result = tableEnv.sql(sqlState);
            SchemaRegistryClient.addSchemaFromTableResult(SchemaRegistryHostPort, targetTopic, result);
            // delivered properties for sink
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, targetTopic);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT) + "");
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());

            Kafka09AvroTableSink avro_sink =
                    new Kafka09AvroTableSink(targetTopic, properties, new FlinkFixedPartitioner());
            result.writeToSink(avro_sink);
            env.execute("DF_FlinkSQL_Client_" + srcTopic + "-" + targetTopic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, DecoderException {
        //tcFlinkAvroSQL("localhost:9092", "localhost:8081", "test_stock", "SQLSTATE_UNION_01", "symbol", "consumergroupid", SQLSTATE_UNION_01);
        tcFlinkAvroSQL(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);

    }
}
