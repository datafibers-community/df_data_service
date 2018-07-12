package com.datafibers.util;

import com.datafibers.flinknext.Kafka011AvroTableSource;
import com.datafibers.flinknext.Kafka011AvroTableSink;
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
        // TODO Flink v1.5.0 job run "program-args" does use commas as separators.
        // TODO In this case, we have to replace , to ^ from UI input, pass to flink SQL client, then replace ^ back to ,
        // TODO similar use ? to replace '
        srcTopic = srcTopic.replace("^", ",");
        sqlState = sqlState.replace("^", ",").replace("?", "'");

        System.out.println("sqlState = " + sqlState);

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
            tableEnv.registerTableSource(srcTopicList[i],
                    Kafka011AvroTableSource
                    .builder()
                    .forTopic(srcTopicList[i])
                    .withKafkaProperties(properties)
                    .build());
        }

        try {
            Table result = tableEnv.sqlQuery(sqlState);
            SchemaRegistryClient.addSchemaFromTableResult(SchemaRegistryHostPort, targetTopic, result);
            // For old producer, we need to create topic-value subject as well
            SchemaRegistryClient.addSchemaFromTableResult(SchemaRegistryHostPort, targetTopic + "-value", result);

            // delivered properties for sink
            properties.setProperty(ConstantApp.PK_SCHEMA_SUB_OUTPUT, targetTopic);
            properties.setProperty(ConstantApp.PK_SCHEMA_ID_OUTPUT, SchemaRegistryClient.getLatestSchemaIDFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT) + "");
            properties.setProperty(ConstantApp.PK_SCHEMA_STR_OUTPUT, SchemaRegistryClient.getLatestSchemaFromProperty(properties, ConstantApp.PK_SCHEMA_SUB_OUTPUT).toString());

            Kafka011AvroTableSink avro_sink =
                    new Kafka011AvroTableSink(targetTopic, properties, new FlinkFixedPartitioner());
            result.writeToSink(avro_sink);
            env.execute("DF_FlinkSQL_Client_" + srcTopic + "-" + targetTopic);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, DecoderException {
        //tcFlinkAvroSQL("localhost:9092", "localhost:8002", "source_stock", "source_stock_out", "consumergroupid", "symbol", "select symbol, open_price from source_stock");
        tcFlinkAvroSQL(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        // System.out.println("where a = 'd'".replace("'", "%27").replace("%27", "'"));
    }
}
