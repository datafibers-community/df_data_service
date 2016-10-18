package com.datafibers.api;

import com.datafibers.service.DFDataProcessor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * API to help with Flink UDF
 */
public class FlinkUDF {

    public boolean transform_engine_flink_enabled() {
        return DFDataProcessor.transform_engine_flink_enabled;
    }

    public DataStream<String> getFlinkStreamFromKafkaTopic (String inputTopic) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", DFDataProcessor.kafka_server_host_and_port);
        DataStream<String> stream = DFDataProcessor
                .env
                .addSource(new FlinkKafkaConsumer09<>(inputTopic, new SimpleStringSchema(), properties));
        return stream;
    }

    // TODO: Get Flink Run Time

    // TODO: Read from topic

    // TODO: Write to the topic

    // TODO: Del topic - optional
}
