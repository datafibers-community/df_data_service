package com.datafibers.test_tool;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.util.*;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * Created by will on 2017-09-12.
 */
public class AvroConsumerTest {

    private static final Properties props = new Properties();

    static {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group1");
        props.put("schema.registry.url", "http://localhost:8002");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    }

    public static void consumeAll(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            if (!records.isEmpty()) {
                consumer.commitSync();
                break;
            }
        }
    }

    public static void consumeFromTime(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        boolean flag = true;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            if (flag) {
                Set<TopicPartition> assignments = consumer.assignment();
                Map<TopicPartition, Long> query = new HashMap<>();
                for (TopicPartition topicPartition : assignments) {
                    query.put(
                            topicPartition,
                            Instant.now().minus(5, MINUTES).toEpochMilli());
                }

                Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

                result.entrySet()
                        .stream()
                        .forEach(entry ->
                                consumer.seek(
                                        entry.getKey(),
                                        Optional.ofNullable(entry.getValue())
                                                .map(OffsetAndTimestamp::offset)
                                                .orElse(new Long(0))));

                flag = false;
            }

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            if (!records.isEmpty()) break;
        }


    }

    public static void consumeBatch(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 10;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                consumer.commitSync();
                buffer.forEach(System.out::println);
                buffer.clear();
                break;
            }
        }
    }

    public static void main(String[] args) {
        AvroConsumerTest.consumeFromTime("test_stock");
    }
}
