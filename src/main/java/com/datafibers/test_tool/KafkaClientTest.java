package com.datafibers.test_tool;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaClientTest {

    public static void main(String[] args) {
        System.out.println("TestCase_New Kafka Admin Client");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);
        NewTopic topic = new NewTopic("my_topic", 2, (short)1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));

        try {
            createTopicsResult.all().get();
            // real failure cause is wrapped inside the raised ExecutionException
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof TopicExistsException) {
                System.err.println("Topic already exists !!");
            } else if (e.getCause() instanceof TimeoutException) {
                System.err.println("Timeout !!");
            }
            e.printStackTrace();
        } finally {
            adminClient.close();
        }

    }
}
