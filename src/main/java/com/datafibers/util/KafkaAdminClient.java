package com.datafibers.util;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This class contains static methods to operate on queues, allowing queue
 * creation and deletion, checking whether queues exist on the broker, and
 * listing all queues on the broker. This new admin API is available after kafka 0.10.1.0.
 * In new version of API, zookeeper are no longer required.
 */
public class KafkaAdminClient
{
    private static final String DEFAULT_BOOTSTRAP_SERVERS_HOST_PORT = "localhost:9092";

    public static AdminClient createAdminClient (String BOOTSTRAP_SERVERS_HOST_PORT) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_HOST_PORT);
        return AdminClient.create(props);
    }

    /**
     * Given its name, checks if a topic exists on the Kafka broker.
     *
     * @param topicName The name of the topic.
     *
     * @return <code>true</code> if the topic exists on the broker,
     *         <code>false</code> if it doesn't.
     */
    static boolean existsTopic (AdminClient adminClient, String topicName) {
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            return listTopicsResult.names().get().contains(topicName);

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Creates new topics, which remain persistent on the Kafka broker.
     *
     * @param topicName The names of the topic separated by ,.
     * @param partitions The number of partitions in the topic.
     * @param replication The number of brokers to host the topic.
     */
    public static void createTopic (String BOOTSTRAP_SERVERS_HOST_PORT, String topicName, int partitions, int replication) {
        AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS_HOST_PORT);
        if(!existsTopic(adminClient, topicName)) {
            NewTopic topic = new NewTopic(topicName, partitions, (short)replication);
            CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(topic));
            try {
                createTopicsResult.all().get();
                // real failure cause is wrapped inside the raised ExecutionException
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            } finally {
                adminClient.close();
            }
        } else {
            System.out.println(topicName + " already exists and will not create");
        }
    }

    public static void createTopic (String topicName, int partitions, int replication) {
        createTopic(DEFAULT_BOOTSTRAP_SERVERS_HOST_PORT, topicName, partitions, replication);
    }

    public static void createTopic (String topicName) {
        createTopic(DEFAULT_BOOTSTRAP_SERVERS_HOST_PORT, topicName, 1, 1);
    }

    /**
     * Given its name, deletes a topic on the Kafka broker.
     *
     * @param topicsName The name of the topic.
     */
    public static void deleteTopics (String BOOTSTRAP_SERVERS_HOST_PORT, String topicsName) {
        AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS_HOST_PORT);
        // remove topic which is not already exists
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicsName.split(",")));
        try {
            deleteTopicsResult.all().get();
            // real failure cause is wrapped inside the raised ExecutionException
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                System.err.println("Topic not exists !!");
            } else if (e.getCause() instanceof TimeoutException) {
                System.err.println("Timeout !!");
            }
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
    }

    public static void deleteTopics (String topicsName) {
        deleteTopics(DEFAULT_BOOTSTRAP_SERVERS_HOST_PORT, topicsName);
    }

    /**
     * Lists all topics on the Kafka broker.
     */
    public static Set<String> listTopics (String BOOTSTRAP_SERVERS_HOST_PORT) {
        AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS_HOST_PORT);
        try {
            return adminClient.listTopics().names().get();
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return null;
    }

    public static Set<String> listTopics () {
       return listTopics(DEFAULT_BOOTSTRAP_SERVERS_HOST_PORT);
    }

    /**
     * Given its name, deletes a topic on the Kafka broker.
     *
     * @param topicsName The name of the topic.
     */
    public static void describeTopics (String BOOTSTRAP_SERVERS_HOST_PORT, String topicsName) {
        AdminClient adminClient = createAdminClient(BOOTSTRAP_SERVERS_HOST_PORT);
        // remove topic which is not already exists
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topicsName.split(",")));
        try {
            describeTopicsResult.all().get().forEach((key, value) -> {
                System.out.println("Key : " + key + " Value : " + value);
            });
            // real failure cause is wrapped inside the raised ExecutionException
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                System.err.println("Topic not exists !!");
            } else if (e.getCause() instanceof TimeoutException) {
                System.err.println("Timeout !!");
            }
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
    }

}