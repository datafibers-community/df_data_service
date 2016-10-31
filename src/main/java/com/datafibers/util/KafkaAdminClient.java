package com.datafibers.util;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Properties;

/**
 * This class contains static methods to operate on queues, allowing queue
 * creation and deletion, checking whether queues exist on the broker, and
 * listing all queues on the broker.
 *
 * A topic represents a queue.
 *
 * Sample Usage
 *         KafkaAdminClient admin = new KafkaAdminClient();
 *         admin.createTopics("topic1, topic2").closeAdminClient();
 */
public class KafkaAdminClient
{
    private static final int DEFAULT_SESSION_TIMEOUT = 10 * 1000;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 8 * 1000;
    private static String ZOOKEEPER_CONNECT;
    private static ZkClient zkClient;
    private static ZkUtils zkUtils;

    /**
     * Opens a new ZooKeeper client to access the Kafka broker.
     */
    public KafkaAdminClient () {
        this("localhost:2181");
    }

    public KafkaAdminClient (String ZOOKEEPER_CONNECT_STRING) {
        ZOOKEEPER_CONNECT = ZOOKEEPER_CONNECT_STRING;
        this.zkClient = new ZkClient(ZOOKEEPER_CONNECT_STRING, DEFAULT_SESSION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT,
                ZKStringSerializer$.MODULE$);
        boolean isSecureCluster = false;
        this.zkUtils = new ZkUtils(this.zkClient, new ZkConnection(ZOOKEEPER_CONNECT), isSecureCluster);
    }

    /**
     * Given its name, checks if a topic exists on the Kafka broker.
     *
     * @param name The name of the topic.
     *
     * @return <code>true</code> if the topic exists on the broker,
     *         <code>false</code> if it doesn't.
     */
    public static boolean existsTopic (String name) {
        boolean topicExists = AdminUtils.topicExists(zkUtils, name.trim());
        return topicExists;
    }

    /**
     * Creates new topics, which remain persistent on the Kafka broker.
     *
     * @param names The names of the topic separated by ,.
     * @param partitions The number of partitions in the topic.
     * @param replication The number of brokers to host the topic.
     */
    public KafkaAdminClient createTopics (String names, int partitions, int replication) {
        for (String name: names.trim().replace(" ", "").split(",")) {
            if (existsTopic(name))
                continue;
            AdminUtils.createTopic(zkUtils, name, partitions, replication, new Properties());
        }
        return this;
    }

    public KafkaAdminClient createTopics (String names) {
        return createTopics(names, 1, 1);
    }

    /**
     * Given its name, deletes a topic on the Kafka broker.
     *
     * @param names The name of the topic.
     */
    public KafkaAdminClient deleteTopics (String names) {
        for (String name: names.trim().replace(" ", "").split(",")) {
            if (!existsTopic(name))
                return this;
            AdminUtils.deleteTopic(zkUtils, name);
        }
        return this;
    }

    /**
     * Lists all topics on the Kafka broker.
     */
    public void listTopics () {
        List<String> brokerTopics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
        for (String topic: brokerTopics)
            System.out.println(topic);
    }

    /**
     * Close the adminClient for Kafka
     */
    public void closeAdminClient() {
        zkClient.close();
    }
}