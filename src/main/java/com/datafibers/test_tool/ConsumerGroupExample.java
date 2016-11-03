package com.datafibers.test_tool;

/**
 * Created by duw3 on 11/2/2016.
 */
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by  on 9/1/15.
 */
public class ConsumerGroupExample {
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic){
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId){
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public void shutdown(){
        if (consumer!=null) consumer.shutdown();
        if (executor!=null) executor.shutdown();
        System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
        try{
            if(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)){

            }
        }catch(InterruptedException e){
            System.out.println("Interrupted");
        }

    }


    public void run(int a_numThreads){
        //Make a map of topic as key and no. of threads for that topic
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        //Create message streams for each topic
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        //initialize thread pool
        executor = Executors.newFixedThreadPool(a_numThreads);
        //start consuming from thread
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }
    public static void main(String[] args) {
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);

        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        example.run(threads);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();
    }


}