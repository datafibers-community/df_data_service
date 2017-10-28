package com.datafibers.test_tool;

import com.datafibers.util.KafkaAdminClient;

public class KafkaClientTest {

    public static void main(String[] args) {
        KafkaAdminClient.listTopics("localhost:9092")
                        .forEach(System.out::println);

        KafkaAdminClient.createTopic("localhost:9092", "test", 1, 1);

        KafkaAdminClient.listTopics("localhost:9092")
                .forEach(System.out::println);
        //new KafkaAdminClient().deleteTopics("my_topic");
        //System.out.println(new KafkaAdminClient().existsTopic("df_meta"));
    }
}
