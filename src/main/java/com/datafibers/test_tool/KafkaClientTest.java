package com.datafibers.test_tool;

import com.datafibers.util.KafkaAdminClient;

public class KafkaClientTest {

    public static void main(String[] args) {
        KafkaAdminClient.listTopics().forEach(System.out::println);
        //new KafkaAdminClient().deleteTopics("my_topic");
        //System.out.println(new KafkaAdminClient().existsTopic("df_meta"));
    }
}
