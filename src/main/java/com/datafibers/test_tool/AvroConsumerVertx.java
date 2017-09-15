package com.datafibers.test_tool;

        import io.confluent.kafka.serializers.KafkaAvroDeserializer;
        import io.vertx.core.AbstractVerticle;
        import io.vertx.core.DeploymentOptions;
        import io.vertx.core.Vertx;
        import io.vertx.core.json.JsonObject;
        import io.vertx.kafka.client.consumer.KafkaConsumer;
        import org.apache.kafka.clients.consumer.ConsumerConfig;

        import java.util.ArrayList;
        import java.util.Properties;


/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
public class AvroConsumerVertx extends AbstractVerticle {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new AvroConsumerVertx());
    }


    @Override
    public void start() throws Exception {
        System.out.println("Test");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put("schema.registry.url", "http://localhost:8002"); //TODO may not needed
//            props.put("bootstrap.servers", kafka_server_host_and_port);
//            props.put("group.id", ConstantApp.DF_CONNECT_KAFKA_CONSUMER_GROUP_ID);
//            props.put("schema.registry.url", schema_registry_host_and_port);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
        ArrayList<JsonObject> responseList = new ArrayList<JsonObject>();

        consumer.handler(record -> {// TODO handler does not work
            System.out.println("Processing value=" + record.record().value() +
                    ",partition=" + record.record().partition() + ",offset=" + record.record().offset());
            responseList.add(new JsonObject()
                    .put("offset", record.record().offset())
                    .put("value", record.record().value().toString()));
            if(responseList.size() >= 10 ) {
                consumer.pause();
                consumer.commit();
                consumer.close();
            }
        });
        String topic = "test_stock";
        // Subscribe to a single topic
        consumer.subscribe(topic, ar -> {
            if (ar.succeeded()) {
                System.out.println("topic " + topic + " is subscribed");
            } else {
                System.out.println("Could not subscribe " + ar.cause().getMessage());
            }
        });

    }

    @Override
    public void stop() throws Exception {

    }
}

