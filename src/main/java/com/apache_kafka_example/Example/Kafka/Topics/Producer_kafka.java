package com.apache_kafka_example.Example.Kafka.Topics;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer_kafka {

    private static final String TOPIC = "my-kafka-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static void produce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0;; i++) {
                String key = Integer.toString(i);
                String message = "this is message " + Integer.toString(i);
                producer.send(new ProducerRecord<>(TOPIC, key, message));
                System.out.println("sent msg " + key);
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
        }
    }

    public static void main(String[] args) {
        Thread producerThread = new Thread(Producer_kafka::produce);
        producerThread.start();
    }
}
