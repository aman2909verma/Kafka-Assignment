package com.knoldus;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws JsonProcessingException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.knoldus.serializer.UserSerializer");
        User std = new User();
        std.setId(1);
        std.setName("AMAN VERMA");
        std.setAge(21);

        try (KafkaProducer<String, User> producer = new KafkaProducer(properties)) {
            producer.send(new ProducerRecord<String, User>("user", std));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}