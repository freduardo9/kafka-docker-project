package com.example.consumer;

import com.example.consumer.config.KafkaConfiguration;

import com.example.consumer.config.MessageConfiguration;
import com.example.consumer.config.PubSubConfiguration;
import com.example.consumer.processor.MessageProcessor;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

@SpringBootApplication
@EnableConfigurationProperties({KafkaConfiguration.class, MessageConfiguration.class})
public class ConsumerApp {
    public static void main(String[] args) throws IOException {

        ApplicationContext context = SpringApplication.run(ConsumerApp.class, args);

        KafkaConfiguration kafkaConfiguration = context.getBean(KafkaConfiguration.class);
        MessageConfiguration messageConfiguration = context.getBean(MessageConfiguration.class);
        PubSubConfiguration pubSubConfiguration = context.getBean(PubSubConfiguration.class);

        MessageProcessor messageProcessor = context.getBean(MessageProcessor.class);
        Publisher pubSubPublisher = pubSubConfiguration.pubSubClient();

        try (KafkaConsumer<byte[], byte[]> consumer = kafkaConfiguration.consumer()) {
            consumer.subscribe(Collections.singletonList(kafkaConfiguration.getTopic()));
            System.out.println("Consumer configured to read from topic: " + kafkaConfiguration.getTopic());
            System.out.println("Consumer started. Waiting for messages...");

            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(kafkaConfiguration.getPollTimeMs()));
                if (records.isEmpty()) {
                    System.out.println("No messages received...");
                } else {
                    for (ConsumerRecord<byte[], byte[]> record: records) {
                        // Format to AvroRecord
                        GenericRecord avroRecord = messageProcessor.processMessage(kafkaConfiguration.getTopic(), messageConfiguration.getFileFormat().toString(), record.value());
                        // Convert AvroRecord to ByteString
                        ByteString avroByteString = ByteString.copyFrom(avroRecord.toString().getBytes());
                        // Publish to Pub/Sub
                        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(avroByteString).build();
                        pubSubPublisher.publish(pubsubMessage);
                        System.out.println("Published message to Pub/Sub: " + avroRecord);
                    }
                }
            }
        }
    }
}