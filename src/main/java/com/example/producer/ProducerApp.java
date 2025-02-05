package com.example.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class ProducerApp {
    public static void main(String[] args) throws IOException {
        System.out.println("Executing the Netflix Streaming Activity Producer App");

        String fileFormat = "CSV";
        String topic = "netflix-csv-topic1";

        Schema schema = new Schema.Parser().parse(new File("src/main/resources/actual.avsc"));

        // Kafka Producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (fileFormat.equals("AVRO")) {
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("schema.registry.url", "http://localhost:8081");
        } else {
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }

        KafkaProducer<String, Object> producer = new KafkaProducer<>(props);

        // Arrays for randomized data
        String[] actions = {"start", "pause", "resume", "stop", "complete"};
        String[] devices = {"smart_tv", "mobile", "desktop", "tablet"};
        Random random = new Random();

        try {
            int i = 0; // Message counter
            while (true) {
                String userId = "USER" + (1000 + random.nextInt(9000));
                String movieId = "MOVIE" + (100 + random.nextInt(900));
                String action = actions[random.nextInt(actions.length)];
                int playbackPosition = random.nextInt(6000); // Playback position in seconds
                String device = devices[random.nextInt(devices.length)];
                long timestamp = Instant.now().toEpochMilli();

                // Serialize data based on the file format
                Object value;
                if (fileFormat.equalsIgnoreCase("avro")) {
                    // Create a GenericRecord for Avro
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("user_id", userId);
                    record.put("movie_id", movieId);
                    record.put("action", action);
                    record.put("playback_position", playbackPosition);
                    record.put("timestamp", timestamp);
                    record.put("device", device);
                    value = record;
                } else if (fileFormat.equalsIgnoreCase("csv")) {
                    // Create a CSV string
                    value = String.format("%s,%s,%s,%d,%s,%s", userId, movieId, action, playbackPosition, timestamp, device);
                } else if (fileFormat.equalsIgnoreCase("json")) {
                    // Create a JSON string
                    value = String.format(
                            "{\"user_id\":\"%s\",\"movie_id\":\"%s\",\"action\":\"%s\",\"playback_position\":%d,\"timestamp\":\"%s\",\"device\":\"%s\"}",
                            userId, movieId, action, playbackPosition, timestamp, device
                    );
                } else {
                    throw new IllegalArgumentException("Unsupported file format: " + fileFormat);
                }


                // Produce record to Kafka
                ProducerRecord<String, Object> record = new ProducerRecord<>(topic, userId, value);
                producer.send(record);

                // Print the produced message
                System.out.printf("Produced: key=%s, value=%s%n", userId, value);

                i++;

                // Add a delay between messages (e.g., 1 second)
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.err.println("Producer interrupted: " + e.getMessage());
        } finally {
            producer.close();
        }
    }
}