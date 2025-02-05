package com.example.consumer.processor;

import com.example.consumer.config.MessageConfiguration;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

@Component
public class MessageProcessor {
    private final JsonProcessor jsonProcessor;
    private final AvroProcessor avroProcessor;
    private final CsvProcessor csvProcessor;
    private final MessageConfiguration messageConfiguration;

    public MessageProcessor(JsonProcessor jsonProcessor, AvroProcessor avroProcessor, CsvProcessor csvProcessor, MessageConfiguration messageProperties) {
        this.jsonProcessor = jsonProcessor;
        this.avroProcessor = avroProcessor;
        this.csvProcessor = csvProcessor;
        this.messageConfiguration = messageProperties;
    }

    public GenericRecord processMessage(String topic, String fileFormat, byte[] value) {
        try {
            switch (fileFormat) {
                case "JSON":
                    return jsonProcessor.processJson(
                            value,
                            messageConfiguration.getSchemaFileLocation(),
                            messageConfiguration.getFilteredKeys()
                    );
                case "AVRO":
                    return avroProcessor.processAvro(
                            value,
                            messageConfiguration.getSchemaFileLocation(),
                            messageConfiguration.getFilteredKeys()
                    );
                case "CSV":
                    return csvProcessor.processCsv(
                            value,
                            messageConfiguration.getSeparatorChar(),
                            messageConfiguration.getQuoteChar(),
                            messageConfiguration.getEscapeChar(),
                            messageConfiguration.getSchemaFileLocation(),
                            messageConfiguration.getFilteredKeys()
                    );
                default:
                    System.err.println("Unsupported file format: " + fileFormat);
                    System.exit(1);
                    return null;
            }
        } catch (Exception e) {
            System.err.println("Error processing message from topic " + topic + ": " + e.getMessage());
            System.exit(1);
        }
        System.err.println("Error processing message from topic");
        System.exit(1);
        return null;
    }
}