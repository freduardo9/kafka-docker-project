package com.example.consumer.processor;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class AvroProcessor {

    public GenericRecord processAvro(byte[] data, String schemaRegistryURL, List<String> filterKeys) throws IOException {
        try {
            // Handle null filterKeys by assigning an empty list
            if (filterKeys == null) {
                filterKeys = new ArrayList<>();
            }

            // Remove the first 5 bytes (magic byte + schema ID)
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int magicByte = buffer.get();
            if (magicByte != 0) {
                throw new IllegalArgumentException("Invalid magic byte!");
            }
            int schemaId = buffer.getInt();
            byte[] avroBytes = new byte[buffer.remaining()];
            buffer.get(avroBytes);

            // Parse the schema
            Schema schema = fetchSchemaFromRegistry(schemaId, schemaRegistryURL);

            // Read the Avro record
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord originalRecord = reader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroBytes), null));

            System.out.println("Original Avro message: " + originalRecord);

            // Filter schema and record
            if (!filterKeys.isEmpty()) {
                Schema filteredSchema = filterSchema(schema, filterKeys);
                GenericRecord filteredRecord = filterRecord(originalRecord, filteredSchema);
                System.out.println("Filtered avro message: " + filteredRecord);
                return filteredRecord;
            } else {
                return originalRecord;
            }
        } catch (Exception e) {
            System.err.println("Error processing Avro: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.err.println("No valid Avro record was produced. Exiting ...");
        System.exit(1);
        return null;
    }

    private Schema filterSchema(Schema schema, List<String> filterKeys) {
        // Create a filtered list of fields
        List<Schema.Field> filteredFields = schema.getFields().stream()
                .filter(field -> !filterKeys.contains(field.name()))
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
                .collect(Collectors.toList());

        // Create a new schema with the filtered fields
        Schema filteredSchema = Schema.createRecord(
                schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                schema.isError()
        );
        filteredSchema.setFields(filteredFields);

        return filteredSchema;
    }

    private Schema fetchSchemaFromRegistry(int schemaId, String schemaRegistryURL) throws IOException {
        try {
            // Fetch SchemaID from the Schema Registry
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 100);
            String schemaString = schemaRegistryClient.getSchemaById(schemaId).toString();

            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch schema from Schema Registry: " + e);
        }
    }

    private GenericRecord filterRecord(GenericRecord originalRecord, Schema filteredSchema) {
        // Create a new record using the filtered schema
        GenericRecord filteredRecord = new GenericData.Record(filteredSchema);

        // Copy only the fields present in the filtered schema
        for (Schema.Field field : filteredSchema.getFields()) {
            filteredRecord.put(field.name(), originalRecord.get(field.name()));
        }

        return filteredRecord;
    }
}
