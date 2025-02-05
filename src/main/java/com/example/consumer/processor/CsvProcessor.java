package com.example.consumer.processor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

@Component
public class CsvProcessor {

    public GenericRecord processCsv(byte[] data, char separatorChar, char quoteChar, char escapeChar, String schemaFileLocation, List<String> filterKeys) {
        try {
            // Handle null filterKeys by assigning an empty list
            if (filterKeys == null) {
                filterKeys = new ArrayList<>();
            }

            // Convert bytes to String
            String csvString = new String(data);

            // Parse the CSV string using Apache Commons CSV with custom format
            CSVFormat format = CSVFormat.Builder.create()
                    .setDelimiter(separatorChar)
                    .setQuote(quoteChar)
                    .setEscape(escapeChar)
                    .setSkipHeaderRecord(false)
                    .build();

            CSVParser parser = format.parse(new StringReader(csvString));

            for (CSVRecord csvRecord: parser) {
                // Convert CSV record to Avro, applying filtering
                GenericRecord avroRecord = convertToAvro(csvRecord, schemaFileLocation, filterKeys);
                System.out.println("Processed CSV message into Avro: " + avroRecord);
                return avroRecord; // Return the first processed record
            }
        } catch (Exception e) {
            System.err.println("Error processing CSV: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        System.err.println("No valid Avro record was produced. Exiting...");
        System.exit(1);
        return null;
    }

    private GenericRecord convertToAvro(CSVRecord csvRecord, String schemaFileLocation, List<String> filterKeys) throws IOException {
        // Generate a GenericRecord based on the Avro schema
        Schema schema = new Schema.Parser().parse(new File(schemaFileLocation));
        Schema new_schema = filterSchema(schema, filterKeys);

        GenericRecord record = new GenericData.Record(new_schema);

        // Map CSV fields to Avro fields (ensure the order matches your Avro schema)
        try {
            // Dynamically map CSV fields to Avro fields based on the schema
            for (Schema.Field field : schema.getFields()) {
                String fieldName = field.name();

                if (filterKeys.contains(fieldName)) {
                    continue;
                }

                // Get the corresponding CSV value
                String csvValue = csvRecord.get(field.pos());

                // Parse the CSV value into the appropriate type for the Avro field
                Object value = parseFieldValue(field.schema(), csvValue.trim());
                record.put(fieldName, value);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error mapping CSV to Avro record", e);
        }

        return record;
    }

    private Object parseFieldValue(Schema fieldSchema, String value) {
        // Handle UNION type fields
        if (fieldSchema.getType() == Schema.Type.UNION) {
            for (Schema subSchema : fieldSchema.getTypes()) {
                if (subSchema.getType() == Schema.Type.NULL && (value == null || value.isEmpty())) {
                    return null;
                }
                try {
                    return parseFieldValue(subSchema, value);
                } catch (Exception ignored) {
                    // Continue trying other types in the UNION
                }
            }
            throw new IllegalArgumentException("Value '" + value + "' does not match any types in UNION: " + fieldSchema.getTypes());
        }

        return switch (fieldSchema.getType()) {
            case STRING -> value;
            case INT -> Integer.parseInt(value);
            case LONG -> Long.parseLong(value);
            case FLOAT -> Float.parseFloat(value);
            case DOUBLE -> Double.parseDouble(value);
            case BOOLEAN -> Boolean.parseBoolean(value);
            default -> throw new IllegalArgumentException("Unsupported Avro field type: " + fieldSchema.getType());
        };
    }

    private Schema filterSchema(Schema originalSchema, List<String> filterKeys) {
        Schema filteredSchema = Schema.createRecord(
                originalSchema.getName(),
                originalSchema.getDoc(),
                originalSchema.getNamespace(),
                originalSchema.isError());

        // Create a list of filtered fields, ensuring no duplicate fields
        List<Schema.Field> filteredFields = originalSchema.getFields().stream()
                .filter(field -> !filterKeys.contains(field.name()))
                .map(field -> new Schema.Field(
                        field.name(),
                        field.schema(),
                        field.doc(),
                        field.defaultVal()))
                .collect(Collectors.toList());

        filteredSchema.setFields(filteredFields);

        return filteredSchema;
    }
}
