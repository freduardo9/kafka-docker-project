package com.example.consumer.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class JsonProcessor {

    public GenericRecord processJson(byte[] data, String schemaFileLocation, List<String> filterKeys) throws Exception {
        // Convert bytes to String
        String jsonString = new String(data);
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println("Processing JSON record: " + jsonString);
        JsonNode jsonNode = objectMapper.readTree(jsonString);

        // Filter unwanted keys from JSON
        if (jsonNode.isObject()) {
            ((ObjectNode) jsonNode).remove(filterKeys);
        }

        GenericRecord avroRecord = convertToAvro(jsonNode, schemaFileLocation, filterKeys);
        System.out.println("Processed JSON message into Avro: " + avroRecord);
        return avroRecord;
    }

    private GenericRecord convertToAvro(JsonNode jsonNode, String schemaFileLocation, List<String> filterKeys) throws IOException {
        Schema schema = new Schema.Parser().parse(new File(schemaFileLocation));
        Schema new_schema = filterSchema(schema, filterKeys);
        GenericRecord avroRecord = new GenericData.Record(new_schema);

        jsonNode.fieldNames().forEachRemaining(fieldName -> {
            JsonNode value = jsonNode.get(fieldName);
            avroRecord.put(fieldName, value.asText());
        });

        return avroRecord;
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
