package com.example.consumer.config;

import com.example.consumer.enums.FileFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.annotation.PostConstruct;
import java.util.List;

@ConfigurationProperties(prefix = "message")
@Validated
@Getter
@Setter
public class MessageConfiguration {
    @JsonProperty("file_format")
    @NotNull
    private FileFormat fileFormat;

    @JsonProperty("separator_char")
    private Character separatorChar;

    @JsonProperty("quote_char")
    private Character quoteChar;

    @JsonProperty("escape_char")
    private Character escapeChar;

    @JsonProperty("schema_file_location")
    @NotNull
    @NotBlank
    private String schemaFileLocation;

    @JsonProperty("filtered_keys")
    private List<String> filteredKeys;

    @PostConstruct
    private void validateCSVFields() {
        if (fileFormat == FileFormat.CSV) {
            StringBuilder errors = new StringBuilder();

            if (separatorChar == null) {
                errors.append("Message property: 'separator_char' must be set when 'file_format' is CSV.");
            }

            if (quoteChar == null) {
                errors.append("Message property: 'quote_char' must be set when 'file_format' is CSV.");
            }

            if (escapeChar == null) {
                errors.append("Message property: 'escape_char' must be set when 'file_format' is CSV.");
            }

            if (!errors.isEmpty()) {
                throw new IllegalStateException("CSV Field Validation failed:\n" + errors);
            }
        }
    }
}
