package com.example.consumer.config;

import com.example.consumer.enums.AutoOffsetReset;
import com.example.consumer.enums.GroupProtocol;
import com.example.consumer.enums.SecurityProtocol;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "kafka")
@Validated
@Getter
@Setter
public class KafkaConfiguration {
    @NotNull
    @NotBlank
    private String broker;

    @NotNull
    @NotBlank
    private String topic;

    @JsonProperty("fetch_min_bytes")
    @Min(value = 1)
    private Integer fetchMinBytes = 1;

    @NotBlank
    @NotNull
    private String group;

    @JsonProperty("group_protocol")
    @NotNull
    private GroupProtocol groupProtocol = GroupProtocol.CLASSIC;

    @JsonProperty("heartbeat_interval_ms")
    @Min(value = 3000)
    private Integer heartbeatIntervalMs = 3000;

    @JsonProperty("max_partition_fetch_bytes")
    @Min(value = 1)
    private Integer maxPartitionFetchBytes = 1048576;

    @JsonProperty("session_timeout_ms")
    @Min(value = 1)
    private Integer sessionTimeoutMs = 45000;

    @JsonProperty("allow_auto_create_topics")
    private boolean allowAutoCreateTopics = true;

    @JsonProperty("auto_offset_reset")
    private AutoOffsetReset autoOffsetReset = AutoOffsetReset.latest;

    @JsonProperty("connections_max_idle_ms")
    private long connectionsMaxIdleMs = 540000;

    @JsonProperty("enable_auto_commit")
    private boolean enableAutoCommit = true;

    @JsonProperty("fetch_max_bytes")
    private Integer fetchMaxBytes = 5000000;

    @JsonProperty("max_poll_records")
    @Min(value = 1)
    private Integer maxPollRecords = 1000;

    @JsonProperty("poll_time_ms")
    @Min(value = 1000)
    private Integer pollTimeMs = 1000;

    @JsonProperty("auto_commit_interval_ms")
    @Min(value = 0, message = "autoCommitIntervalMs must be 0 or greater")
    private Integer autoCommitIntervalMs = 5000; // Default: 5000ms

    @JsonProperty("security_protocol")
    @NotNull
    private SecurityProtocol securityProtocol = SecurityProtocol.SSL; // Default value

    @JsonProperty("ssl_truststore_location")
    private String sslTrustStoreLocation;

    @JsonProperty("ssl_truststore_password")
    private String sslTruststorePassword;

    @JsonProperty("ssl_keystore_location")
    private String sslKeystoreLocation;

    @JsonProperty("ssl_keystore_password")
    private String sslKeystorePassword;

    @JsonProperty("use_ssl_security")
    private boolean useSSLSecurity = false; // Default: false

    @PostConstruct
    private void validateSSLFields() {
        if (useSSLSecurity) {
            StringBuilder errors = new StringBuilder();

            if (sslTrustStoreLocation == null || sslTrustStoreLocation.isBlank()) {
                errors.append("Kafka property: 'ssl_truststore_location' must be set when 'use_ssl_security' is True.\n");
            }

            if (sslTruststorePassword == null || sslTruststorePassword.isBlank()) {
                errors.append("Kafka property: 'ssl_truststore_password' must be set when 'use_ssl_security' is True.\n");
            }

            if (sslKeystoreLocation == null || sslKeystoreLocation.isBlank()) {
                errors.append("Kafka property: 'ssl_keystore_location' must be set when 'use_ssl_security' is True.\n");
            }

            if (sslKeystorePassword == null || sslKeystorePassword.isBlank()) {
                errors.append("Kafka property: 'ssl_keystore_password' must be set when 'use_ssl_security' is True.\n");
            }

            if (!errors.isEmpty()) {
                throw new IllegalStateException("SSL Validation failed:\n" + errors);
            }
        }
    }

    @Override
    public String toString() {
        return "{\n" +
                "    \"broker\": \"" + broker + "\",\n" +
                "    \"topic\": \"" + topic + "\",\n" +
                "    \"fetch_min_bytes\": \"" + fetchMinBytes + "\",\n" +
                "    \"group\": \"" + group + "\",\n" +
                "    \"group_protocol\": \"" + groupProtocol + "\",\n" +
                "    \"heartbeat_interval_ms\": \"" + heartbeatIntervalMs + "\",\n" +
                "    \"max_partition_fetch_bytes\": \"" + maxPartitionFetchBytes + "\",\n" +
                "    \"session_timeout_ms\": \"" + sessionTimeoutMs + "\",\n" +
                "    \"allow_auto_create_topics\": \"" + allowAutoCreateTopics + "\",\n" +
                "    \"auto_offset_reset\": \"" + autoOffsetReset + "\",\n" +
                "    \"connections_max_idle_ms\": \"" + connectionsMaxIdleMs + "\",\n" +
                "    \"enable_auto_commit\": \"" + enableAutoCommit + "\",\n" +
                "    \"fetch_max_bytes\": \"" + fetchMaxBytes + "\",\n" +
                "    \"max_poll_records\": \"" + maxPollRecords + "\",\n" +
                "    \"auto_commit_interval_ms\": \"" + autoCommitIntervalMs + "\",\n" +
                "    \"security_protocol\": \"" + securityProtocol + "\",\n" +
                "    \"use_ssl_security\": \"" + useSSLSecurity + "\",\n" +
                "}";
    }

    public KafkaConsumer<byte[], byte[]> consumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopics);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset.toString());
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
        return new KafkaConsumer<>(props);
    }
}