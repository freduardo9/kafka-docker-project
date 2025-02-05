package com.example.consumer.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.threeten.bp.Duration;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.io.IOException;

@ConfigurationProperties(prefix = "pubsub")
@Validated
@Getter
@Setter
public class PubSubConfiguration {
    @JsonProperty("project_id")
    @NotNull
    @NotBlank
    private String projectId;

    @JsonProperty("topic_id")
    @NotNull
    @NotBlank
    private String topicId;

    @NotNull
    @NotBlank
    private String endpoint;

    @JsonProperty("io_threads")
    private Integer ioThreads = 40;

    @JsonProperty("batch_request_bytes_threshold")
    private long requestBytesThreshold = 4000;

    @JsonProperty("message_count_batch_size")
    private long messageCountBatchSize = 1000;

    @JsonProperty("publish_delay_threshold")
    private Integer publishDelayThreshold = 1;

    public Publisher pubSubClient() throws IOException {
        // Build the topic name
        TopicName topicName = TopicName.of(projectId, topicId);

        // Provides an executor service for processing messages. The default
        // `executorProvider` used by the publisher has a default count of
        // 5 * the number of processors available to the Java virtual machine.
        ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(ioThreads).build();

        // Batch settings control how the publisher batches messages
        // messageCount = 1000
        // messageSize = 10 MB
        // publishDelay = 1 sec
        Duration publishDelayThresholdDuration = Duration.ofMillis(publishDelayThreshold);

        BatchingSettings batchingSettings =
                BatchingSettings.newBuilder()
                        .setElementCountThreshold(messageCountBatchSize)
                        .setRequestByteThreshold(requestBytesThreshold)
                        .setDelayThreshold(publishDelayThresholdDuration)
                        .build();

        return Publisher.newBuilder(topicName)
                .setEndpoint(endpoint)
                .setExecutorProvider(executorProvider)
                .setBatchingSettings(batchingSettings)
                .build();
    }
}
