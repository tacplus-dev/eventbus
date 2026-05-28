// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a KafkaProducer for serialized protobuf messages.
 * Uses acks=all and lz4 compression for durability and efficiency.
 */
public class KafkaPublisher implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);
    private final KafkaProducer<String, byte[]> producer;

    public KafkaPublisher(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Publishes a serialized protobuf message to the given topic with headers.
     * Returns partition and offset on success.
     */
    public CompletableFuture<RecordMetadata> publish(String topic, String key,
                                                      Map<String, String> headers,
                                                      byte[] value) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                record.headers().add(entry.getKey(),
                        entry.getValue().getBytes(StandardCharsets.UTF_8));
            }
        }
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(metadata);
                }
            });
        } catch (Throwable t) {
            // producer.send can throw synchronously (e.g. IllegalStateException
            // if the producer is closing). Without this catch the future would
            // be silently abandoned — callers would never see success or error.
            future.completeExceptionally(t);
        }
        return future;
    }

    @Override
    public void close() {
        producer.close();
    }
}
