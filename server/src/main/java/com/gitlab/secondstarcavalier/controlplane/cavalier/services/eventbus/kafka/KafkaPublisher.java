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

        // Fail fast when the broker is unreachable. Default delivery.timeout.ms
        // is 120s — events silently buffer for 2 minutes before the caller
        // learns they failed (observed in drill 7: 8 events expired after 120s).
        // 30s surfaces failures before gRPC deadlines expire, giving the caller
        // a chance to retry or propagate the error.
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        // request.timeout.ms must be < delivery.timeout.ms for the producer to
        // distinguish "broker slow" from "total delivery budget exhausted".
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        // max.block.ms limits how long producer.send() blocks waiting for
        // metadata (topic autocreation, leader election). Default is 60s;
        // 15s surfaces metadata failures before the delivery timeout.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        // Retry backoff — same rationale as consumer: don't hammer a
        // struggling controller with metadata requests.
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
        props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "30000");

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
