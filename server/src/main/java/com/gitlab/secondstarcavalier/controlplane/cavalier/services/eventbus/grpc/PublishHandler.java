// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.libs.servicemanager.Auth;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.model.types.services.eventbus.Eventbus;
import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Generic bidi-streaming publish handler for typed event-bus RPCs.
 * Receives typed protobuf messages from the client, serializes to bytes,
 * and publishes to the corresponding Kafka topic.
 *
 * Caller identity claims are extracted from the bearer token and attached
 * as Kafka message headers. For C2S topics, the generated code passes an
 * {@link IdentityStamper} that stamps the caller's subject SPIFFE id (the sub claim, already a SPIFFE URI)
 * onto the proto message's spiffe_id field before serialization.
 *
 * One instance per stream. The type parameter is the proto message type
 * for the topic (e.g., SensorEvent).
 */
public class PublishHandler<T extends MessageLite> implements StreamObserver<T> {

    private static final Logger logger = LoggerFactory.getLogger(PublishHandler.class);

    /** JWT claims to propagate as Kafka headers. */
    private static final String[] CLAIM_KEYS = {"sub", "aud", "iss", "email", "service_id", "instance_id"};

    /**
     * Stamps identity onto a proto message. Generated per-type by the codegen
     * plugin for C2S topics (sets the spiffe_id field from the JWT sub claim).
     */
    @FunctionalInterface
    public interface IdentityStamper<T> {
        T stamp(T message, String spiffeId);
    }

    private final String kafkaTopic;
    private final KafkaPublisher publisher;
    private final StreamObserver<Eventbus.PublishResponse> responseObserver;
    private final Map<String, String> callerHeaders;
    private final IdentityStamper<T> stamper;
    /** Extracts trace_id from an incoming message (field 200). Null = no tracing. */
    private final Function<T, String> traceIdExtractor;
    /** Called with the trace_id when a message with a non-empty trace_id is published.
     *  Null for the trace topic to prevent trace-of-traces recursion. */
    private final Consumer<String> traceDispatcher;

    /** Outstanding publish futures whose responses have not been written yet. */
    private final AtomicInteger inFlight = new AtomicInteger();
    /** Set when the client has called onCompleted (half-closed). The response
     *  stream is only closed once this is true AND inFlight == 0, so we never
     *  acknowledge a stream before Kafka has acked all in-flight publishes. */
    private final AtomicBoolean clientCompleted = new AtomicBoolean(false);
    /** Set once a terminal method (onCompleted/onError) has been invoked on
     *  responseObserver. Guards against the gRPC contract violation of
     *  invoking two terminal methods, e.g. when a later publish completes
     *  successfully after an earlier one errored. */
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    /** Backward-compatible constructor without trace support. */
    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper) {
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper, null, null);
    }

    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher) {
        this.kafkaTopic = kafkaTopic;
        this.publisher = publisher;
        this.responseObserver = responseObserver;
        this.callerHeaders = extractCallerClaims();
        this.stamper = stamper;
        this.traceIdExtractor = traceIdExtractor;
        this.traceDispatcher = traceDispatcher;
        topicManager.ensureTopicExists(kafkaTopic);
    }

    /**
     * Extracts identity claims from the bearer token payload stored in gRPC context
     * by the Auth interceptor.
     */
    private static Map<String, String> extractCallerClaims() {
        Map<String, String> claims = new LinkedHashMap<>();
        String payload = Auth.tokenPayloadFromContext();
        if (payload.isEmpty()) {
            return claims;
        }
        for (String key : CLAIM_KEYS) {
            String value = extractJsonField(payload, key);
            if (value != null) {
                claims.put("caller." + key, value);
            }
        }
        return claims;
    }

    /**
     * Extracts a JSON field using Gson for correctness with nested/escaped values.
     */
    private static String extractJsonField(String json, String key) {
        try {
            JsonObject obj = JsonParser.parseString(json).getAsJsonObject();
            JsonElement field = obj.get(key);
            if (field == null || field.isJsonNull()) return null;
            return field.isJsonPrimitive() ? field.getAsString() : field.toString();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void onNext(T message) {
        // Extract and propagate trace_id from field 200 before stamping.
        if (traceIdExtractor != null) {
            String traceId = traceIdExtractor.apply(message);
            if (traceId != null && !traceId.isEmpty()) {
                synchronized (callerHeaders) {
                    callerHeaders.put("trace_id", traceId);
                }
                if (traceDispatcher != null) {
                    traceDispatcher.accept(traceId);
                }
            }
        }

        T stamped = message;
        if (stamper != null) {
            String spiffeId = callerHeaders.getOrDefault("caller.sub", "");
            if (!spiffeId.isEmpty()) {
                stamped = stamper.stamp(message, spiffeId);
            }
        }
        byte[] bytes = stamped.toByteArray();
        inFlight.incrementAndGet();
        java.util.concurrent.CompletableFuture<org.apache.kafka.clients.producer.RecordMetadata> pub;
        try {
            pub = publisher.publish(kafkaTopic, null, callerHeaders, bytes);
        } catch (Throwable t) {
            // Defense in depth — KafkaPublisher.publish already wraps producer.send,
            // but if anything throws before the future is returned the inFlight
            // counter would leak. Surface as an UNAVAILABLE error.
            inFlight.decrementAndGet();
            logger.error("Publish failed synchronously on topic {}", kafkaTopic, t);
            if (terminated.compareAndSet(false, true)) {
                synchronized (responseObserver) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("Publish failed synchronously: " + t.getMessage())
                            .asRuntimeException());
                }
            }
            return;
        }
        pub
                .thenAccept(metadata -> {
                    try {
                        synchronized (responseObserver) {
                            if (!terminated.get()) {
                                responseObserver.onNext(Eventbus.PublishResponse.newBuilder()
                                        .setPartition(metadata.partition())
                                        .setOffset(metadata.offset())
                                        .build());
                            }
                        }
                    } finally {
                        inFlight.decrementAndGet();
                        tryComplete();
                    }
                })
                .exceptionally(ex -> {
                    try {
                        logger.error("Publish failed on topic {}", kafkaTopic, ex);
                        if (terminated.compareAndSet(false, true)) {
                            synchronized (responseObserver) {
                                responseObserver.onError(Status.INTERNAL
                                        .withDescription("Publish failed: " + ex.getMessage())
                                        .asRuntimeException());
                            }
                        }
                    } finally {
                        inFlight.decrementAndGet();
                        // No tryComplete here — the stream is already terminated
                        // via onError; calling onCompleted afterwards would
                        // violate the gRPC single-terminal contract.
                    }
                    return null;
                });
    }

    @Override
    public void onError(Throwable t) {
        // Client cancelled or its half of the stream errored. No response
        // expected — but log so we can see the rate of client cancellations.
        logger.debug("Client error on publish stream for topic {}", kafkaTopic, t);
    }

    @Override
    public void onCompleted() {
        // The client has finished sending publishes. Do NOT close the response
        // stream yet — wait for any in-flight Kafka publishes to complete so
        // the client sees a real ack/error for every message it sent.
        clientCompleted.set(true);
        tryComplete();
    }

    /**
     * Closes the response stream with onCompleted iff the client has half-closed
     * AND every in-flight publish has resolved AND no terminal method has been
     * called yet. Idempotent under concurrent invocation from publish callbacks
     * and the client's onCompleted.
     */
    private void tryComplete() {
        if (!clientCompleted.get()) return;
        if (inFlight.get() != 0) return;
        if (terminated.compareAndSet(false, true)) {
            synchronized (responseObserver) {
                responseObserver.onCompleted();
            }
        }
    }
}
