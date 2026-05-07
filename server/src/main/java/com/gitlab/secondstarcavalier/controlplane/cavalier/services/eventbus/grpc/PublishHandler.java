// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.tacplus.libs.servicemanager.Auth;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.tacplus.types.services.eventbus.Eventbus;
import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Generic bidi-streaming publish handler for typed event-bus RPCs.
 * Receives typed protobuf messages from the client, serializes to bytes,
 * and publishes to the corresponding Kafka topic.
 *
 * Caller identity claims are extracted from the bearer token and attached
 * as Kafka message headers. For C2S topics, the generated code passes an
 * {@link IdentityStamper} that stamps the caller's email onto the proto
 * message before serialization.
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
     * plugin for C2S topics (e.g., sets the email field from the JWT claim).
     */
    @FunctionalInterface
    public interface IdentityStamper<T> {
        T stamp(T message, String email);
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
     * Minimal JSON field extractor for flat JWT payloads.
     * Handles string and numeric values.
     */
    private static String extractJsonField(String json, String key) {
        String needle = "\"" + key + "\"";
        int idx = json.indexOf(needle);
        if (idx < 0) return null;
        String rest = json.substring(idx + needle.length()).stripLeading();
        if (!rest.startsWith(":")) return null;
        rest = rest.substring(1).stripLeading();
        if (rest.startsWith("\"")) {
            int end = rest.indexOf('"', 1);
            return end > 0 ? rest.substring(1, end) : null;
        }
        int end = 0;
        while (end < rest.length() && (Character.isDigit(rest.charAt(end)) || rest.charAt(end) == '.')) end++;
        return end > 0 ? rest.substring(0, end) : null;
    }

    @Override
    public void onNext(T message) {
        // Extract and propagate trace_id from field 200 before stamping.
        if (traceIdExtractor != null) {
            String traceId = traceIdExtractor.apply(message);
            if (traceId != null && !traceId.isEmpty()) {
                callerHeaders.put("trace_id", traceId);
                if (traceDispatcher != null) {
                    traceDispatcher.accept(traceId);
                }
            }
        }

        T stamped = message;
        if (stamper != null) {
            String email = callerHeaders.getOrDefault("caller.email", "");
            if (!email.isEmpty()) {
                stamped = stamper.stamp(message, email);
            }
        }
        byte[] bytes = stamped.toByteArray();
        publisher.publish(kafkaTopic, null, callerHeaders, bytes)
                .thenAccept(metadata -> {
                    synchronized (responseObserver) {
                        responseObserver.onNext(Eventbus.PublishResponse.newBuilder()
                                .setPartition(metadata.partition())
                                .setOffset(metadata.offset())
                                .build());
                    }
                })
                .exceptionally(ex -> {
                    logger.error("Publish failed on topic {}", kafkaTopic, ex);
                    synchronized (responseObserver) {
                        responseObserver.onError(Status.INTERNAL
                                .withDescription("Publish failed: " + ex.getMessage())
                                .asRuntimeException());
                    }
                    return null;
                });
    }

    @Override
    public void onError(Throwable t) {
        logger.debug("Client error on publish stream for topic {}", kafkaTopic, t);
    }

    @Override
    public void onCompleted() {
        responseObserver.onCompleted();
    }
}
