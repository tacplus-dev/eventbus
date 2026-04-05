// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.services.eventbus.grpc;

import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.libs.servicemanager.Auth;

import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.services.eventbus.Eventbus;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generic bidi-streaming publish handler for typed event-bus RPCs.
 * Receives typed protobuf messages from the client, serializes to bytes,
 * and publishes to the corresponding Kafka topic.
 *
 * Caller identity claims (sub, aud, iss, email, service_id, instance_id) are
 * extracted from the bearer token and attached as Kafka message headers,
 * providing identity provenance for every published event.
 *
 * One instance per stream. The type parameter is the proto message type
 * for the topic (e.g., TopologyEvent).
 */
public class PublishHandler<T extends MessageLite> implements StreamObserver<T> {

    private static final Logger logger = LoggerFactory.getLogger(PublishHandler.class);

    /** JWT claims to propagate as Kafka headers. */
    private static final String[] CLAIM_KEYS = {"sub", "aud", "iss", "email", "service_id", "instance_id"};

    /** Maps JWT claim names to proto field names for identity stamping. */
    private static final String[][] CLAIM_TO_FIELD = {
        {"caller.sub", "subject"},
        {"caller.iss", "issuer"},
        {"caller.aud", "audience"},
        {"caller.email", "email"},
        {"caller.service_id", "service_id"},
        {"caller.instance_id", "instance_id"},
    };

    private final String kafkaTopic;
    private final KafkaPublisher publisher;
    private final StreamObserver<Eventbus.PublishResponse> responseObserver;
    private final Map<String, String> callerHeaders;

    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver) {
        this.kafkaTopic = kafkaTopic;
        this.publisher = publisher;
        this.responseObserver = responseObserver;
        this.callerHeaders = extractCallerClaims();
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

    /**
     * Stamps identity claims from the caller's JWT onto proto message fields
     * using protobuf reflection. Fields not present on the message type are
     * silently skipped. This ensures EventBus is the trust boundary for
     * identity — clients cannot forge identity fields.
     */
    @SuppressWarnings("unchecked")
    private T stampIdentityClaims(T message) {
        if (!(message instanceof Message msg)) return message;
        if (callerHeaders.isEmpty()) return message;

        Message.Builder builder = msg.toBuilder();
        Descriptors.Descriptor desc = msg.getDescriptorForType();
        boolean modified = false;

        for (String[] mapping : CLAIM_TO_FIELD) {
            String claimKey = mapping[0];
            String fieldName = mapping[1];
            String value = callerHeaders.get(claimKey);
            if (value == null || value.isEmpty()) continue;

            Descriptors.FieldDescriptor fd = desc.findFieldByName(fieldName);
            if (fd != null && fd.getType() == Descriptors.FieldDescriptor.Type.STRING) {
                builder.setField(fd, value);
                modified = true;
            }
        }

        return modified ? (T) builder.build() : message;
    }

    @Override
    public void onNext(T message) {
        T stamped = stampIdentityClaims(message);
        byte[] bytes = stamped.toByteArray();
        // DEBUG: log every publish attempt
        logger.info("[DEBUG] Publishing {} bytes to topic={}", bytes.length, kafkaTopic);
        publisher.publish(kafkaTopic, null, callerHeaders, bytes)
                .thenAccept(metadata -> {
                    // DEBUG: log successful publish with partition/offset
                    logger.info("[DEBUG] Published to topic={} partition={} offset={}",
                            kafkaTopic, metadata.partition(), metadata.offset());
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
