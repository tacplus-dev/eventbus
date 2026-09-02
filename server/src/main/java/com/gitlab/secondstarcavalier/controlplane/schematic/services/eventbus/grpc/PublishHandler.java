// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.grpc;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.KafkaPublisher;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.TopicManager;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.chassis.servicemanager.Auth;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
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
 * onto the proto message's spiffe_id field before serialization. Topic types
 * that also declare actor_spiffe_id additionally receive an {@link ActorStamper},
 * which stamps that field from the caller's `act` claim (item 6 Piece A) —
 * the delegated-mint actor, present only when a delegated-user token made the
 * call. Absent (the ordinary case), actor_spiffe_id is left unset.
 *
 * One instance per stream. The type parameter is the proto message type
 * for the topic (e.g., SensorEvent).
 */
public class PublishHandler<T extends MessageLite> implements StreamObserver<T> {

    private static final Logger logger = LoggerFactory.getLogger(PublishHandler.class);

    /** JWT claims to propagate as Kafka headers. */
    private static final String[] CLAIM_KEYS = {"sub", "aud", "iss", "email", "service_id", "instance_id", "act", "namespace"};

    /**
     * Stamps identity onto a proto message. Generated per-type by the codegen
     * plugin for C2S topics (sets the spiffe_id field from the JWT sub claim).
     */
    @FunctionalInterface
    public interface IdentityStamper<T> {
        T stamp(T message, String spiffeId);
    }

    /**
     * Stamps the delegated-mint actor onto a proto message. Generated per-type
     * by the codegen plugin ONLY for C2S topic types that declare an
     * actor_spiffe_id field (see TopicDiscovery.collectTypesWithActorSpiffeIdField).
     * A sibling interface to {@link IdentityStamper}, not an overload of it, so
     * existing per-type stamper wiring never changes signature. Only invoked
     * when the caller's token in fact carries an `act` claim (item 6 Piece A) —
     * the ordinary, non-delegated case leaves actor_spiffe_id unset.
     */
    @FunctionalInterface
    public interface ActorStamper<T> {
        T stamp(T message, String actorSpiffeId);
    }

    /**
     * Stamps the caller's owning namespace onto a proto message. Generated
     * per-type by the codegen plugin ONLY for topic types that declare a
     * string namespace field — the (spec.store_namespace) tenant boundary
     * every (spec.record) must carry.
     *
     * <p>Applied only when the row arrives with the field EMPTY. A row whose
     * (spec.store_namespace) field is already set was tenanted by the
     * publishing service's own already-authenticated code — the same
     * server-side derivation (spec.store_principal)'s `acted_by` is written
     * from — and is accepted as it stands, so a service writing a row on a
     * specific user's behalf can land it in THAT user's tenant rather than its
     * own. See decisions/namespace-fix-corrected-2026-09-02.md (scotty).
     *
     * <p>Unlike {@link IdentityStamper} and {@link ActorStamper}, the empty
     * case is NOT conditional on the claim. Those two leave the field as the
     * publisher sent it when the claim is absent; doing that here would leave
     * an unset tenant boundary on a row that has none, which is the whole
     * thing the boundary exists to prevent. A publish of a row with an empty
     * namespace, to a topic whose type declares the field, by a caller whose
     * verified token carries no namespace claim, is refused PERMISSION_DENIED
     * — the same posture the generated egress filter takes when it reads an
     * empty caller namespace.
     */
    @FunctionalInterface
    public interface NamespaceStamper<T> {
        T stamp(T message, String namespace);
    }

    /**
     * Validates an inbound message against its declared proto constraints.
     * Generated per-type by the codegen plugin; throws when the message violates
     * one.
     */
    @FunctionalInterface
    public interface IngressValidator<T> {
        void validate(T message) throws Exception;
    }

    private final String kafkaTopic;
    private final KafkaPublisher publisher;
    private final StreamObserver<Eventbus.PublishResponse> responseObserver;
    private final Map<String, String> callerHeaders;
    private final IdentityStamper<T> stamper;
    /** Optionally stamps actor_spiffe_id from the caller's `act` claim. Null
     *  for topic types that declare no actor_spiffe_id field. */
    private final ActorStamper<T> actorStamper;
    /** Stamps the (spec.store_namespace) field from the caller's `namespace`
     *  claim, or refuses the publish when the claim is absent. Null for topic
     *  types that declare no namespace field. */
    private final NamespaceStamper<T> namespaceStamper;
    /** Reads the (spec.store_namespace) field already on an inbound message, so
     *  a value the publishing service's own code set can be told apart from an
     *  unset one. Generated alongside `namespaceStamper` and null exactly when
     *  that is. */
    private final Function<T, String> namespaceReader;
    /** Rejects malformed events at ingress. Null = no generated validator. */
    private final IngressValidator<T> validator;
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
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper, null, null, null);
    }

    /** Backward-compatible constructor without an ingress validator. */
    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher) {
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper,
                traceIdExtractor, traceDispatcher, null);
    }

    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher,
                          IngressValidator<T> validator) {
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper,
                traceIdExtractor, traceDispatcher, validator, null);
    }

    /** Backward-compatible constructor without an actor stamper: no C2S topic
     *  type declared actor_spiffe_id, so caller.act is never applied. */
    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher,
                          IngressValidator<T> validator,
                          ActorStamper<T> actorStamper) {
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper,
                traceIdExtractor, traceDispatcher, validator, actorStamper, null);
    }

    /** Backward-compatible constructor without a namespace stamper: the topic
     *  type declares no (spec.store_namespace) field, so nothing is stamped
     *  and no publish is refused for want of the claim. */
    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher,
                          IngressValidator<T> validator,
                          ActorStamper<T> actorStamper,
                          NamespaceStamper<T> namespaceStamper) {
        this(kafkaTopic, publisher, topicManager, responseObserver, stamper,
                traceIdExtractor, traceDispatcher, validator, actorStamper,
                namespaceStamper, null);
    }

    public PublishHandler(String kafkaTopic, KafkaPublisher publisher,
                          TopicManager topicManager,
                          StreamObserver<Eventbus.PublishResponse> responseObserver,
                          IdentityStamper<T> stamper,
                          Function<T, String> traceIdExtractor,
                          Consumer<String> traceDispatcher,
                          IngressValidator<T> validator,
                          ActorStamper<T> actorStamper,
                          NamespaceStamper<T> namespaceStamper,
                          Function<T, String> namespaceReader) {
        this.kafkaTopic = kafkaTopic;
        this.validator = validator;
        this.publisher = publisher;
        this.responseObserver = responseObserver;
        this.callerHeaders = extractCallerClaims();
        this.stamper = stamper;
        this.actorStamper = actorStamper;
        this.namespaceStamper = namespaceStamper;
        this.namespaceReader = namespaceReader;
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
        // Validate BEFORE anything else touches the message.
        //
        // The publish path used to accept anything. A declared constraint was
        // enforced only on the way OUT: agentsensor.AgentEvent.event_type is
        // [(spec.assert_enum_defined) = true], and an event published without it
        // was accepted, written to Kafka, consumed, and stored -- and then every
        // read of that collection failed with "response validation failed: field
        // eventType must be defined". One malformed record made the entire
        // AgentEvent query unreadable, permanently, and nothing on the write path
        // had objected.
        //
        // Ingress is the only choke point where every event enters the platform,
        // so it is where a constraint the store will later insist on has to be
        // enforced. Rejecting here costs the publisher one bad message; accepting
        // here costs every future reader the whole collection.
        if (validator != null) {
            try {
                validator.validate(message);
            } catch (Exception e) {
                logger.warn("Rejecting invalid event on {}: {}", kafkaTopic, e.getMessage());
                if (terminated.compareAndSet(false, true)) {
                    synchronized (responseObserver) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("event failed validation: " + e.getMessage())
                                .asRuntimeException());
                    }
                }
                return;
            }
        }

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
        if (actorStamper != null) {
            String actorSpiffeId = callerHeaders.getOrDefault("caller.act", "");
            if (!actorSpiffeId.isEmpty()) {
                stamped = actorStamper.stamp(stamped, actorSpiffeId);
            }
        }
        // (spec.store_namespace): accept-or-stamp-or-refuse. The field is the
        // row's tenant boundary and the generated egress filter holds every
        // read to the reader's own namespace claim, so a row published with it
        // empty is a row no legitimate caller can ever read.
        //
        // A row that arrives with the field ALREADY SET keeps that value: it
        // was set by the publishing service's own row-construction code, which
        // authenticated the acting user in its own request context before it
        // ever reached this stream — the identical trust
        // (spec.store_principal)'s `acted_by` already rides on, and the only
        // way a service writing on a user's behalf can land the row in THAT
        // user's tenant. Per decisions/namespace-fix-corrected-2026-09-02.md.
        if (namespaceStamper != null
                && (namespaceReader == null || namespaceReader.apply(stamped).isEmpty())) {
            // The row left the publisher with no tenant of its own, so this is a
            // write the publisher made purely on its own behalf and the publisher's
            // own namespace is the right one. A write on someone else's behalf sets
            // the field itself and never reaches here.
            //
            // There is deliberately NO branch on caller.act, because both a plain
            // and a delegated service token resolve to the same header: kernel's
            // delegated mint (IdentityServiceImpl.doMintDelegatedToken) sets the
            // token's namespace from the delegated SUBJECT's own OPA result, never
            // the delegate's. What must not change without changing this line: if
            // that mint ever resolved namespace from the DELEGATE, every
            // service-published record would be silently re-tenanted to the publisher.
            String namespace = callerHeaders.getOrDefault("caller.namespace", "");
            if (namespace.isEmpty()) {
                logger.warn("Refusing publish on {}: caller token carries no namespace claim", kafkaTopic);
                if (terminated.compareAndSet(false, true)) {
                    synchronized (responseObserver) {
                        responseObserver.onError(Status.PERMISSION_DENIED
                                .withDescription("caller token carries no namespace claim; "
                                        + "this topic's type declares (spec.store_namespace)")
                                .asRuntimeException());
                    }
                }
                return;
            }
            stamped = namespaceStamper.stamp(stamped, namespace);
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
