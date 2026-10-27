// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.grpc;

import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber.*;

import com.github.tacplusdev.eventbus.model.chassis.servicemanager.Auth;
import com.github.tacplusdev.eventbus.model.types.services.eventbus.Eventbus;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Generic bidi-streaming subscribe handler for typed event-bus RPCs.
 * Receives SubscribeRequests from the client to control seek/speed/pause,
 * and streams back metadata-bearing typed protobuf records from Kafka.
 *
 * The first SubscribeRequest creates a KafkaSubscriber.Session on a
 * dedicated platform thread. Subsequent requests call session.resubscribe().
 *
 * The client-supplied consumer_group is namespaced under the caller's own
 * verified identity before it reaches Kafka -- see
 * {@link #boundConsumerGroup}.
 *
 * On topics whose type declares the (spec.store_namespace) field, delivery is
 * additionally pinned to the caller's own tenant -- see
 * {@link #namespacePinned}.
 */
public class SubscribeHandler<T extends MessageLite> implements StreamObserver<Eventbus.SubscribeRequest> {

	private static final Logger logger = LoggerFactory.getLogger(SubscribeHandler.class);
	private static final Descriptors.Descriptor RECORD_META_DESCRIPTOR;
	private static final Descriptors.Descriptor SUBSCRIBE_RECORD_DESCRIPTOR;
	private static final Descriptors.FieldDescriptor META_KEY_FIELD;
	private static final Descriptors.FieldDescriptor META_TOPIC_FIELD;
	private static final Descriptors.FieldDescriptor META_PARTITION_FIELD;
	private static final Descriptors.FieldDescriptor META_OFFSET_FIELD;
	private static final Descriptors.FieldDescriptor META_TIMESTAMP_FIELD;
	private static final Descriptors.FieldDescriptor SUBSCRIBE_RECORD_META_FIELD;
	private static final Descriptors.FieldDescriptor SUBSCRIBE_RECORD_PAYLOAD_FIELD;

	static {
		try {
			DescriptorProtos.DescriptorProto recordMeta = DescriptorProtos.DescriptorProto.newBuilder()
					.setName("RecordMeta")
					.addField(field("key", 1, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))
					.addField(field("topic", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
					.addField(field("partition", 3, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32))
					.addField(field("offset", 4, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
					.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
							.setName("timestamp")
							.setNumber(5)
							.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
							.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
							.setTypeName(".google.protobuf.Timestamp")
							.build())
					.build();
			DescriptorProtos.DescriptorProto subscribeRecord = DescriptorProtos.DescriptorProto.newBuilder()
					.setName("SubscribeRecord")
					.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
							.setName("meta")
							.setNumber(1)
							.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
							.setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
							.setTypeName(".eventbuswire.RecordMeta")
							.build())
					.addField(field("payload", 2, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES))
					.build();
			DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
					.setName("eventbus_subscribe_envelope.proto")
					.setPackage("eventbuswire")
					.setSyntax("proto3")
					.addDependency("google/protobuf/timestamp.proto")
					.addMessageType(recordMeta)
					.addMessageType(subscribeRecord)
					.build();
			Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
					file, new Descriptors.FileDescriptor[]{TimestampProto.getDescriptor()});
			RECORD_META_DESCRIPTOR = fd.findMessageTypeByName("RecordMeta");
			SUBSCRIBE_RECORD_DESCRIPTOR = fd.findMessageTypeByName("SubscribeRecord");
			META_KEY_FIELD = requireField(RECORD_META_DESCRIPTOR, "key");
			META_TOPIC_FIELD = requireField(RECORD_META_DESCRIPTOR, "topic");
			META_PARTITION_FIELD = requireField(RECORD_META_DESCRIPTOR, "partition");
			META_OFFSET_FIELD = requireField(RECORD_META_DESCRIPTOR, "offset");
			META_TIMESTAMP_FIELD = requireField(RECORD_META_DESCRIPTOR, "timestamp");
			SUBSCRIBE_RECORD_META_FIELD = requireField(SUBSCRIBE_RECORD_DESCRIPTOR, "meta");
			SUBSCRIBE_RECORD_PAYLOAD_FIELD = requireField(SUBSCRIBE_RECORD_DESCRIPTOR, "payload");
		} catch (Descriptors.DescriptorValidationException e) {
			throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * Parser function that deserializes bytes to a typed proto message.
	 */
	@FunctionalInterface
	public interface ProtoParser<T> {
		T parse(byte[] data) throws InvalidProtocolBufferException;
	}

	/**
	 * One PLATFORM thread per subscribe session. Deliberately not
	 * {@code newVirtualThreadPerTaskExecutor()}, which is what this was and
	 * what silently killed the whole subscribe plane for 25 hours.
	 *
	 * A Kafka consumer poll loop is the exact shape virtual threads are not
	 * for: long-lived, and blocking inside the classic consumer's
	 * {@code synchronized} coordinator methods. A virtual thread that contends
	 * on a lock while pinned by such a frame cannot unmount -- it parks ON its
	 * carrier ({@code VirtualThread.parkOnCarrierThread}) and holds it for the
	 * whole wait. The carrier count starts at one per CPU (2 on this fleet's
	 * nodes), so with ~25 sessions all logging through one appender lock from
	 * inside the coordinator monitor, carriers are consumed faster than the
	 * scheduler can compensate and sessions submitted here stop being run at
	 * all. Their consumers never poll and never heartbeat, the coordinator
	 * evicts every member of their group one {@code session.timeout.ms} after
	 * it stabilizes, and the gRPC stream stays open on both sides with nothing
	 * flowing. A live thread dump of the wedged pod had 37 threads -- 20
	 * subscribe loops, 11 gRPC request threads and 6 of Kafka's own heartbeat
	 * threads -- parked on that one lock.
	 *
	 * Platform threads make each poll loop's blocking its own problem instead
	 * of the scheduler's. See
	 * {@code decisions/2026-09-15-kafka-consumer-starvation-fix.md}.
	 *
	 * Package-private so a test can assert what kind of thread this actually
	 * runs on — the distinction is the whole fix and is invisible at the call
	 * site, so nothing else would notice it being changed back.
	 */
	static final ExecutorService SUBSCRIBE_EXECUTOR =
			Executors.newCachedThreadPool(
					Thread.ofPlatform().daemon().name("eventbus-subscribe-", 0).factory());

	private final KafkaSubscriber subscriber;
	private final String kafkaTopic;
	private final StreamObserver<DynamicMessage> responseObserver;
	private final ProtoParser<T> parser;
	/** Reads the (spec.store_namespace) field off a parsed event, so delivery can
	 *  be held to the subscriber's own tenant. Generated per-type exactly when
	 *  PublishHandler's namespaceReader is -- null for topic types that declare
	 *  no namespace field, and null is what makes such a topic exempt. */
	private final Function<T, String> namespaceReader;
	private volatile Session session;
	private final AtomicBoolean firstMessage = new AtomicBoolean(true);
	/** Set once the stream has been failed, so a later onNext does not fail it twice. */
	private final AtomicBoolean streamFailed = new AtomicBoolean(false);
	/**
	 * Whether the stream has already been terminated, so the session task's
	 * finally block does not also call onCompleted — gRPC forbids invoking both
	 * terminal methods on a stream. A field rather than a local of the session
	 * task because the stall watchdog terminates the stream from its own thread.
	 */
	private final AtomicBoolean terminated = new AtomicBoolean(false);
	/** Set by the call's cancel handler, so a session onNext creates after the cancel is closed at once. */
	private volatile boolean cancelled;

	/** Backward-compatible constructor without a namespace reader: the topic type
	 *  declares no (spec.store_namespace) field, so every subscriber sees the
	 *  whole topic exactly as before. */
	public SubscribeHandler(KafkaSubscriber subscriber, String kafkaTopic,
			StreamObserver<DynamicMessage> responseObserver, ProtoParser<T> parser) {
		this(subscriber, kafkaTopic, responseObserver, parser, null);
	}

	public SubscribeHandler(KafkaSubscriber subscriber, String kafkaTopic,
			StreamObserver<DynamicMessage> responseObserver, ProtoParser<T> parser,
			Function<T, String> namespaceReader) {
		this.subscriber = subscriber;
		this.kafkaTopic = kafkaTopic;
		this.responseObserver = responseObserver;
		this.parser = parser;
		this.namespaceReader = namespaceReader;
		// The client half-closes after its SubscribeRequest, and grpc-java does
		// not deliver onError for a cancel that arrives after a half-close. The
		// cancel handler is the only notice the server gets that the subscriber
		// went away; without it the session's consumer stayed in its group
		// holding partitions until the next record failed to send. Registered
		// here because the bindings construct this inside the call's startup,
		// where grpc requires it to be set.
		if (responseObserver instanceof ServerCallStreamObserver<?> call) {
			call.setOnCancelHandler(this::onCancelled);
		}
	}

	/**
	 * Whether a caller's namespace claim confines it to its own tenant.
	 *
	 * A /system caller is the fleet: shared platform services read every
	 * tenant's events, which is what stratum's ingestion and the operator
	 * depend on. A /tenants/&lt;t&gt; caller is one tenant-owned instance among
	 * many and sees only its own.
	 *
	 * This is the runtime branch the generated S2S record plane takes on the
	 * same claim (recordnamespace_gen.go's __nsPinned), spelled the same way on
	 * purpose: the two seams must agree on what "the caller's own tenant" means.
	 */
	static boolean namespacePinned(String callerNamespace) {
		return !callerNamespace.startsWith("/system");
	}

	public static DynamicMessage subscribeRecordDefaultInstance() {
		return DynamicMessage.getDefaultInstance(SUBSCRIBE_RECORD_DESCRIPTOR);
	}

	@Override
	public void onNext(Eventbus.SubscribeRequest request) {
		if (streamFailed.get()) {
			return;
		}

		// The group id the caller asked for is not the group id Kafka gets: it
		// is namespaced under the caller's own verified identity first. Without
		// that, any authenticated caller could name another service's group
		// (e.g. stratum's ingestion group) and silently steal or duplicate its
		// share of the partitions.
		String callerSub = Auth.callerSubFromContext();
		if (!request.getConsumerGroup().isEmpty() && callerSub.isEmpty()) {
			// Nothing verified a token on this RPC, so there is no identity to
			// bind the group to. An anonymous caller may still subscribe with no
			// group at all -- KafkaSubscriber then gives it a fresh random one,
			// which is nobody else's.
			if (streamFailed.compareAndSet(false, true)) {
				synchronized (responseObserver) {
					responseObserver.onError(Status.UNAUTHENTICATED
							.withDescription("consumer_group requires a verified caller identity")
							.asRuntimeException());
				}
			}
			return;
		}

		// The tenant boundary on the read side. It is read HERE, on the gRPC
		// thread, because the Kafka consumer below runs on its own thread
		// where the caller's gRPC context is no longer current -- the claim
		// would come back empty if it were read down there.
		//
		// Only topics whose type declares (spec.store_namespace) are gated:
		// namespaceReader is null for every other topic and nothing changes for
		// them, including for an anonymous subscriber. On a topic that DOES
		// declare it, an absent claim is refused rather than served fleet-wide,
		// which is the posture PublishHandler already takes on exactly the same
		// set of topics (see its (spec.store_namespace) block). Serving instead
		// would hand every tenant's events to a caller that proved no tenant --
		// the one outcome the field exists to prevent.
		String callerNamespace = Auth.callerNamespaceFromContext();
		if (namespaceReader != null && callerNamespace.isEmpty()) {
			logger.warn("Refusing subscribe on {}: caller token carries no namespace claim", kafkaTopic);
			if (streamFailed.compareAndSet(false, true)) {
				synchronized (responseObserver) {
					responseObserver.onError(Status.PERMISSION_DENIED
							.withDescription("caller token carries no namespace claim; "
									+ "this topic's type declares (spec.store_namespace)")
							.asRuntimeException());
				}
			}
			return;
		}
		final boolean nsPinned = namespaceReader != null && namespacePinned(callerNamespace);

		SeekParams params = toSeekParams(request, callerSub);

		if (firstMessage.compareAndSet(true, false)) {
			Session s = subscriber.createSession(kafkaTopic, params);
			this.session = s;
			// A cancel that landed before the assignment above saw no session to
			// close. run() still runs so the consumer is closed when it exits.
			if (cancelled) {
				s.close();
			}

			// Terminate the stream from the watchdog's own thread when the
			// consumer behind it stops polling. Leaving that to the poll loop
			// is what made this failure silent: the loop is the thing that is
			// stuck, so it may never reach the check.
			s.onStalled(this::failStalled);

			SUBSCRIBE_EXECUTOR.submit(() -> {
				try {
					s.run(params, record -> {
						try {
							T event = parser.parse(record.value());
							// Another tenant's event on a shared topic: drop it
							// and keep streaming. Not an error -- the events are
							// legitimately interleaved on one Kafka topic, this
							// caller is simply not entitled to this one.
							//
							// The row's OWN field is authoritative, not the
							// publisher's claim on the Kafka header: a service
							// publishing on another tenant's behalf sets the
							// field itself and the two differ exactly there.
							if (nsPinned && !callerNamespace.equals(namespaceReader.apply(event))) {
								return true;
							}
							DynamicMessage message = subscribeRecordFor(record);
							synchronized (responseObserver) {
								responseObserver.onNext(message);
							}
							return true;
						} catch (InvalidProtocolBufferException e) {
							logger.error("Failed to parse record on topic {} partition {} offset {}",
									record.topic(), record.partition(), record.offset(), e);
							if (terminated.compareAndSet(false, true)) {
								synchronized (responseObserver) {
									responseObserver.onError(Status.INTERNAL
											.withDescription("Failed to parse event: " + e.getMessage())
											.asRuntimeException());
								}
							}
							return false;
						}
					});
				} catch (KafkaSubscriber.PartitionAssignmentTimeoutException e) {
					logger.warn("Partition assignment timeout for topic {}: {}", kafkaTopic, e.getMessage());
					if (terminated.compareAndSet(false, true)) {
						synchronized (responseObserver) {
							responseObserver.onError(Status.UNAVAILABLE
									.withDescription("Kafka partition assignment timeout for topic " + kafkaTopic)
									.asRuntimeException());
						}
					}
				} catch (KafkaSubscriber.ConsumerStalledException e) {
					// The consumer behind this stream stopped polling long
					// enough that the coordinator has already dropped it from
					// its group. Leaving the stream open is what made this
					// failure silent: the client sees an open stream, no error
					// and no events, so its reconnect loop never fires. Fail it.
					logger.error("Consumer stalled on topic {}: {}", kafkaTopic, e.getMessage());
					if (terminated.compareAndSet(false, true)) {
						synchronized (responseObserver) {
							responseObserver.onError(Status.UNAVAILABLE
									.withDescription("Kafka consumer for topic " + kafkaTopic
											+ " left its consumer group: " + e.getMessage())
									.asRuntimeException());
						}
					}
				} finally {
					if (terminated.compareAndSet(false, true)) {
						synchronized (responseObserver) {
							responseObserver.onCompleted();
						}
					}
				}
			});
		} else {
			if (session != null) {
				session.resubscribe(params);
			}
		}
	}

	/**
	 * Fails the stream because the Kafka consumer behind it stopped polling long
	 * enough that the coordinator has already dropped it from its group. Called
	 * on the poll-stall watchdog's thread.
	 *
	 * This is the whole point of the change: a deaf consumer under an open
	 * stream is indistinguishable from an idle topic, so the client's own
	 * reconnect loop never fires. UNAVAILABLE is what makes it fire.
	 */
	private void failStalled(String reason) {
		logger.error("Consumer stalled on topic {}, failing the stream: {}", kafkaTopic, reason);
		if (terminated.compareAndSet(false, true)) {
			synchronized (responseObserver) {
				responseObserver.onError(Status.UNAVAILABLE
						.withDescription("Kafka consumer for topic " + kafkaTopic
								+ " left its consumer group: " + reason)
						.asRuntimeException());
			}
		}
	}

	static DynamicMessage subscribeRecordFor(KafkaSubscriber.Record record) {
		DynamicMessage.Builder meta = DynamicMessage.newBuilder(RECORD_META_DESCRIPTOR)
				.setField(META_TOPIC_FIELD, record.topic())
				.setField(META_PARTITION_FIELD, record.partition())
				.setField(META_OFFSET_FIELD, record.offset());
		if (record.key() != null && !record.key().isEmpty()) {
			meta.setField(META_KEY_FIELD, ByteString.copyFromUtf8(record.key()));
		}
		if (record.timestamp() > 0) {
			meta.setField(META_TIMESTAMP_FIELD, timestampFromMillis(record.timestamp()));
		}
		return DynamicMessage.newBuilder(SUBSCRIBE_RECORD_DESCRIPTOR)
				.setField(SUBSCRIBE_RECORD_META_FIELD, meta.build())
				.setField(SUBSCRIBE_RECORD_PAYLOAD_FIELD, ByteString.copyFrom(record.value()))
				.build();
	}

	private static DescriptorProtos.FieldDescriptorProto field(
			String name, int number, DescriptorProtos.FieldDescriptorProto.Type type) {
		return DescriptorProtos.FieldDescriptorProto.newBuilder()
				.setName(name)
				.setNumber(number)
				.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
				.setType(type)
				.build();
	}

	private static Descriptors.FieldDescriptor requireField(Descriptors.Descriptor descriptor, String name) {
		Descriptors.FieldDescriptor field = descriptor.findFieldByName(name);
		if (field == null) {
			throw new ExceptionInInitializerError("missing field " + descriptor.getFullName() + "." + name);
		}
		return field;
	}

	private static Timestamp timestampFromMillis(long millis) {
		long seconds = Math.floorDiv(millis, 1000);
		int nanos = (int) Math.floorMod(millis, 1000) * 1_000_000;
		return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
	}

	@Override
	public void onError(Throwable t) {
		logger.debug("Client error on subscribe stream for topic {}", kafkaTopic, t);
		if (session != null) {
			session.close();
		}
	}

	private void onCancelled() {
		logger.debug("Client cancelled subscribe stream for topic {}", kafkaTopic);
		cancelled = true;
		Session s = session;
		if (s != null) {
			s.close();
		}
	}

	@Override
	public void onCompleted() {
		// Client half-close means no more SubscribeRequests will be sent.
		// Do NOT close the session — the server should keep streaming events
		// until the client disconnects. After a half-close that disconnect
		// arrives as the call's cancel handler, not onError.
		logger.debug("Client half-closed subscribe stream for topic {}", kafkaTopic);
	}

	/**
	 * Namespaces a client-supplied consumer group under the caller's own
	 * identity, so two callers naming the same group land in two different
	 * Kafka groups and neither can join the other's.
	 *
	 * An empty request means "no group" and stays empty: KafkaSubscriber then
	 * assigns a fresh random group id, which no other caller can name.
	 *
	 * The separator is '|', which cannot appear in the SPIFFE URI that is the
	 * caller's sub, so no two subjects can produce the same bound group.
	 */
	static String boundConsumerGroup(String callerSub, String requested) {
		if (requested == null || requested.isEmpty()) {
			return "";
		}
		return callerSub + "|" + requested;
	}

	/**
	 * The inverse of {@link #boundConsumerGroup}: whether a Kafka group id is
	 * one this caller's own subscribes produced.
	 *
	 * A group with no '|' at all is an anonymous subscribe's random id or an
	 * eventbus-internal group, which belongs to no caller and so is owned by
	 * nobody. The separator cannot appear in a SPIFFE URI, so a prefix match on
	 * {@code callerSub + "|"} cannot be satisfied by another caller's subject.
	 */
	public static boolean ownsConsumerGroup(String callerSub, String groupId) {
		if (callerSub == null || callerSub.isEmpty() || groupId == null) {
			return false;
		}
		return groupId.startsWith(callerSub + "|");
	}

	/**
	 * Resolves the Kafka group id an admin RPC from this caller may act on, or
	 * "" when the caller may not act on the requested group at all.
	 *
	 * Admin callers name a group the same way subscribers do -- with the
	 * unbound name they passed to Subscribe -- so the requested name is bound
	 * through {@link #boundConsumerGroup} exactly as the subscribe path binds
	 * it. An already-bound id (as {@code ListConsumerGroups} reports it) is
	 * accepted as-is when it is the caller's own. Anything else -- no verified
	 * caller, an empty name, or an id already bound to some other subject --
	 * names a group this caller does not own, and resolves to "".
	 */
	public static String ownedConsumerGroup(String callerSub, String requested) {
		if (callerSub == null || callerSub.isEmpty() || requested == null || requested.isEmpty()) {
			return "";
		}
		if (ownsConsumerGroup(callerSub, requested)) {
			return requested;
		}
		if (requested.indexOf('|') >= 0) {
			// Already bound, but not to this caller.
			return "";
		}
		return boundConsumerGroup(callerSub, requested);
	}

	private static SeekParams toSeekParams(Eventbus.SubscribeRequest request, String callerSub) {
		SeekMode mode = SeekMode.LATEST;
		long value = 0;

		switch (request.getStartFromCase()) {
			case OFFSET:
				mode = SeekMode.OFFSET;
				value = request.getOffset();
				break;
			case TIMESTAMP:
				mode = SeekMode.TIMESTAMP;
				Timestamp ts = request.getTimestamp();
				value = ts.getSeconds() * 1000 + ts.getNanos() / 1_000_000;
				break;
			case LATEST:
				mode = SeekMode.LATEST;
				break;
			case EARLIEST:
				mode = SeekMode.EARLIEST;
				break;
			default:
				mode = SeekMode.LATEST;
				break;
		}

		BackpressurePolicy bp;
		switch (request.getBackpressure()) {
			case BACKPRESSURE_DROP_OLD:
				bp = BackpressurePolicy.DROP_OLD;
				break;
			case BACKPRESSURE_SAMPLE:
				bp = BackpressurePolicy.SAMPLE;
				break;
			default:
				bp = BackpressurePolicy.BUFFER;
				break;
		}

		List<Integer> partitions = request.getPartitionsList();

		return new SeekParams(
				mode, value,
				request.getSpeedMultiplier(),
				boundConsumerGroup(callerSub, request.getConsumerGroup()),
				request.getPause(),
				bp,
				request.getSampleInterval(),
				partitions);
	}
}
