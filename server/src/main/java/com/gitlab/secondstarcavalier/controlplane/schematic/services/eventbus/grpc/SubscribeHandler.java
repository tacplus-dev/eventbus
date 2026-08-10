// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.grpc;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber.*;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.model.types.services.eventbus.Eventbus;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generic bidi-streaming subscribe handler for typed event-bus RPCs.
 * Receives SubscribeRequests from the client to control seek/speed/pause,
 * and streams back metadata-bearing typed protobuf records from Kafka.
 *
 * The first SubscribeRequest creates a KafkaSubscriber.Session on a
 * virtual thread. Subsequent requests call session.resubscribe().
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

	private static final ExecutorService SUBSCRIBE_EXECUTOR =
			Executors.newVirtualThreadPerTaskExecutor();

	private final KafkaSubscriber subscriber;
	private final String kafkaTopic;
	private final StreamObserver<DynamicMessage> responseObserver;
	private final ProtoParser<T> parser;
	private volatile Session session;
	private final AtomicBoolean firstMessage = new AtomicBoolean(true);

	public SubscribeHandler(KafkaSubscriber subscriber, String kafkaTopic,
			StreamObserver<DynamicMessage> responseObserver, ProtoParser<T> parser) {
		this.subscriber = subscriber;
		this.kafkaTopic = kafkaTopic;
		this.responseObserver = responseObserver;
		this.parser = parser;
	}

	public static DynamicMessage subscribeRecordDefaultInstance() {
		return DynamicMessage.getDefaultInstance(SUBSCRIBE_RECORD_DESCRIPTOR);
	}

	@Override
	public void onNext(Eventbus.SubscribeRequest request) {
		SeekParams params = toSeekParams(request);

		if (firstMessage.compareAndSet(true, false)) {
			Session s = subscriber.createSession(kafkaTopic, params);
			this.session = s;

			SUBSCRIBE_EXECUTOR.submit(() -> {
				// Tracks whether the stream has already been terminated via
				// onError so the finally block does not also call onCompleted —
				// gRPC forbids invoking both terminal methods on a stream.
				AtomicBoolean terminated = new AtomicBoolean(false);
				try {
					s.run(params, record -> {
						try {
							parser.parse(record.value());
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

	@Override
	public void onCompleted() {
		// Client half-close means no more SubscribeRequests will be sent.
		// Do NOT close the session — the server should keep streaming events
		// until the client disconnects (onError with CANCELLED).
		logger.debug("Client half-closed subscribe stream for topic {}", kafkaTopic);
	}

	private static SeekParams toSeekParams(Eventbus.SubscribeRequest request) {
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
				request.getConsumerGroup(),
				request.getPause(),
				bp,
				request.getSampleInterval(),
				partitions);
	}
}
