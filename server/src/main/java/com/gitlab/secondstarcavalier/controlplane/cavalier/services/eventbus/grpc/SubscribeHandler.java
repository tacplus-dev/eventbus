// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.grpc;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaSubscriber;
import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.kafka.KafkaSubscriber.*;

import com.gitlab.secondstarcavalier.controlplane.cavalier.services.eventbus.tacplus.types.services.eventbus.Eventbus;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Generic bidi-streaming subscribe handler for typed event-bus RPCs.
 * Receives SubscribeRequests from the client to control seek/speed/pause,
 * and streams back typed protobuf messages from Kafka.
 *
 * The first SubscribeRequest creates a KafkaSubscriber.Session on a
 * virtual thread. Subsequent requests call session.resubscribe().
 */
public class SubscribeHandler<T extends MessageLite> implements StreamObserver<Eventbus.SubscribeRequest> {

	private static final Logger logger = LoggerFactory.getLogger(SubscribeHandler.class);

	/**
	 * Parser function that deserializes bytes to a typed proto message.
	 */
	@FunctionalInterface
	public interface ProtoParser<T> {
		T parse(byte[] data) throws InvalidProtocolBufferException;
	}

	private final KafkaSubscriber subscriber;
	private final String kafkaTopic;
	private final StreamObserver<T> responseObserver;
	private final ProtoParser<T> parser;
	private volatile Session session;
	private boolean firstMessage = true;

	public SubscribeHandler(KafkaSubscriber subscriber, String kafkaTopic,
			StreamObserver<T> responseObserver, ProtoParser<T> parser) {
		this.subscriber = subscriber;
		this.kafkaTopic = kafkaTopic;
		this.responseObserver = responseObserver;
		this.parser = parser;
	}

	@Override
	public void onNext(Eventbus.SubscribeRequest request) {
		SeekParams params = toSeekParams(request);

		if (firstMessage) {
			firstMessage = false;
			Session s = subscriber.createSession(kafkaTopic, params);
			this.session = s;

			// AGENT : TODO : why are we not using a thread executor pool here?

			Thread.startVirtualThread(() -> {
				try {
					s.run(params, record -> {
						try {
							T message = parser.parse(record.value());
							synchronized (responseObserver) {
								responseObserver.onNext(message);
							}
							return true;
						} catch (InvalidProtocolBufferException e) {
							logger.error("Failed to parse record on topic {} partition {} offset {}",
									record.topic(), record.partition(), record.offset(), e);
							synchronized (responseObserver) {
								responseObserver.onError(Status.INTERNAL
										.withDescription("Failed to parse event: " + e.getMessage())
										.asRuntimeException());
							}
							return false;
						}
					});
				} finally {
					synchronized (responseObserver) {
						responseObserver.onCompleted();
					}
				}
			});
		} else {
			if (session != null) {
				session.resubscribe(params);
			}
		}
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
