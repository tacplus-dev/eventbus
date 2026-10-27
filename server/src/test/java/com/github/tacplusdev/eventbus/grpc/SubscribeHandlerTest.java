// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.eventbus.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.tacplusdev.eventbus.kafka.KafkaSubscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.WireFormat;

class SubscribeHandlerTest {

	/**
	 * The regression test for the 2026-09-15 incident: every stratum consumer
	 * group in the dev cluster went Empty on heartbeat expiration ~30s after
	 * every join, for 25 hours, with all the gRPC streams still open.
	 *
	 * The cause was this executor being
	 * {@code Executors.newVirtualThreadPerTaskExecutor()}. A Kafka consumer poll
	 * loop blocks inside the classic consumer's {@code synchronized} coordinator
	 * methods, which pins the carrier; a pinned virtual thread that then waits
	 * on a lock parks ON its carrier instead of unmounting, so it holds it for
	 * the whole wait. With one carrier per CPU (2 on the fleet's nodes) the
	 * scheduler ran out and sessions submitted here stopped being run at all —
	 * they never polled, never heartbeat, and were evicted one
	 * session.timeout.ms after their group stabilized.
	 *
	 * Nothing at the call site changes when the executor changes, so this is
	 * the only thing that would notice it going back.
	 */
	@Test
	void subscribeSessionsRunOnPlatformThreadsNotVirtualThreads() throws Exception {
		java.util.concurrent.CompletableFuture<Thread> ran = new java.util.concurrent.CompletableFuture<>();
		SubscribeHandler.SUBSCRIBE_EXECUTOR.submit(() -> ran.complete(Thread.currentThread()));
		Thread t = ran.get(10, java.util.concurrent.TimeUnit.SECONDS);

		assertFalse(t.isVirtual(),
				"Kafka poll loops must not run on virtual threads: they block inside the "
						+ "consumer's synchronized coordinator methods, which pins the carrier");
		assertTrue(t.isDaemon(), "subscribe threads must not hold the JVM open");
	}

	@Test
	void subscribeRecordForWrapsPayloadWithNonZeroMetadata() throws Exception {
		byte[] payload = new byte[]{0x08, 0x2a};
		KafkaSubscriber.Record record = new KafkaSubscriber.Record(
				"contractfixtures.FixtureState.events",
				7,
				42,
				1_700_000_000_123L,
				"fixture-key",
				Map.of("authorization", "redacted"),
				payload);

		DynamicMessage envelope = SubscribeHandler.subscribeRecordFor(record);
		DecodedEnvelope decoded = decodeEnvelope(envelope.toByteArray());

		assertArrayEquals(payload, decoded.payload().toByteArray());
		assertArrayEquals("fixture-key".getBytes(java.nio.charset.StandardCharsets.UTF_8), decoded.meta().key().toByteArray());
		assertEquals("contractfixtures.FixtureState.events", decoded.meta().topic());
		assertEquals(7, decoded.meta().partition());
		assertEquals(42, decoded.meta().offset());
		assertEquals(1_700_000_000L, decoded.meta().timestamp().getSeconds());
		assertEquals(123_000_000, decoded.meta().timestamp().getNanos());
	}

	@Test
	void boundConsumerGroupNamespacesTheRequestedGroupUnderTheCaller() {
		String victim = "spiffe://test/system/services/stratum";
		String attacker = "spiffe://test/system/services/tasks";

		assertEquals(victim + "|stratum-rebuild",
				SubscribeHandler.boundConsumerGroup(victim, "stratum-rebuild"));
		// Same string asked for, two different Kafka groups.
		assertNotEquals(SubscribeHandler.boundConsumerGroup(victim, "stratum-rebuild"),
				SubscribeHandler.boundConsumerGroup(attacker, "stratum-rebuild"));
		// No group asked for stays no group: KafkaSubscriber assigns a random one.
		assertEquals("", SubscribeHandler.boundConsumerGroup(victim, ""));
		assertEquals("", SubscribeHandler.boundConsumerGroup(victim, null));
	}

	private static DecodedEnvelope decodeEnvelope(byte[] data) throws IOException {
		CodedInputStream in = CodedInputStream.newInstance(data);
		ByteString meta = ByteString.EMPTY;
		ByteString payload = ByteString.EMPTY;
		while (!in.isAtEnd()) {
			int tag = in.readTag();
			switch (WireFormat.getTagFieldNumber(tag)) {
				case 1 -> meta = in.readBytes();
				case 2 -> payload = in.readBytes();
				default -> in.skipField(tag);
			}
		}
		return new DecodedEnvelope(decodeMeta(meta.toByteArray()), payload);
	}

	private static DecodedMeta decodeMeta(byte[] data) throws IOException {
		CodedInputStream in = CodedInputStream.newInstance(data);
		ByteString key = ByteString.EMPTY;
		String topic = "";
		int partition = 0;
		long offset = 0;
		Timestamp timestamp = Timestamp.getDefaultInstance();
		while (!in.isAtEnd()) {
			int tag = in.readTag();
			switch (WireFormat.getTagFieldNumber(tag)) {
				case 1 -> key = in.readBytes();
				case 2 -> topic = in.readStringRequireUtf8();
				case 3 -> partition = in.readInt32();
				case 4 -> offset = in.readInt64();
				case 5 -> timestamp = Timestamp.parseFrom(in.readBytes());
				default -> in.skipField(tag);
			}
		}
		return new DecodedMeta(key, topic, partition, offset, timestamp);
	}

	private record DecodedEnvelope(DecodedMeta meta, ByteString payload) {}

	private record DecodedMeta(
			ByteString key,
			String topic,
			int partition,
			long offset,
			Timestamp timestamp) {}
}
