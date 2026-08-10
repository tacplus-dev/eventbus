// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.grpc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.gitlab.secondstarcavalier.controlplane.schematic.services.eventbus.kafka.KafkaSubscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.WireFormat;

class SubscribeHandlerTest {

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
