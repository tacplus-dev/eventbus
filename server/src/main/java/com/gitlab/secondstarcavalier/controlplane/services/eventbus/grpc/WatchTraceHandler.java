// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.secondstarcavalier.controlplane.services.eventbus.grpc;

import com.gitlab.secondstarcavalier.controlplane.services.eventbus.tacplus.types.libs.trace.Trace;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implements the WatchTrace RPC on the eventbus.Trace gRPC service.
 *
 * WatchTrace opens a server-streaming trace session:
 * 1. Server generates a unique trace_id and sends it as the first TraceEvent
 * 2. Client attaches the trace_id to subsequent requests
 * 3. Services processing traced requests publish TraceEvents to the trace topic
 * 4. EventBus dispatches matching events to this stream via {@link #dispatch}
 * 5. On disconnect, the trace_id entry is cleaned up
 *
 * No authentication — the trace_id itself is the access credential.
 */
public class WatchTraceHandler {

    private static final Logger logger = LoggerFactory.getLogger(WatchTraceHandler.class);
    private static final String SERVICE_NAME = "eventbus.Trace";

    public static final MethodDescriptor<Trace.TraceRequest, Trace.TraceEvent> WATCH_TRACE =
            MethodDescriptor.<Trace.TraceRequest, Trace.TraceEvent>newBuilder()
                    .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                    .setFullMethodName(SERVICE_NAME + "/WatchTrace")
                    .setRequestMarshaller(ProtoUtils.marshaller(Trace.TraceRequest.getDefaultInstance()))
                    .setResponseMarshaller(ProtoUtils.marshaller(Trace.TraceEvent.getDefaultInstance()))
                    .build();

    private final ConcurrentHashMap<String, StreamObserver<Trace.TraceEvent>> traceStreams =
            new ConcurrentHashMap<>();

    /**
     * Dispatches a trace event to the active WatchTrace stream matching the trace_id.
     * Unknown trace_ids are silently dropped.
     */
    public void dispatch(Trace.TraceEvent event) {
        String traceId = event.getTraceId();
        if (traceId.isEmpty()) {
            return;
        }
        StreamObserver<Trace.TraceEvent> observer = traceStreams.get(traceId);
        if (observer != null) {
            try {
                synchronized (observer) {
                    observer.onNext(event);
                }
            } catch (Exception e) {
                logger.debug("Trace stream {} send failed, removing", traceId, e);
                traceStreams.remove(traceId);
            }
        }
    }

    /**
     * Returns the gRPC service definition for eventbus.Trace.
     */
    public ServerServiceDefinition bindService() {
        ServerCallHandler<Trace.TraceRequest, Trace.TraceEvent> handler =
                ServerCalls.asyncServerStreamingCall(this::watchTrace);
        return ServerServiceDefinition.builder(SERVICE_NAME)
                .addMethod(WATCH_TRACE, handler)
                .build();
    }

    private void watchTrace(Trace.TraceRequest request, StreamObserver<Trace.TraceEvent> responseObserver) {
        String traceId = UUID.randomUUID().toString();

        // Register cleanup on client disconnect
        if (responseObserver instanceof ServerCallStreamObserver) {
            ServerCallStreamObserver<Trace.TraceEvent> serverObserver =
                    (ServerCallStreamObserver<Trace.TraceEvent>) responseObserver;
            serverObserver.setOnCancelHandler(() -> {
                traceStreams.remove(traceId);
                logger.info("Trace stream {} closed (client cancelled)", traceId);
            });
        }

        traceStreams.put(traceId, responseObserver);
        logger.info("Trace stream {} opened", traceId);

        // First message: TraceEvent with only trace_id set (the "here's your trace_id" message)
        responseObserver.onNext(Trace.TraceEvent.newBuilder()
                .setTraceId(traceId)
                .build());
    }

    /** Returns the number of active trace streams (for diagnostics). */
    public int activeStreamCount() {
        return traceStreams.size();
    }

    /** Closes all active trace streams during shutdown. */
    public void shutdown() {
        traceStreams.forEach((traceId, observer) -> {
            try {
                synchronized (observer) {
                    observer.onCompleted();
                }
            } catch (Exception e) {
                // Stream already closed
            }
        });
        traceStreams.clear();
        logger.info("WatchTrace shutdown, all trace streams closed");
    }
}
