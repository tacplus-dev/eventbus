Read-only audit completed. I found 15 high-confidence issues in the requested sources. No files were modified.

## Critical

1. **Tenant namespace can be forged by a publisher** — [PublishHandler.java:326](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/PublishHandler.java:326)  
   The handler preserves any non-empty message namespace without checking it against the authenticated caller’s namespace or delegated authority.  
   **Failure scenario:** A tenant-scoped service authorized to publish constructs an event with another tenant’s namespace; the event is stored as belonging to that tenant and delivered to that tenant’s subscribers.

2. **Trace delivery breaks across multiple eventbus instances** — [EventBusHandler.java:191](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:191)  
   Every instance consumes trace events through the same fixed Kafka group, although each instance owns a different in-memory set of `WatchTrace` streams.  
   **Failure scenario:** A trace stream is connected to instance A, but Kafka assigns its event to instance B; B does not recognize the trace ID and drops the event, so the client loses trace rounds nondeterministically.

## High

3. **Prefix matching grants fleet-wide access to non-system namespaces** — [SubscribeHandler.java:163](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/SubscribeHandler.java:163)  
   `startsWith("/system")` treats `/systematic`, `/system-tenant`, and similar namespaces as privileged system callers.  
   **Failure scenario:** A valid token bearing such a namespace subscribes to a namespace-bearing topic and receives events from every tenant instead of only its own.

4. **Broker-only nodes are inserted into the controller quorum** — [EventBusHandler.java:1017](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:1017)  
   Peer discovery adds every registered node—and always adds self—to `controller.quorum.voters` without filtering by `KafkaNodeRole`.  
   **Failure scenario:** In a split broker/controller topology, quorum membership includes broker-only nodes with no controller listener, preventing elections or destroying quorum availability.

5. **Runtime peer discovery produces incompatible static quorum configurations** — [EventBusHandler.java:994](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:994)  
   Each node snapshots whichever peers happen to be registered at its startup, but KRaft voter membership is static and existing nodes are never reconfigured.  
   **Failure scenario:** The first controller starts with only itself, while later controllers start with larger voter lists; nodes disagree about quorum membership and the intended multi-controller cluster cannot reliably form.

6. **Initialization retries leak a successfully started broker and can never recover** — [EventBusHandler.java:165](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:165)  
   `initialize()` publishes partially constructed resources directly to fields, while `initializeWithRetry()` retries without closing them.  
   **Failure scenario:** Kafka starts but construction of `TopicManager`, publisher, subscriber, or trace session fails; the next retry attempts to bind the already-used ports and thereafter fails indefinitely while resources and scheduled refresh jobs accumulate.

7. **Unexpected consumer failures are converted into successful end-of-stream** — [KafkaSubscriber.java:391](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java:391), [SubscribeHandler.java:280](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/SubscribeHandler.java:280)  
   `Session.run()` logs and swallows all non-assignment exceptions, after which the gRPC handler calls `onCompleted()`.  
   **Failure scenario:** An invalid seek, authorization-related broker error, deserialization callback failure, or Kafka outage terminates consumption, but the client sees a normal successful completion and may not retry or alert.

8. **A future timestamp subscription can replay the entire topic** — [KafkaSubscriber.java:701](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java:701)  
   When `offsetsForTimes()` returns `null` because no record exists at or after the requested timestamp, the partition is left at its prior position.  
   **Failure scenario:** A new random-group subscriber requests a timestamp newer than the latest record; because `auto.offset.reset=earliest`, it receives the full retained history instead of no records.

9. **Interrupting a partially processed poll batch can commit undelivered records** — [KafkaSubscriber.java:547](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java:547), [KafkaSubscriber.java:618](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java:618)  
   The loop breaks on close or pending resubscribe but then commits Kafka’s position for the entire fetched batch.  
   **Failure scenario:** A client disconnects while a large or paced batch is being processed; records remaining in that batch are committed without delivery and are permanently skipped for other consumers using the group.

## Medium

10. **Publish processing continues after the response stream has failed** — [PublishHandler.java:267](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/PublishHandler.java:267)  
    `onNext()` does not check `terminated` before validating and publishing additional messages.  
    **Failure scenario:** One message fails validation or namespace checks and terminates the stream, but already-dispatched subsequent `onNext` calls still write events to Kafka even though the client received a failure and no acknowledgements for them.

11. **A trace ID leaks from one streamed publish into later messages** — [PublishHandler.java:300](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/PublishHandler.java:300)  
    The mutable header map adds `trace_id` for traced messages but never removes it when a later message has no trace ID.  
    **Failure scenario:** A bidi publish stream sends one traced event followed by an untraced event; the second Kafka record incorrectly carries the first event’s trace ID, corrupting provenance and trace correlation.

12. **Changing `consumer_group` mid-stream is silently ignored** — [SubscribeHandler.java:288](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/grpc/SubscribeHandler.java:288), [KafkaSubscriber.java:639](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java:639)  
    Resubscribe replaces `SeekParams`, but the Kafka consumer’s group ID is immutable and was fixed when the first request created it.  
    **Failure scenario:** A client sends a second subscribe request naming another group and believes subsequent offsets belong to it, while consumption and commits continue under the original group.

13. **Controller-only nodes are always reported healthy after initialization** — [EventBusHandler.java:268](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:268)  
    Health checking returns healthy whenever `topicManager` is null, which is permanently true for the supported controller-only role.  
    **Failure scenario:** A controller wedges or loses quorum after startup; its liveness check remains healthy indefinitely, so orchestration never restarts it.

14. **TLS renewal failure is swallowed and then reported as successful** — [KafkaEmbeddedBroker.java:198](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/kafka/KafkaEmbeddedBroker.java:198), [EventBusHandler.java:335](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/server/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/services/eventbus/EventBusHandler.java:335)  
    `updateTls()` catches every exception and returns normally, after which its caller logs that renewal succeeded.  
    **Failure scenario:** Kafka rejects a dynamic certificate update; the service emits a false success message and continues using the old certificate until it expires and inter-node communication fails.

15. **Codegen infers tenant boundaries from the field name instead of the annotation** — [TopicDiscovery.java:462](/Users/jody/workspace/gitlab/secondstarcavalier/schematic/platform/services/eventbus/codegen/src/main/java/com/gitlab/secondstarcavalier/controlplane/schematic/codegen/TopicDiscovery.java:462)  
    Any string field literally named `namespace` receives tenant enforcement, while an annotated tenant field with another name would not.  
    **Failure scenario:** A valid event type introduces an ordinary business field named `namespace` and is unexpectedly filtered/stamped, or a `(spec.store_namespace)` field is renamed and its generated publish/subscribe paths lose tenant isolation entirely.

The most urgent fixes are the caller-supplied namespace trust, trace consumer grouping, and KRaft voter discovery; those directly affect tenant isolation or core multi-node operation.
