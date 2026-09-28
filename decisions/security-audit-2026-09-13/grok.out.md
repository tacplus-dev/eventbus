I'll run a dedicated security review in parallel and inspect `server/src` and `codegen/src` myself for correctness and pattern issues. This is read-only.

● Security-review (requested model: grok-4.6) Eventbus security review
  └ Agent started in background with agent_id: 6df9a9f2-2bb9-4441-ad82-2ec3aea0b6...

● Todo added 2 items
  │ Running security-review specialist
  └ Auditing correctness and patterns

/ Search (glob)
  │ "**/*.{java,kt,xml,yml,yaml}" in server/src
  └ 24 files found

/ Search (glob)
  │ "**/*.{java,kt,xml,yml,yaml}" in codegen/src
  └ 10 files found

Security review is running. I'll read the production Java sources next.

● Read PublishHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /PublishHandler.java
  └ 1 line read

● Read EventBusHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Even
  │ tBusHandler.java
  └ 1 line read

● Todo started quality-audit

● Read EventBusServiceImpl.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /EventBusServiceImpl.java
  └ 311 lines read

● Read SubscribeHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /SubscribeHandler.java
  └ 419 lines read

● Read Main.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Main
  │ .java
  └ 16 lines read

● Read WatchTraceHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /WatchTraceHandler.java
  └ 99 lines read

● Read Bootstrap.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Boot
  │ strap.java
  └ 37 lines read

● Read KafkaPublisher.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaPublisher.java
  └ 91 lines read

● Read EventBusHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Even
  │ tBusHandler.java
  └ L1:250 (250 lines read)

● Read PublishHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /PublishHandler.java
  └ L1:250 (250 lines read)

● Read KafkaSubscriber.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaSubscriber.java
  └ L1:250 (250 lines read)

● Read KafkaSubscriber.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaSubscriber.java
  └ L250:550 (301 lines read)

● Read PublishHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /PublishHandler.java
  └ L250:550 (205 lines read)

● Read EventBusHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Even
  │ tBusHandler.java
  └ L250:550 (301 lines read)

● Read TopicManager.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/TopicManager.java
  └ L1:250 (250 lines read)

● Read EventBusHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Even
  │ tBusHandler.java
  └ L550:900 (351 lines read)

● Read TopicManager.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/TopicManager.java
  └ L250:500 (143 lines read)

● Read KafkaSubscriber.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaSubscriber.java
  └ L550:900 (172 lines read)

/ Search (grep)
  │ "auth_disabled|namespacePinned|boundConsumerGroup|knownTopics|pageStart|Integer.parseInt" in
  │ **/*.{java,go,proto} (server/src/main)
  └ 7 files found

● Read TopicDiscovery.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/TopicDiscover
  │ y.java
  └ L1:200 (200 lines read)

● Read EventBusHandler.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Even
  │ tBusHandler.java
  └ L900:1400 (165 lines read)

● Read TypedRpcGenerator.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/generator/Typ
  │ edRpcGenerator.java
  └ L1:200 (200 lines read)

/ Search (glob)
  │ "**/*.java" in server/src/main/java/com/gitlab/seabeam/controlplane/schematic/service
  │ s/eventbus/kafka
  └ 4 files found

● Read TypedRpcGenerator.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/generator/Typ
  │ edRpcGenerator.java
  └ L350:750 (76 lines read)

● Read TopicDiscovery.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/TopicDiscover
  │ y.java
  └ L200:500 (301 lines read)

● Read KafkaEmbeddedBroker.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaEmbeddedBroker.java
  └ L1:250 (250 lines read)

/ Search (grep)
  │ "emitLazyPublishBinding|emitRegisterMethodAuth|hasNamespaceField|auth_services|fail" in
  │ **/TypedRpcGenerator.java
  └ 1 file found

● Read KafkaEmbeddedBroker.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaEmbeddedBroker.java
  └ L250:500 (163 lines read)

● Read EventBusMojo.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/EventBusMojo.
  │ java
  └ 76 lines read

● Read TopicRegistryGenerator.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/generator/Top
  │ icRegistryGenerator.java
  └ 83 lines read

/ Search (grep)
  │ "emitRegisterMethodAuth|registerMethodAuth|no registered|fail" in **/TypedRpcGenerator.java
  └ 1 file found

/ Search (grep)
  │ "handlerImpl" in **/*.java
  └ 1 file found

● Read TypedRpcGenerator.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/generator/Typ
  │ edRpcGenerator.java
  └ L200:360 (161 lines read)

● Read TopicAuthorizationTest.java
  │ server/src/test/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Topi
  │ cAuthorizationTest.java
  └ L1:80 (79 lines read)

● Read SubscribeTenantFilterTest.java
  │ server/src/test/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /SubscribeTenantFilterTest.java
  └ L1:120 (119 lines read)

/ Search (grep)
  │ "registerMethodAuth" in **/*.java
  └ 3 files found

● Read logback.xml
  │ server/src/main/resources/logback.xml
  └ 38 lines read

● Read ClaimsProvenanceTest.java
  │ server/src/test/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/Clai
  │ msProvenanceTest.java
  └ L1:80 (80 lines read)

● Read TopicDiscovery.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/TopicDiscover
  │ y.java
  └ L500:750 (102 lines read)

● Read ConsumerGroupBindingTest.java
  │ server/src/test/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /ConsumerGroupBindingTest.java
  └ L1:80 (80 lines read)

/ Search (grep)
  │ "EventBusServiceImpl|new EventBusService" in **/*.{java,xml}
  └ 2 files found

/ Search (grep)
  │ "setActorSpiffeId|actor_spiffe" in **/*.java
  └ 5 files found

● Read StringUtils.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/StringUtils.j
  │ ava
  └ 42 lines read

● Read CodegenConfig.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/CodegenConfig
  │ .java
  └ 32 lines read

● Read NamespaceStampTest.java
  │ server/src/test/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/grpc
  │ /NamespaceStampTest.java
  └ L1:100 (99 lines read)

/ Search (grep)
  │ "bigdata" in **/*.{java,json}
  └ 1 file found

/ Search (grep)
  │ "eventbus-trace-dispatch|auth_disabled" in **/*.{java,proto}
  └ 2 files found

● Read TopicDiscoveryTest.java
  │ codegen/src/test/java/com/gitlab/seabeam/controlplane/schematic/codegen/TopicDiscover
  │ yTest.java
  └ L1:80 (79 lines read)

● Read KafkaSubscriber.java
  │ server/src/main/java/com/gitlab/seabeam/controlplane/schematic/services/eventbus/kafk
  │ a/KafkaSubscriber.java
  └ L620:680 (61 lines read)

/ Search (grep)
  │ "addRemainingLazy|EventBusTypedService" in **/*.java
  └ 2 files found

/ Search (grep)
  │ "already|setNamespace|other" in **/FleetRecordNamespaceStampTest.java
  └ 1 file found

/ Search (grep)
  │ "already|other tenant|setNamespace|keeps" in **/NamespaceStampTest.java
  └ 1 file found

● Read TypedRpcGenerator.java
  │ codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codegen/generator/Typ
  │ edRpcGenerator.java
  └ L1:50 (50 lines read)

/ Search (grep)
  │ "void" in **/NamespaceStampTest.java
  └ 1 file found

/ Search (grep)
  │ "handlerImpl" in **/EventBusHandler.java
  └ 1 file found

/ Search (grep)
  │ "void" in **/FleetRecordNamespaceStampTest.java
  └ 1 file found

Quality pass is done. Waiting on the security specialist so I can merge findings into one ranked report.

✔ Todo completed quality-audit

Security review is still running. I’ll merge those results with the correctness/pattern findings as soon as it finishes.

● Read (Security-review agent — Eventbus security review)
  └ idle

✔ Todo completed security-review

Confirming exact line numbers for the quality findings, then I’ll publish the ranked report.

/ Search (grep)
  │ "catch \(Exception e\) \{" in **/kafka/KafkaSubscriber.java
  └ 1 file found

/ Search (grep)
  │ "eventbus-trace-dispatch|getAllNodes|knownTopics|setConfig" in **/EventBusHandler.java
  └ 1 file found

/ Search (grep)
  │ "setOnCancelHandler|auth_disabled" in **/WatchTraceHandler.java
  └ 1 file found

/ Search (grep)
  │ "knownTopics" in **/TopicManager.java
  └ 1 file found

/ Search (grep)
  │ "if \(newParams != null\)" in **/KafkaSubscriber.java
  └ 1 file found

/ Search (grep)
  │ "Failed to parse record|firstMessage.compareAndSet|namespacePinned" in **/SubscribeHandler.java
  └ 1 file found

/ Search (grep)
  │ "return request;" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/ser
  │ vices/eventbus/EventBusHandler.java)
  └ 1 line found

/ Search (grep)
  │ "eventbus-trace-dispatch" (server/src/main/java/com/gitlab/seabeam/controlplane/schem
  │ atic/services/eventbus/EventBusHandler.java)
  └ 2 lines found

/ Search (grep)
  │ "if \(newParams != null\)" (server/src/main/java/com/gitlab/seabeam/controlplane/sche
  │ matic/services/eventbus/kafka/KafkaSubscriber.java)
  └ 1 line found

/ Search (grep)
  │ "if \(publisher != null\)" (server/src/main/java/com/gitlab/seabeam/controlplane/sche
  │ matic/services/eventbus/EventBusHandler.java)
  └ 1 line found

/ Search (grep)
  │ "actorStamper != null" (server/src/main/java/com/gitlab/seabeam/controlplane/schemati
  │ c/services/eventbus/grpc/PublishHandler.java)
  └ 1 line found

/ Search (grep)
  │ "Failed to parse record|firstMessage.compareAndSet" (server/src/main/java/com/gitlab/secondstarc
  │ avalier/controlplane/schematic/services/eventbus/grpc/SubscribeHandler.java)
  └ 2 lines found

/ Search (grep)
  │ "knownTopics" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/service
  │ s/eventbus/kafka/TopicManager.java)
  └ 6 lines found

/ Search (grep)
  │ "controller.id" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/servi
  │ ces/eventbus/kafka/TopicManager.java)
  └ 2 lines found

/ Search (grep)
  │ "bigdata_name" (codegen/src/main/java/com/gitlab/seabeam/controlplane/schematic/codeg
  │ en/TopicDiscovery.java)
  └ 1 line found

/ Search (grep)
  │ "sessionsFailed.incrementAndGet" (server/src/main/java/com/gitlab/seabeam/controlplan
  │ e/schematic/services/eventbus/kafka/KafkaSubscriber.java)
  └ 1 line found

/ Search (grep)
  │ "stateStub.getAllNodes|setRole" (server/src/main/java/com/gitlab/seabeam/controlplane
  │ /schematic/services/eventbus/EventBusHandler.java)
  └ 2 lines found

/ Search (grep)
  │ "setOnCancelHandler" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/
  │ services/eventbus/grpc/WatchTraceHandler.java)
  └ 1 line found

/ Search (grep)
  │ "handlerImpl =" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/servi
  │ ces/eventbus/EventBusHandler.java)
  └ 1 line found

/ Search (grep)
  │ "peer.getNodeId" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/serv
  │ ices/eventbus/EventBusHandler.java)
  └ 2 lines found

/ Search (grep)
  │ "consumer.poll\(Duration.ofMillis\(5000\)\)" (server/src/main/java/com/gitlab/seabeam
  │ /controlplane/schematic/services/eventbus/kafka/KafkaSubscriber.java)
  └ 1 line found

/ Search (grep)
  │ "EventBusTypedService|addRemainingLazy" (codegen/src/main/java/com/gitlab/seabeam/con
  │ trolplane/schematic/codegen/generator/TypedRpcGenerator.java)
  └ 6 lines found

/ Search (grep)
  │ "if \(subscriber" (server/src/main/java/com/gitlab/seabeam/controlplane/schematic/ser
  │ vices/eventbus/EventBusHandler.java)
  └ 1 line found

/ Search (grep)
  │ "Failed to update TLS" (server/src/main/java/com/gitlab/seabeam/controlplane/schemati
  │ c/services/eventbus/kafka/KafkaEmbeddedBroker.java)
  └ 1 line found

/ Search (grep)
  │ "private class EventBusServiceHandlerImpl" (server/src/main/java/com/gitlab/seabeam/c
  │ ontrolplane/schematic/services/eventbus/EventBusHandler.java)
  └ No matches found

/ Search (grep)
  │ "class EventBusServiceHandlerImpl" (server/src/main/java/com/gitlab/seabeam/controlpl
  │ ane/schematic/services/eventbus/EventBusHandler.java)
  └ 1 line found

/ Search (grep)
  │ "public void shutdown" (server/src/main/java/com/gitlab/seabeam/controlplane/schemati
  │ c/services/eventbus/EventBusHandler.java)
  └ 1 line found

## Security

| # | Severity | File | Lines | Vulnerability | Confidence |
|---|----------|------|-------|---------------|------------|
| 1 | 🟠 HIGH | server/src/main/java/.../grpc/PublishHandler.java | 339 | Tenant write isolation bypass: a pre-set `namespace` is accepted as-is, so any publisher allowed on the topic can inject events into another tenant’s stream | 8/10 |

**Failure:** Tenant-owned service `/tenants/acme` publishes with `namespace=/tenants/beta`. Topic auth is service-pattern/`["*"]`, not tenant. Subscribe pins on the row’s field, so beta (and `/system` readers) receive acme’s event.

---

## Correctness / anti-patterns / drift

1. **`KafkaSubscriber.java:392-394`** — `run()` swallows all exceptions except assignment timeout. **Failure:** `OffsetOutOfRange` / commit failure → gRPC `onCompleted` instead of `onError`; client thinks the stream ended cleanly.

2. **`EventBusHandler.java:193`** — Trace dispatcher uses shared group `eventbus-trace-dispatch`. **Failure:** Multi-broker eventbus splits partitions; WatchTrace on node A misses events consumed on node B.

3. **`EventBusHandler.java:1022-1024`** — Peer discovery adds every EventbusState node as a KRaft voter, ignoring role. **Failure:** Dedicated broker nodes join `controller.quorum.voters` though they have no CONTROLLER listener; quorum hangs/splits.

4. **`SubscribeHandler.java:259-270`** — One `InvalidProtocolBufferException` fails the whole subscribe. **Failure:** A poison-pill record on a retained topic kills every new subscriber until retention expires.

5. **`SubscribeHandler.java:228`** — `firstMessage` is CAS’d before `createSession`. **Failure:** If create/poll throws, later `onNext`s no-op (`session == null`); stream hangs.

6. **`KafkaSubscriber.java:472`** — Resubscribe applies seek/speed/pause only. **Failure:** Client sends a new `consumer_group` or `partitions` list; Kafka session keeps the original group/assignment with no error.

7. **`TopicManager.java:30,50-66`** — `knownTopics` is a non-thread-safe `HashSet`. **Failure:** Concurrent first publishes can corrupt the set or skip create.

8. **`WatchTraceHandler.java:35,65`** — `auth_disabled`; only `onCancel` removes entries; no cap. **Failure:** Client disconnect without cancel leaks observers; unauthenticated callers can open unbounded streams.

9. **`EventBusHandler.java:633`** — `setConfig` returns the request and does not apply it. **Failure:** Clients believe broker config changed.

10. **`PublishHandler.java:320-324`** — `actor_spiffe_id` is stamped only when `act` is present; client-supplied values are kept. **Failure:** Publisher without a delegated token can forge actor provenance.

11. **`EventBusHandler.java:643-658`** — Shutdown closes publisher/broker/trace session, not live subscribe sessions. **Failure:** In-flight Kafka consumers run against a shutting broker.

12. **`TopicManager.java:352-355`** — `controller.id()` with no null check. **Failure:** Health/admin NPEs during controller election.

13. **`KafkaEmbeddedBroker.java:220`** — TLS renewal errors are logged and ignored. **Failure:** Broker keeps the expired cert; inter-node mTLS dies later.

14. **`TopicDiscovery.java:559-568`** — `bigdata_name` topics use a constructor that drops namespace/actor/auth. **Failure:** Those topics skip tenant pinning and can fail fail-closed auth generation.

15. **Pattern drift** — Live admin is `getAdminGrpcImpl()`; `EventBusServiceImpl` and unused `EventBusServiceHandlerImpl` (`EventBusHandler.java:187,727`) duplicate it. Codegen javadoc still refers to `EventBusTypedService` / `addRemainingLazy`.

16. **`KafkaSubscriber.java:686`** — Initial 5s `poll()` runs on the gRPC thread before the virtual-thread loop. **Failure:** Concurrent first subscribes stall the server call executor.

---

Reply with a number:
1. **Fix highest severity issues**
2. **Fix all issues**
3. **Commit a summary of findings**

