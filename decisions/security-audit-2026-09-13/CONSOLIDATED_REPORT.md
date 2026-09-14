# eventbus Security & Quality Audit — Consolidated Report
2026-09-13 — multimodal review: Codex CLI (GPT), Grok 4.6 (via GitHub Copilot CLI), Fable 5.1 (Claude)
Scope: `server/src` (23 Java files) + `codegen/src` (10 files, eventbus-specific codegen).

eventbus is the platform's Kafka-backed event bus — every record write/read that isn't a direct
kernel state call flows through it, and it also runs the embedded Kafka/KRaft cluster itself.

---

## Critical — Admin RPCs (delete/reset any topic or consumer group) have no caller check at all [VERIFIED]

**File:** `platform/proto/services/eventbus.proto:222-239` (the `Admin` service, `auth_services: ["*"]`),
handler at `server/.../EventBusHandler.java:397-560` (live) and `:831-930` (dead duplicate, see Low).

Found by Fable, and I read this directly: `Admin`'s proto policy grants every registered platform
service equal access — not scoped to eventbus's own tenant/service boundaries, just "any
authenticated service." Every admin method (`deleteTopic`, `resetConsumerGroupOffsets`,
`deleteConsumerGroup`, `listConsumerGroups`) takes a `Caller caller` parameter and never references
it — confirmed by reading each method body, `caller` is dead on arrival in all four.

**Concretely:** `SubscribeHandler.boundConsumerGroup` (verified, `SubscribeHandler.java:363-364`)
deliberately namespaces every bound consumer group by the caller's own SPIFFE subject specifically
so no other caller can join or touch it — real, working isolation on the subscribe path. The Admin
surface routes straight around that: **any service on the platform can call
`DeleteConsumerGroup`/`ResetConsumerGroupOffsets` naming stratum's or mailbox's own bound group**,
forcing it to replay its entire backlog or silently skip everything published since the reset, or
call `DeleteTopic` on any topic outright. `ListConsumerGroups` additionally discloses every caller's
SPIFFE subject to any service that asks.

**Fix shape:** Admin needs real per-caller scoping — at minimum, a service should only be able to
name its own consumer groups (mirroring `boundConsumerGroup`'s existing `callerSub|group` scheme),
and topic deletion needs an actual entitlement check, not `auth_services: ["*"]`.

## Reviewed, not a bug: the publish-time namespace trust is a deliberate, documented design decision

Codex flagged `PublishHandler.java`'s namespace-stamping as a Critical tenant-forgery bug (a
pre-set `namespace` on a published row is kept as-is, not overwritten from the caller's own
token). **I read the code and the design record it cites** (`decisions/
namespace-fix-corrected-2026-09-02.md`, `PublishHandler.java:326-353`'s own comment): this is
intentional. A publishing service that has already authenticated the real acting user in its own
request context (the same trust `acted_by`/`(spec.store_principal)` rides on) is allowed to set the
row's tenant directly, so a service can publish on a user's behalf into that user's own tenant. An
empty namespace still gets stamped from the publisher's own token, unchanged.

**Still worth a second look, which is what Fable actually recommended:** this makes the *topic's*
`auth_services` entitlement list the entire tenant-isolation boundary for that topic — literally
any service named there can set literally any namespace on a published row, with no per-field
validation that the namespace it's setting is one that service is actually allowed to write for.
That's fine if every topic's `auth_services` list is kept tight and audited, and is a real gap the
moment it isn't. Worth a standing check (as part of a schema/topic review, not a code fix) that no
topic's `auth_services` is broader than the set of services that legitimately construct rows for
every tenant on it.

## High — real correctness bugs, several in the KRaft/cluster-membership family

- **Broker-only nodes get added to the KRaft controller voter list; role is stored but never
  checked** [VERIFIED — Codex + Fable + Grok, all three independently, and I confirmed
  `EventbusNode.role` is set but never read in the cited range] — `EventBusHandler.java:994-1028`.
  Peer discovery adds every registered node (and always self) to `controller.quorum.voters`
  regardless of `KafkaNodeRole`. Kafka 3.9 rejects a broker-only node id in the voter set outright
  (Fable confirmed against the actual jar's error message), so a mixed broker/controller topology
  fails to form a quorum, or fails startup on the broker-only node.
- **Quorum voter lists are snapshotted per-node at startup and never reconciled** (Codex) — the
  first controller starts believing it's the only voter; later controllers see a larger, different
  list. KRaft voter membership is static once written, so nodes can permanently disagree about who's
  in the quorum.
- **Cross-instance trace delivery is broken by construction** [VERIFIED, all three tools] —
  `EventBusHandler.java:191-207` consumes the trace topic under one fixed consumer group shared by
  every eventbus instance, while `WatchTraceHandler.traceStreams` (the registry of who's watching
  what) is per-JVM. With more than one instance, Kafka can and will hand a trace event to the
  instance that isn't holding the watcher for it, and that instance drops it silently — trace rounds
  vanish nondeterministically, worse as instance count grows.
- **Any mid-stream failure completes the gRPC call as a clean success with zero further records**
  [VERIFIED — read directly] — `KafkaSubscriber.java:391-393` catches every non-assignment
  exception, logs it, and returns; `SubscribeHandler.java:280-285` then calls `onCompleted()`. An
  invalid seek, a bad offset, a broker-side authorization failure, or an outage all look identical
  to the client: a normal, complete stream. No error, no reason to retry.
- **`PublishHandler` keeps accepting and writing messages on a bidi stream after it has already
  failed that stream** (Codex + Fable) — `onNext` has no guard checking `terminated` at the top,
  unlike `SubscribeHandler`. A validation or namespace failure on message 3 of 5 still lets 4 and 5
  land in Kafka with no ack ever sent for them.
- **A `trace_id` sticks to every subsequent message on a publish stream once set once** [VERIFIED —
  read directly, `PublishHandler.java:300-311`] — added to the shared, mutable `callerHeaders` map
  and never cleared. One traced message followed by any number of untraced ones stamps all of them
  with the first message's trace id.
- **Consumer offsets are committed but a reconnect always overrides them back to LATEST** (Fable) —
  an unset `start_from` maps to `LATEST` unconditionally, so a bound-group subscriber that
  disconnects and reconnects skips everything published in the gap regardless of what was already
  committed for that group.

## Medium
- A future-timestamp subscribe on an empty/exhausted range falls back to replaying the entire
  topic instead of returning nothing, because `offsetsForTimes()` returning null is treated as "stay
  at prior position" under `auto.offset.reset=earliest` (Codex).
- Resubscribing with a different `consumer_group` or `partitions` list is silently ignored — the
  original Kafka consumer group/assignment from the first request just keeps running (Codex + Grok).
- A poll batch interrupted by close/resubscribe still commits the whole fetched batch's position,
  permanently skipping whatever in that batch was never delivered (Codex + Fable).
- Controller-only nodes report healthy forever once `topicManager` is null-by-role, so a wedged or
  quorum-less controller is never restarted by orchestration (Codex).
- TLS renewal failures are logged and swallowed as success; the old cert stays in place silently
  until it expires and inter-node mTLS breaks with no prior warning (all three tools).
- `actor_spiffe_id` is stamped only when a delegated `act` claim is present; otherwise a
  client-supplied value on the row is kept as-is, letting a non-delegated publisher forge actor
  provenance on the row (Grok).
- Codegen infers the tenant boundary from a field literally named `namespace` rather than from the
  `(spec.store_namespace)` annotation, and separately detects trace types by name suffix
  (`endsWith("TraceEvent")`) rather than annotation — either an ordinary field named `namespace` gets
  incorrectly tenant-filtered, or a real tenant field under another name gets no isolation at all
  (all three tools, independently).

## Low / anti-patterns / dead code
- Three copies of the admin/service logic exist; two are dead (`EventBusServiceImpl`, unused;
  `EventBusServiceHandlerImpl` instantiated but never read) and already diverge in error handling
  from the one actually registered (`getAdminGrpcImpl`) — a live bug fixed in one copy would stay
  broken in the others if anyone edited the wrong one (Fable + Grok).
- `TopicManager.knownTopics` is a plain, non-thread-safe `HashSet` mutated from concurrent
  `PublishHandler` constructors on every new stream (Fable + Grok).
- The `bigdata_name` codegen discovery path builds an incomplete `TopicField` that would throw at
  generation time the moment anyone actually uses a `bigdata_name` annotation — dead code today, a
  build break waiting to happen (Fable + Grok).
- Anonymous/unbound subscribes each leave a permanent, never-cleaned-up consumer group and committed
  offsets behind in `__consumer_offsets`, contributing to the exact metadata-log churn the broker's
  own tuning elsewhere is trying to bound (Fable).
- Normal client cancellation on a subscribe stream is counted and logged as a session failure rather
  than handled as an expected shutdown path (Fable).
- `setConfig` accepts a request and echoes it back without actually applying anything (Grok).
- WatchTrace has no cap on open streams and its javadoc claims an `auth_disabled` option the proto
  doesn't declare (Grok).

## Recommended next step
Fix the Admin-surface authorization gap first — it's a small, well-localized change (scope every
admin RPC's group/topic argument to the caller, mirroring `boundConsumerGroup`'s existing pattern)
and it's the one finding here that lets any service reach across tenant/service lines today, no
edge case required. Then the KRaft voter-list role filtering, since a real mixed broker/controller
deployment cannot reliably start without it.

## Raw per-tool output
- Codex: `codex.out.md` (15 findings)
- Grok 4.6: `grok.out.md` (16 findings — did not independently surface the Admin-surface authz gap)
- Fable 5.1: `fable.out.md` (17 findings — the only tool to find and correctly characterize the
  Admin authz gap; also the one that caught the namespace "bug" was actually a documented decision)
