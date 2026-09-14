# Scoping the Admin RPCs to their caller

**Date:** 2026-09-13
**Status:** CODE CHANGED, TESTED LOCALLY — not deployed, not committed.
**Fixes:** the one Critical finding in `decisions/security-audit-2026-09-13/CONSOLIDATED_REPORT.md`
— "Admin RPCs (delete/reset any topic or consumer group) have no caller check at all".
**Touches:** `server/.../EventBusHandler.java` (the live admin impl, `getAdminGrpcImpl()`),
`server/.../grpc/SubscribeHandler.java` (two helpers next to `boundConsumerGroup`),
`server/.../AdminCallerScopingTest.java` (new).

---

## 1. The gap

`Admin` is declared `option (spec.service_s2s) = {auth_services: ["*"]}`
(`platform/proto/services/eventbus.proto:222`). Every registered platform service therefore clears
the Authorizer on every admin method, and each of those methods took a `Caller caller` and never
read it — so the request's own `group_id` / `topic` was the only thing that decided what got
deleted or rewound.

Meanwhile `SubscribeHandler.boundConsumerGroup` namespaces every bound consumer group under the
caller's verified SPIFFE subject (`callerSub|group`) precisely so no other caller can join or
touch it. Admin routed around that: any service could name stratum's or mailbox's bound group and
`DeleteConsumerGroup` it (full backlog replay on the next connect) or
`ResetConsumerGroupOffsets` it (silent skip of everything published since), or `DeleteTopic`
outright. `ListConsumerGroups` handed back every caller's SPIFFE subject, since the subject *is*
the group-id prefix.

## 2. What tacplus already gives us, and what it does not

Checked `model/target/generated-sources/tacplus/` (i.e. `model/target/generated-sources/model/`,
where this repo's tacplus output actually lands) before writing anything:

- `Authorizer.setMethodAuth(methodKey, patterns)` is **per-RPC**, not per-service. The generated
  `Admin.java:972-980` registers all nine admin methods, each with `List.of("*")` — one line per
  RPC, fed from the service-level default.
- `spec.proto:1257` confirms the proto can express a narrower policy per RPC:
  `(spec.rpc).auth_services` "overrides the service-level default for this method". **This is the
  right lever for `DeleteTopic` and it already exists — no new mechanism is needed.**
- What tacplus's auth model does **not** track is resource ownership. `auth_services` is an
  identity grant (spec.proto:839, "CROSS-TENANT DOCTRINE. auth_services is an identity grant, not
  a namespace…"), so "this caller owns this consumer group" and "this caller owns this topic" are
  not expressible in the proto at all. Those have to be enforced in the handler, against the
  scheme that creates the resource — which for consumer groups is `boundConsumerGroup`.

**The blocker on the proto half:** `eventbus.proto` lives in the `platform/proto` repo
(`tacplus.yaml`: `src: https://gitlab.com/…/platform/proto.git`), a different git repo from this
one, and this task is scoped to `platform/services/eventbus`. So the `(spec.rpc).auth_services`
narrowing is **written up here as a follow-up (§6), not applied.** Everything below is enforced in
the handler, which is the layer that stays correct whatever the proto says.

Nothing here re-implements generated plumbing: the caller still arrives as the tacplus-built
`Caller` from the verified token, and the Authorizer still runs first. The handler only adds the
ownership question tacplus cannot ask.

## 3. What changed

**`SubscribeHandler`** — two helpers next to `boundConsumerGroup`, reusing its scheme rather than
parsing it a second way:

- `ownsConsumerGroup(callerSub, groupId)` — the inverse of `boundConsumerGroup`: a prefix match on
  `callerSub + "|"`. A group id with no `|` (an anonymous subscribe's random id, or one of
  eventbus's own internal groups) belongs to nobody.
- `ownedConsumerGroup(callerSub, requested)` — resolves what an admin caller may act on, or `""`
  for "may not". An admin caller names a group the way a subscriber does, with the plain name, and
  it is bound through `boundConsumerGroup`; an already-bound id (the form `ListConsumerGroups`
  reports) is accepted when it is the caller's own; an id bound to some other subject is refused
  rather than re-bound into a group that exists for nobody.

**`EventBusHandler`**:

- `adminOwnedGroup(caller, requested)` — the gate: resolves through `ownedConsumerGroup` or throws
  `PERMISSION_DENIED`. Its message does not say whether the group exists.
- `deleteConsumerGroup` / `resetConsumerGroupOffsets` — call the gate and act on the **resolved**
  group id, not `request.getGroupId()`. The gate runs *before* `checkReady()`, so a caller that
  owns nothing does not learn the broker's state either.
- `listConsumerGroups` — filters to groups the caller owns.
- `deleteTopic` — `PERMISSION_DENIED` for every caller (§5).

## 4. Decision: `ListConsumerGroups` is scoped, not entitled

Chose **scope the results** over gating a fleet-wide list behind an entitlement.

There is no fleet-wide caller to gate: a repo-wide grep for `DeleteTopic`/`deleteTopic` and for an
eventbus admin client outside this repo returns nothing — no service in the fleet calls the Admin
service at all today. Building an entitlement for a caller that does not exist is the abstraction
this repo's rules say not to add, and the gate would have to be `["*"]` anyway until the proto
changes. Meanwhile the disclosure is real and unconditional: the group id *is* the caller's SPIFFE
subject, so an unscoped list is a directory of every subscribing service and what it subscribes as.

Group ids are returned in their bound (`callerSub|group`) form, unchanged — that is the id Kafka
actually holds, and `ownedConsumerGroup` accepts it back verbatim, so list → delete round-trips.

If a fleet-wide operator view is ever genuinely wanted, the right shape is a separate RPC with its
own `(spec.rpc).auth_services` naming that operator, not a flag on this one.

## 5. Decision: `DeleteTopic` is denied to everyone, pending the proto

Chose **deny in the handler** over inventing a topic-ownership check.

- Topic ownership is not in tacplus's auth model (§2), and the nearest available proxy — a topic's
  own per-RPC publish `auth_services`, which eventbus's codegen already reads — is not ownership:
  three of the eight topics declare `PublishToStream: ["*"]`, so "publisher" would mean "anyone"
  for exactly the topics an attacker would pick. Deriving an owner from it would be a new
  mechanism built on a value that does not mean what it would have to mean.
- No caller invokes `DeleteTopic` anywhere in the fleet, so the "small set of services that
  legitimately need to delete topics" is **empty**. Narrowing `auth_services` to the empty set
  loses nothing today, and the handler is the only place this repo can enforce it.
- Topics are created from their `(spec.topic)` declaration. Removing one by RPC, behind the
  declaration's back, has no legitimate lifecycle behind it anyway.

This is deliberately a stand-in for the proto change in §6. When `(spec.rpc).auth_services` names a
real caller for `DeleteTopic`, the handler's blanket denial is what has to be revisited — the
comment on it says so.

## 6. Follow-up, in the `platform/proto` repo (not done here)

On `eventbus.proto`'s `Admin` service, replace the blanket `auth_services: ["*"]` with per-RPC
policy:

```proto
rpc DeleteTopic(DeleteTopicRequest) returns (google.protobuf.Empty) {
  option (spec.rpc) = {auth_services: []};        // or the real operator, when one exists
}
rpc UpdateTopicConfig(UpdateTopicConfigRequest) returns (TopicInfo) {
  option (spec.rpc) = {auth_services: [...]};     // see §7
}
```

Until then the handler is the only enforcement point, and a reader of the proto alone will
conclude — wrongly — that any service may delete any topic.

## 7. Deliberately left alone

- **`UpdateTopicConfig` and `GetConsumerGroupLag` are still `["*"]` and still unscoped.** Both are
  real siblings of what was fixed: `UpdateTopicConfig` can shrink another service's topic
  retention (data loss, slower than `DeleteTopic` but not by much), and `GetConsumerGroupLag`
  answers for any group id, so it still leaks per-group state to a caller that can guess a group
  id — which partly undercuts §4's scoping, since §4 only stops *enumeration*. Both are outside
  this task's stated scope. **They should be the next thing fixed, and the fix is one line each
  against the same gate.**
- **The three copies of the admin logic** (`EventBusServiceImpl`, `EventBusServiceHandlerImpl`,
  both dead; the Low-tier finding). Only `getAdminGrpcImpl()` is registered and only it was
  changed, so the dead copies now differ from the live one in authorization too. They did not get
  in the way; they stay for whoever does that cleanup.
- **`PublishHandler`'s namespace stamping** — reviewed and confirmed intentional
  (`decisions/namespace-fix-corrected-2026-09-02.md`).

## 8. Verification

`JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -o -pl server -am -Dtacplus.skip=true test`

```
[INFO] Tests run: 30, Failures: 0, Errors: 0, Skipped: 0     <- codegen module
[INFO] Tests run: 79, Failures: 0, Errors: 0, Skipped: 0     <- server module
[INFO] BUILD SUCCESS
[INFO] Total time:  04:47 min
```

79 server tests, of which 9 are the new `AdminCallerScopingTest`; the other 70 are the pre-existing
suite, unchanged. Every real-broker suite (`ConsumerGroupBindingTest`, `SubscribeTenantFilterTest`,
`ConsumerGroupStarvationTest`, `KafkaIntegrationTest`, `ReplayIntegrationTest`,
`FleetRecordNamespaceStampTest`) is in that number and green.

### The tests fail without the fix

Verified, not assumed. With the four call sites reverted to their pre-fix bodies (helpers left in
place so it still compiles), `-Dtest=AdminCallerScopingTest`:

```
[ERROR] Tests run: 9, Failures: 5, Errors: 0, Skipped: 0
[ERROR] AdminCallerScopingTest.deleteConsumerGroup_onAnotherServicesBoundGroup_isDenied:83
          Kafka not yet initialized ==> expected: <7> but was: <14>
[ERROR] AdminCallerScopingTest.resetConsumerGroupOffsets_onAnotherServicesBoundGroup_isDenied:90
          Kafka not yet initialized ==> expected: <7> but was: <14>
[ERROR] AdminCallerScopingTest.anUnverifiedCaller_isDenied:98        expected: <7> but was: <14>
[ERROR] AdminCallerScopingTest.anEmptyGroupId_isDenied:107           expected: <7> but was: <14>
[ERROR] AdminCallerScopingTest.deleteTopic_isDeniedToEveryCaller:174 expected: <7> but was: <14>
```

7 is PERMISSION_DENIED, 14 is UNAVAILABLE — i.e. pre-fix, the attacker's call cleared every check
and reached the Kafka client, which is the reported gap, reproduced. The handler was restored
byte-for-byte afterwards and re-verified (the run above).


### The build was already broken before this change

Both baseline runs, on the unmodified tree, failed the same way:

```
Failed to execute goal …:codegen:…:generate-eventbus-rpcs on project eventbus-server:
eventbus topics with no auth_services in the registry:
[tasks.DelegationProbeBusStateService (services.tasks.tasks.DelegationProbeBus.Stream)]
— declare (spec.service_s2s) auth_services on the topic's state service
```

`server` never compiled; only the 30 `codegen` tests ran. The cause is unrelated to this change: a
new topic `tasks.DelegationProbeBus` appeared in the regenerated registry (`registry.json`,
2026-09-13 19:27) whose state service is **c2s** — `auth_model: "c2s"`, `auth_entitled_users:
true`, no `auth_services` — while `TypedRpcGenerator.collectMethodAuth` treats a topic with no S2S
`auth_services` as a hard failure. Two things to decide, neither of them this task's:

1. the `tasks` proto should declare `(spec.service_s2s) auth_services` on
   `DelegationProbeBusStateService` if that topic is meant to be reachable service-to-service; and
2. eventbus's codegen arguably should not fail the build for a topic that is c2s-only — but it is
   also right to refuse to serve a topic with no policy, so this needs a real decision rather than
   a loosened check.

To verify this change at all, the local build artifact
`model/target/generated-sources/model/registry.json` was edited **for the test run only** — the
`eventbus` routing label was removed from the `DelegationProbeBus` topic boundary, which is exactly
the state of the last successful build (the committed `TopicRegistry.java` from 14:19 has eight
topics and no `DelegationProbeBus`) — and restored afterwards. No source file was changed for it;
`just generate` regenerates that artifact. **The numbers above therefore come from a tree whose
generated bindings cover the same eight topics as the last good build.** The `DelegationProbeBus`
breakage is still there and still blocks CI.
