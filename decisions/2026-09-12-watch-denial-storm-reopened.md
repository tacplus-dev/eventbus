# Watch-denial storm, reopened: the cause is `store: true` in the tacplus generator

Date: 2026-09-12
Status: **Phase 1 only — investigation complete, no fix applied.** The correct fix is a tacplus
generator change; per the standing rule this stops here and is filed as an ask.

Supersedes the "no bug found, likely stale image" conclusion in
`2026-09-12-missing-state-read-grants.md` (eventbus@ca9e89f). That conclusion was wrong, and this
document says exactly where.

---

## Summary

eventbus's proto declares `option (spec.service) = {store: true}` (`platform/proto/services/eventbus.proto:13`)
because it backs its own topic state. The tacplus commit the fleet is pinned to treats **any**
`store: true` service as *the* backing store and grants it RW to **every state root in the build**.
That inflates eventbus's generated state-client catalog from 2 roots to 42, and because a generated
Java `StateClient` starts its Watch streams from `setChannel()`, all 42 are dialled at boot. 75 Watch
streams open; 71 are denied by policy and retry forever at a 30 s backoff cap.

`710 denials / 5 min ÷ (300 s / ~32 s per retry) = 71`. The arithmetic closes exactly.

The bug is in **tacplus**. Not eventbus application code, not the OPA/state policy — the denials are
correct enforcement of a correct policy against a client that should never have been generated,
let alone dialled.

**A fix for this already exists, uncommitted, in the tacplus working tree** (see §5). Nothing has
been committed and no build carries it.

---

## 1. Where eventbus uses state clients

eventbus application code touches its own two roots and nothing else:

- `server/src/main/java/.../EventBusHandler.java:118` — `sm.getStateClients().services().eventbusConfigState().stateStub()`
- `server/src/main/java/.../EventBusHandler.java:1011` — `...services().eventbusState().stateStub()`
- `server/src/main/java/.../EventBusHandler.java:1040` — `...services().eventbusState().stateStub().setNodes(...)`

There is no generic "watch everything" loop, no health-check or metrics sweep over all state
clients, and no `.watch(` call anywhere in `server/src/main`. The only `Watch*` identifiers in
hand-written code are `WatchTraceHandler` (`grpc/WatchTraceHandler.java`), which serves eventbus's
own `WatchTrace` RPC and has nothing to do with state.

**The prior investigation was right that eventbus app code only uses two roots. It was wrong to
conclude from that that the other 40 wrappers are "inert, never dialed."** Nothing in application
code has to touch them — the generated runtime dials them unconditionally.

## 2. It is generated code, and here is the chain

The live log line names its own emitter:

```
Sep 13, 2026 5:01:44 AM com.gitlab...eventbus.model.s2sm.internal.state.services.letsencrypt.letsencryptstate.StateClient$1 onError
WARNING: WatchCertData stream failed; retrying: PERMISSION_DENIED: service "eventbus" is not authorized for READ on "letsencrypt.LetsEncryptState.cert_data"
```

`java.util.logging` two-line format, emitted from an anonymous `StreamObserver` inside a *generated*
`StateClient`. The chain, all generated:

1. `model/.../s2sm/ServiceManager.java:134-135` — the ServiceManager constructor does
   `new StateClients()` then `stateClients.setChannel(stateStoreConn.conn(), ...)`.
2. `model/.../s2sm/internal/state/StateClients.java` — root aggregator fans `setChannel` out to the
   `services`, `sys` and `vservices` group aggregators.
3. `model/.../s2sm/internal/state/services/StateClients.java:32-70` — the group aggregator declares
   **39 state clients as eagerly-initialised final fields** (`= new ...StateClient()`), e.g.
   `letsEncryptState` at line 44, and `setChannel`s every one of them. Template:
   `tacplus/pkg/templates/java/types/group_state_clients.java.tmpl:11-21`.
4. `model/.../services/letsencrypt/letsencryptstate/StateClient.java:85` — `setChannel()` ends with
   `startWatches()`. Template: `tacplus/pkg/templates/java/types/statewrapper.java.tmpl:92-104`.
5. `.../StateClient.java:387` — `onError` logs the warning and calls `scheduleWatchRestart(...)`,
   which reschedules on a per-client dedicated single daemon thread
   (`statewrapper.java.tmpl:62`) with exponential backoff capped at `MAX_WATCH_BACKOFF_MILLIS = 30000L`
   (`statewrapper.java.tmpl:51`) plus 20 % jitter → observed steady-state retry period of ~30–32 s.

Counting the `startWatch*(0)` calls in every generated `startWatches()` across the regenerated tree
gives **75** auto-started watches. Live logs show **71** distinct denied `root.field` pairs and
**710** denials per 5 minutes. The 4 that succeed are eventbus's own two roots, the shared
`grpcconfig.ServiceConfigState`, and one further kernel root.

Eager watch-on-construct is *deliberate and cross-language* — the Go template spawns
`go c.watch<Field>Loop()` (`templates/go/types/state_wrapper.go.tmpl:143-149`) and the Rust one
`tokio::spawn`s equivalents (`templates/rust/types/statewrapper.rs.tmpl:152-168`). The state wrapper
is designed as a self-populating cache. **That design is not the bug.** The bug is which wrappers
exist at all.

## 3. Why eventbus has 42 wrappers and comparable services have 2–3

Comparison across the same build:

| service | language | state clients generated |
|---|---|---|
| operator | Go | **2** (`model/s2sm/state/services/states.go:20-31` — `service_health`, `operator_config_state`) |
| stratum | Java | **3** |
| kernel | Java | **42** (correct — kernel *is* the state store) |
| **eventbus** | **Java** | **42** |

So the generator does normally scope the catalog. eventbus is getting kernel's catalog.

The selection filter is `Config.AccessModeForState` (`tacplus/pkg/internal/tactype/config.go:205-215`),
consulted per state root by `generateJavaStateClientWrappers`
(`tacplus/pkg/internal/java/types/statewrapper_gen.go:181-184`). It returns `shouldGenerate=true`
only for roots present in `config.States`. So `config.States` must hold all 42 — and it does,
because of this, at the pinned generator commit `c33d7d56`
(`tacplus/pkg/internal/proto/configholder.go:1424-1430`):

```go
isStore := config.AllStates
for _, fd := range allFiles {
    if fd.Package == config.SelfPackage && fd.ServiceStore {
        isStore = true
        break
    }
}
```

…followed by (`configholder.go:1499-1524`):

```go
// 3. Backing store (proto store: true): RW to ALL state partitions in the build
if isStore {
    for key, info := range stateIndex { /* append every root as RW */ }
}
```

`ServiceStore` is set from `option (spec.service) = {store: true}`. Two services declare it:

- `platform/proto/services/kernel.proto:34` — `{store: true, config_store: true}`
- `platform/proto/services/eventbus.proto:13` — `{store: true}`

eventbus declares `store: true` because it genuinely backs its own topic state. The generator reads
that as "backs *all* state", and hands it RW to all 42 roots.

**Reproduced, not inferred.** Regenerating eventbus at the pinned generator (`c33d7d56`) via
`scotty/backend/build.sh --generate-only eventbus` produces 39/1/2 clients and 75 auto-started
watches — identical to the deployed build. The generation log shows `ResolveStatesFromProtos`
resolving only 2 states as "own, RW"; the other 40 are added silently by the unlogged `isStore`
branch.

## 4. Is there a grant manifest that is out of sync with policy? No.

Per-root readership is declared in the proto (`(spec.service_state)` on each root, plus
`(spec.state_imports)` / field-level `(spec.state_import)` on readers) and compiled by tacplus into
the store's `GRAPH_POLICIES`; `platform/services/kernel/codegen/.../InfraStatePolicies.java` patches
the one root (`grpcconfig.ServiceConfigState`, `ro: ["*"]`) that lives in an infra proto the computed
pass skips. eventbus declares no `state_import` for any of the 71 denied roots, and has no business
reading `letsencrypt.LetsEncryptState.cert_data`, `scotty.ScottyState.humans` or
`work.WorkState.permits`.

**So the denials are correct and the policy is correct.** The fix is emphatically *not* "grant
eventbus these roots" — that would hand the event bus read access to the entire fleet's state to
silence a log line. The fix is to stop generating (and dialling) clients eventbus was never
entitled to.

## 5. The poll stall is a separate bug. Scotty's shared-thread hypothesis is disproven.

Three independent lines of evidence:

**No shared executor.** Each generated `StateClient` owns a *private* single-thread daemon scheduler
for its retries (`statewrapper.java.tmpl:60-64`, `"<Root>-state-watch-retry"`). The Kafka poll loops
run on `Executors.newVirtualThreadPerTaskExecutor()` (`grpc/SubscribeHandler.java:116-117`,
submitted at `:232`). The assignment watchdog has its own thread again
(`kafka/KafkaSubscriber.java:113-115`). Nothing is shared between the retry loops and the poll loops.

**No CPU starvation.** `kubectl top pod eventbus0-65fddd8df4-kjz4t` → **28m CPU**, 971Mi. Nodes at
7 %/15 %/27 %. A process using 0.028 of a core is not losing a scheduling fight to 2.3 stream
re-establishments per second.

**The stalls have their own, sufficient cause.** They cluster at startup, not with the (continuous,
evenly-spaced) denial retries. The pod started 04:38:10; ~25 subscribe sessions all called
`runLoop` between 04:38:39.3 and 04:38:40.0 — several on the *same* topic and group
(4× `services.messaging.messaging.MessageBus.Stream`). Each join rebalances its group. At
04:39:50.02x — one `max.poll.interval.ms` later — **15 sessions reported STALLED within 6 ms of each
other**, then a decaying tail (04:40:50, 04:41:20, 04:41:50 ×4, …). That is a thundering-herd
rebalance against a single-node embedded KRaft broker in the same JVM
(`Config validated: role=KAFKA_ROLE_COMBINED, controller_count=1, broker_count=1`), and
`KafkaSubscriber.java:414-417` already documents the mechanism in its own comment: *"poll(timeout)
does not bound the JoinGroup/SyncGroup handshake."* A consumer parked in a rebalance is not polling,
so the watchdog at `:315` correctly reports a stall — **the STALLED line is a symptom of the
rebalance, not an independent fault, and not caused by the watch storm.**

That said, the two are worth fixing in the same pass for one reason: the denial storm costs eventbus
~42 extra threads and 75 stream re-establishments every 30 s of pure waste, and removing it is free.
It is not, on this evidence, what stalls the poll loops.

**Open question for a human (default taken: leave it alone).** The poll-stall / continuous-rebalance
behaviour is a real fleet-wide event-delivery risk and is *not* addressed by anything here. It needs
its own investigation — likely "why do ~25 sessions subscribe to the same few topics and groups at
once, and should joins be staggered or share a consumer." That is a design call, out of scope for
this ticket, and no code was changed for it.

---

## Minimal correct fix

**In tacplus** (`pkg/internal/proto/configholder.go`): select the backing store by the *canonical*
marker, not by any `store: true`. `option (spec.service) = {config_store: true}` already exists and
is declared by exactly one service (kernel). Resolve the primary store from `config_store: true`,
falling back to the sole `store: true` service only when there is exactly one, and error when the
set is ambiguous. Then `isStore` is false for eventbus, `config.States` holds its 2 own roots plus
its injected config states, and the generated catalog and the auto-started watches shrink to what
policy actually permits. No template change is needed; eager watch-on-construct stays.

**This fix is already written and sitting uncommitted in the tacplus working tree** — 64 insertions /
47 deletions in `pkg/internal/proto/configholder.go`, introducing `resolvePrimaryStore(allFiles)`,
plus an untracked `pkg/internal/proto/zz_tmp_platform_probe_test.go`. Its comment names this exact
incident. Someone is mid-fix. Nothing is committed, so no build carries it, and the pinned generator
`c33d7d56` (`scotty/backend/build.sh:181`) predates it in any case — **the pin must move forward
after the fix lands, or eventbus keeps regenerating with the bug.**

## Why nothing was changed here

Every eventbus-side option is a hack or a privilege grab:

- Granting eventbus READ on 39 roots — hands the event bus the fleet's state to quiet a log line.
- Closing unused `StateClient`s at boot from `Bootstrap` — hand-rolling around the generator,
  which house rules forbid; and the wrappers would still be generated and still reachable.
- Narrowing eventbus's proto set — breaks topic discovery, which legitimately needs every service's
  protos.
- Dropping `store: true` from `eventbus.proto` — eventbus really does back its own state, and
  `platform/proto` is outside this service's write scope anyway.

Filed as an ask at `~/workspace/loopers/_retired/tacplus/inbox.md`.

## Tree state

`model/target/generated-sources` was regenerated from scratch during this investigation (via
`scotty/backend/build.sh --generate-only eventbus`, at the pinned generator, merged
platform + scotty protos). It is gitignored build output and is byte-equivalent in shape to what was
there before — 39/1/2 clients, 75 watches. `mvn -o -pl server -am test`: **87 tests, 0 failures,
0 errors** (codegen 30, server 57). No tracked file in this repo was modified except this document.
