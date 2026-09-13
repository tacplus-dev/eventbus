# eventbus "71 missing state-read grants" — not reproducible from source; no code change made

**Date:** 2026-09-12
**Trigger:** scotty looper iter3306 relay (assertion-11 investigation), item 3: "Eventbus has 71
missing state-read grants — `PERMISSION_DENIED` on `kernel.Cluster.watch_health`,
`mailbox.MailboxConfigState.config`, `work.WorkState.tasks`, etc., ~142 log lines/min ... it's a
spec/codegen mismatch in eventbus's proto that needs fixing + regeneration."

**Verdict: neither (a) a missing annotation nor (b) a stale regeneration. The source tree is
self-consistent, and eventbus issues none of the calls that were reported denied. No proto edit and
no code change is justified by anything visible in the repo.** The most likely explanation for the
live symptom is the stale-image problem scotty reported in the *same* relay.

---

## How the grant actually works (confirmed, not assumed)

scotty's mechanism description was right in outline, and the exact shape is:

1. A reader declares intent in its own proto, file-level:
   `option (spec.state_imports) = {source: "<pkg>.<StateRoot>"};`
   (`platform/proto/services/sentinel.proto:14`, `letsencrypt.proto:12`, `gateway.proto:14-15`,
   `oauth.proto:55` are the four services that do this today. The field-level
   `[(spec.state_import) = "pkg.State.field"]` form is the preferred spelling per
   `tacplus/pkg/proto/sys/spec.proto:78-81`; nobody in this proto set uses it.)
2. tacplus resolves each state root's caller set from **its owning package plus its declared
   importers** — see the error text in
   `tacplus/pkg/internal/proto/validate_state_service_auth.go:76-84`.
3. The result is emitted twice:
   - client/server RPC auth: `<Root>StateService.registerMethodAuth(...)` in each service's
     generated tree (e.g. kernel's `ClusterStateService.java:8222` →
     `List.of("kernel", "sentinel")` — owner + the one importer).
   - **the store-side policy table**, which is the artifact that produces the reported log line:
     `platform/services/kernel/server/target/generated-sources/tacplus-backend/.../SecurityAuthenticator.java`,
     `GRAPH_POLICIES` (lines ~215-253), consumed by `checkAccess(graph, boundary, op)` at line 283.

`checkAccess`'s denial log is, verbatim:

```
logger.warn("[authz] REJECTED {}.{} {} — caller {} not in policy", graph, boundary, op, caller);
```

which is exactly the `kernel.Cluster.watch_health` / `mailbox.MailboxConfigState.config` shape
scotty quoted. That line is emitted by the **kernel** process (the state-store backend that hosts
every service's state root), not by eventbus; the caller is a field *inside* the line.

## What eventbus actually calls

Every state call eventbus can make goes through `sm.getStateClients()`. Exhaustive grep of both the
generated tree and the hand-written server:

```
$ grep -rn --include='*.java' -e 'stateClients\.' -e 'getStateClients()\.' \
    model/target/generated-sources server/src codegen
ServiceManager.java:135   stateClients.setChannel(...)
ServiceManager.java:387   stateClients.close()
ServiceManager.java:499   stateClients.services().eventbusConfigState().config()
ServiceManager.java:793   stateClients.close()
EventBusHandler.java:110  sm.getStateClients().services().eventbusConfigState().stateStub()
EventBusHandler.java:978  sm.getStateClients().services().eventbusState().stateStub()
EventBusHandler.java:1007 sm.getStateClients().services().eventbusState().stateStub().setNodes(...)
```

Two state roots, both eventbus's own. tacplus generates **42** `StateClient` wrappers into
eventbus's tree (one per state root in the whole proto set, including `kernel/cluster`,
`mailbox/mailboxconfigstate` and `work/workstate`), but they are inert constructor-allocated
wrappers — nothing dials them. `grep` for `.cluster()`, `.workState()`, `.mailboxConfigState()`,
`.letsEncryptState()` anywhere outside the aggregator returns nothing.

The only *cross-service* state traffic eventbus generates is chassis-level, and all of it is
already permitted:

| call | graph | policy | result |
|---|---|---|---|
| `healthStub::setInstances` (HealthReporter, `ServiceManager.java:673-676`) | `kernel.ServiceHealth` | `rw = ["*"]` | allowed |
| per-service config watch | `grpcconfig.ServiceConfigState` | `ro = ["*"]` | allowed |
| Authorizer verifying-key watch | `JwkVerifyingKeysViewServer` | calls `checkAccess(NO_POLICY_GRAPH, ...)` → early return | allowed |
| token/cert mint | `kernel.Mint` (S2S, not state; declared `(spec.grpc_s2s_client)` at `eventbus.proto:14`) | in `NO_AUTH_METHODS` | allowed |

## Ruling out (b), stale regeneration

Regenerated eventbus's model from proto and compared the emitted policy table. Fresh output
(`model/target/generated-sources/model/proto-annotated/codegen-config.json`, 42
`securityPolicies`) contains for eventbus exactly:

```
eventbus.EventbusState        rw=[service:eventbus]  ro=[service:eventbus]
eventbus.EventbusConfigState  rw=[service:eventbus]  ro=[service:eventbus]
kernel.ServiceHealth          rw=[*]                 ro=[service:kernel]
kernel.KernelDiskState        rw=[service:kernel]    ro=[*]
```

— identical to what kernel's already-generated `SecurityAuthenticator.java:248-249,228,237` carries
today. Regeneration changes nothing. The generated tree was not stale.

## Ruling out (a), missing annotation

Adding `(spec.state_imports)` entries for `kernel.Cluster`, `mailbox.MailboxConfigState`,
`work.WorkState` and ~37 further roots would grant eventbus read access to state **no line of
eventbus reads**. That is a security widening with no caller, invented to match a log line rather
than a call site. `git log -S 'state_imports' -- services/eventbus.proto` in the proto repo is
empty: eventbus has never declared one, so there is no regression to restore either.

## Most likely explanation for the live symptom

scotty's own relay, two items above this one, reports that `eventbus0` and `stratum-0` were **stale
pods** — every schematic service pins the mutable `:local` tag, so a re-push changes no pod spec and
no rollout triggers. A `PERMISSION_DENIED` storm against graphs the current source never touches is
exactly what an old image would produce. Nothing in the repo can confirm or refute that; it needs
the caller/version correlated in-cluster.

## What would settle it (needs cluster access — out of this task's remit)

1. Confirm the denial lines really carry `caller eventbus` (they are emitted by **kernel**, so
   whichever pod's logs they were read from, the caller field is the attribution).
2. Correlate the running `eventbus0` image digest against a build of current `main`.
3. If a current-source build still produces the denials, capture a full denied-graph list with
   caller — the 3 named examples plus a count of 71 is not enough to locate a call site, and I could
   not derive one from source.

## Separate defect found while verifying (not fixed here)

`just generate` in this repo — `tacplus build --tag latest` — **fails**:

```
Error: residency annotation errors:
  mailbox.MailboxEntry: (spec.record) declares residency/mutability but no (spec.trait) hosts a
  record query plane, so nothing would read it ... generate the merged application+platform build
  instead, e.g. `tacplus build --src <platform proto dir> --src <application proto dir>`
```

The platform proto source alone no longer satisfies the record-residency check; the trait host lives
in the application proto set (`scotty/backend/proto`, which supplies `scotty`/`work`/`weather` —
present in the generated tree, absent from `platform/proto`). The working invocation is:

```
tacplus build --src <schematic>/platform/proto --src <scotty>/backend/proto
```

This is *not* a tacplus gap — the validator says what to do and the merged build succeeds — but the
repo's own regeneration command is broken, and a service nobody can regenerate is exactly how a
deployed image drifts from source. Encoding an absolute path to a sibling repo in `justfile` would
be a hack, so I made no change; how the merged src is expressed is a decision for the platform
owner. Same one-line `justfile` exists verbatim in stratum and (presumably) every other service;
stratum's build happens to pass because stratum's own proto declares `(spec.trait) = {name:
"query"}`.

## Open questions, and the defaults I proceeded on

- **Q:** Were the denial lines read from `eventbus0`'s logs or kernel's, and did they carry
  `caller eventbus`? **Default taken:** assumed they did, and still found no matching call site.
- **Q:** Is the deployed `eventbus0` image built from current `main`? **Default taken:** assumed
  not, on scotty's own stale-pod finding; this is the explanation I'd test first.
- **Q:** What is the intended source expression for the merged proto build? **Default taken:** left
  `justfile` untouched; used the explicit two-`--src` form locally to restore the generated tree.

## Verification

Regenerated the model (merged `--src`, as above) and ran the suite against it —
`mvn -o -pl server -am test`:

```
[INFO] Results:
[INFO] Tests run: 52, Failures: 0, Errors: 0, Skipped: 0
[INFO] Reactor Summary for Event Bus Parent 1.2.0-SNAPSHOT:
[INFO] Event Bus Parent ................................... SUCCESS [  0.001 s]
[INFO] EventBus Model ..................................... SUCCESS [ 32.357 s]
[INFO] Event Bus Codegen .................................. SUCCESS [  1.401 s]
[INFO] Event Bus Server ................................... SUCCESS [04:16 min]
[INFO] BUILD SUCCESS
```

52/52 pass, no pre-existing failures. The model module compiling clean from the freshly regenerated
sources is also the check that the regeneration reproduced the previous tree: the 42 generated
`StateClient` paths match the prior tree exactly (`diff` of the sorted path lists is empty).

**Working tree left with one file added:** this document. No source, proto, or generated file was
edited.
