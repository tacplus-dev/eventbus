# EventBus Demo Readiness Progress

## 2026-06-17

- Updated EventBus source to match the current tacplus-generated Java API:
  - `CollectionRuntime` import moved from `model.libs.collections` to
    `model.libs.collection`.
  - Admin config mutation handlers now implement generated
    `updateTopicConfig(UpdateTopicConfigRequest)` instead of stale
    `alterTopicConfig(AlterTopicConfigRequest)`.
  - `WatchTraceHandler` now implements the generated
    `TraceSession.TraceSessionHandler` contract via `subscribeTrace(...)`.
  - Raw fallback Admin gRPC health handler now accepts `google.protobuf.Empty`
    for `GetClusterHealth`, matching the generated proto.
- Regenerated EventBus model output with tacplus `57e6130` using:
  `tacplus build --src /Users/jody/workspace/cavalier/proto --tag latest`.
- Verified EventBus builds against that regenerated output with:
  `JAVA_HOME=$(/usr/libexec/java_home -v 21) mvn -pl server -am
  -Dtacplus.skip=true -Dmaven.test.skip=true package`.

## Remaining

- Wait for tacplus `57e6130` CI and wave review before any tacplus tag/fanout.
- Rebuild downstream images/services from normal source once tacplus is green
  and tagged.
