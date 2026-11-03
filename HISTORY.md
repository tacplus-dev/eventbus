# History: eventbus

Append-only: add entries at the end, never edit or remove one (CI enforces it). One line each:
`- <YYYY-MM-DD> <who: looper|agent|jody> <kind: issue|decision|release|deploy> <one line> (<links>)`

- 2026-10-27 looper issue Jetty 11.0.25 (via Javalin 6.7.0) has HIGH advisories with no public Jetty 11 fix: move to Javalin 7 / Jetty 12 (#9, 325f31b)
- 2026-10-27 looper issue No gates: the justfile has no `gates` recipe, so `just gates` fails on main (#10, e8cd5e2)
- 2026-10-27 looper issue Base image follows Debian 12: move to gcr.io/distroless/java21-debian13 (#11, 41729c6)
- 2026-10-28 looper issue guava 33.5.0-android (via grpc-protobuf) has a HIGH advisory (GHSA-xxph-c9ww-hj94): pin 33.7.2-android (#12, ea38520)
- 2026-10-28 looper issue snappy-java 1.1.10.5 (via kafka-clients 3.9.2) has 2 HIGH + 1 MODERATE advisories: pin 1.1.10.12 (#13, 8453887)
- 2026-10-29 looper issue Gates fail on main: stratum now needs tacplus 2.5; move the tacplus floor from 2.4 to 2.5 (#15, 5d7c330)
- 2026-10-29 looper issue plexus-utils 3.5.1 (via maven-plugin-api 3.9.9 in codegen) has a HIGH advisory (GHSA-6fmv-xxpf-w3cw): bump maven-plugin-api to 3.9.16 (#16, 4881d2f)
- 2026-10-31 looper issue server/ and codegen/ justfiles pin the retired tacplus 2.0.2 image (floor is 2.5): the recipes fail with 'manifest unknown' (#17, 1bde5cd)
- 2026-11-02 looper issue maven-shade-plugin 3.6.1 runs on plexus-utils 3.5.1 (GHSA-6fmv-xxpf-w3cw, HIGH): override it to 3.6.1 (#18, 0d0bf9b)
- 2026-11-03 looper issue PublishHandler: no test that the inbound trace_id reaches the Kafka record, and untraced messages inherit an earlier trace_id (#19, 8d4b741)
- 2026-11-03 looper issue exec-maven-plugin 3.5.0, build-helper-maven-plugin 3.6.0 and maven-antrun-plugin 3.1.0 run on plexus-utils with GHSA-6fmv-xxpf-w3cw (and commons-io with GHSA-78wr-2p64-hpwj), both HIGH: bump them and override antrun (#20, bcbe0b0)
