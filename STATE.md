# State: eventbus

Overwritten to stay current (loopers and agents). Last updated: 2026-10-29 by looper (#15).

- **Version / line:** 1.2.0-SNAPSHOT on main (no release recorded); tacplus floor 2.5 (#15)
- **Works:** generates with tacplus 2.5 (2.5.1) and passes `just gates`. Javalin 7.2.4 / Jetty 12.1.14 (#9): there is no Jetty 11 in the resolved tree. The release image is on `gcr.io/distroless/java21-debian13` (#11). It builds for linux/arm64 and linux/amd64 and starts on both: the plaintext health probe (8444) answers, and the zstd-jni, snappy-java and lz4 natives load. Without a kernel it stays NOT_SERVING and TLS 8443 is not up.
- **Released / deployed:** <where each version runs>
- **Known gaps:** codegen's build-time plexus-utils 3.5.1 has an advisory (see PROPOSALS.md); it is the only OSV finding in the resolved tree. guava is pinned to 33.7.2-android (#12) and snappy-java to 1.1.10.12 (#13).
- **Blocked on:** none
