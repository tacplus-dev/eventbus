# Roadmap: eventbus

Owned by Jody; changed only by an agent at Jody's request. Loopers propose in PROPOSALS.md.

## Now
Typed pub/sub on embedded Kafka (KRaft): generated Publish/Subscribe RPCs for every service's annotated topics, plus Admin and TraceSession.
Ported from schematic 2026-10-23. Unreleased; tacplus floor 2.4.
Its role is messaging between services; it is no one's database (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md).

## Next
- Hazelcast 5.5.0 CRITICAL/HIGH advisories: bump to 5.7.0 (#7)
- logback 1.5.29 LOW advisories: bump to 1.5.38 (#8)
- Jetty 11 HIGH advisories: move to Javalin 7 / Jetty 12 (#9)
- Add a `gates` recipe to the justfile (#10)

## Later
- Base image to distroless java21-debian13 (#11), blocked by #10
- Release v0.1.0 and publish its image (#2), blocked by tacplus-dev/looper#12 (needs Jody's decision)
- Port SeaBeam eventbus ea83ea2..1f41acc8 (#3), blocked by #2
- Every topic type that carries a sender has the broker stamp `spiffe_id` (tacplus-dev/roadmap:decisions/tacplus-dev-stable-gate-2026-10-25.md)

## Not doing
- Record fan-out to databases (eventbus to stratum, timedb, graphdb): records are written through Stratum's own API (tacplus-dev/roadmap:decisions/stratum-per-tenant-2026-10-25.md)
