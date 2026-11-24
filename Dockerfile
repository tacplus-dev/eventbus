# EventBus Release Image
# Distroless Java 21 base with no shell.
# The launcher binary reads LAUNCH_CMD and supervises the java process.
#
# Every base is pinned by digest, never a floating tag alone, so a rebuild of
# the same commit ships the same, scanned, base (#38; DockerfilePinnedTest).
# Nightly maintenance moves the digests (tacplus-dev/looper#234).

# Pre-create /var/lib/eventbus with correct ownership for nonroot (65532).
FROM busybox:1.38.0@sha256:a46257e06c82ad85242d0d38077d0a52d9896389ca60a1a59d3463db586e5fa4 AS setup
RUN mkdir -p /var/lib/eventbus/data

FROM gcr.io/distroless/java21-debian13:latest@sha256:e5ddc8b22604bf842e43e0e3cc6694da4c3440714749121f024e2ee1d1808539

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Jody Scott <jody@thescottsweb.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="tacplus.dev" \
      org.opencontainers.image.title="EventBus" \
      org.opencontainers.image.description="Event bus service with embedded Kafka broker"

COPY --from=setup --chown=65532:65532 /var/lib/eventbus /var/lib/eventbus
COPY server/target/event-bus.jar /app/event-bus.jar

ARG TARGETARCH
COPY launcher-linux-${TARGETARCH} /usr/local/bin/launcher

USER nonroot

ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -Djava.awt.headless=true -jar /app/event-bus.jar"
ENV HEALTH_PORT=8443

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["launcher"]
