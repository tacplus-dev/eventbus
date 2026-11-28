# EventBus Release Image
# Distroless Java 21 base with no shell.
# The launcher binary reads LAUNCH_CMD and supervises the java process.

# Every FROM is pinned by digest (#31), so the image builds from the same bases until a
# commit moves a digest: refresh a base by moving its digest when it publishes fixes.
# security-baseline.json names the base images by these digests: regenerate it
# (looper-scan baseline) in the commit that moves it.

# The launcher is a pinned tacplus-dev/launcher release, verified by its sha256
# (the release's checksums.txt) before it goes into the image (#14).
ARG LAUNCHER_VERSION=v0.1.0
ARG LAUNCHER_SHA256_AMD64=903693ab56879b5a5b54779c3e20efa3ddd5f1269fb0c2274882befb7df69e0f
ARG LAUNCHER_SHA256_ARM64=f07933da11e08812afa87d814ecbd187087f20c2af7f62d0c1d356754643ec16

FROM alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS launcher
ARG TARGETARCH
ARG LAUNCHER_VERSION
ARG LAUNCHER_SHA256_AMD64
ARG LAUNCHER_SHA256_ARM64
RUN case "${TARGETARCH}" in \
      amd64) sum="${LAUNCHER_SHA256_AMD64}" ;; \
      arm64) sum="${LAUNCHER_SHA256_ARM64}" ;; \
      *) echo "no launcher pinned for ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && wget -q -O /launcher "https://github.com/tacplus-dev/launcher/releases/download/${LAUNCHER_VERSION}/launcher-linux-${TARGETARCH}" \
 && echo "${sum}  /launcher" | sha256sum -c - \
 && chmod 0755 /launcher

# Pre-create /var/lib/eventbus with correct ownership for nonroot (65532).
FROM busybox:1.38.0@sha256:a46257e06c82ad85242d0d38077d0a52d9896389ca60a1a59d3463db586e5fa4 AS setup
RUN mkdir -p /var/lib/eventbus/data

FROM gcr.io/distroless/java21-debian13@sha256:132eb554519304beb13de16013176efb8aa67bb7bf9e2d1ca340e97e744c3cb3

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

COPY --from=launcher /launcher /usr/local/bin/launcher

USER nonroot

ENV LAUNCH_CMD="java -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -Djava.awt.headless=true -jar /app/event-bus.jar"
ENV HEALTH_PORT=8443

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["launcher"]
