# EventBus Release Image
# Layers the event-bus jar and eventbus binary onto a JRE base.

FROM eclipse-temurin:21-jre-alpine

ARG BUILD_DATE
ARG VCS_REF
ARG VERSION

LABEL org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.authors="Second Star Cavalier LLC <cloudctlplane@secondstarcavalier.com>" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="Second Star Cavalier LLC" \
      org.opencontainers.image.title="EventBus" \
      org.opencontainers.image.description="Event bus service with embedded Kafka broker"

ARG TARGETARCH
RUN wget -qO- "https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_${TARGETARCH}.tar.gz" \
      | tar -xz -C /usr/local/bin grpcurl \
    && addgroup -S eventbus && adduser -S eventbus -G eventbus

COPY server/target/event-bus.jar /app/event-bus.jar
COPY eventbus /usr/local/bin/eventbus
RUN chmod +x /usr/local/bin/eventbus \
    && mkdir -p /var/lib/eventbus/data \
    && chown -R eventbus:eventbus /var/lib/eventbus

USER eventbus

VOLUME /var/lib/eventbus/data

ENTRYPOINT ["eventbus"]
