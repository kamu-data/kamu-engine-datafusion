# TODO: Consider using https://github.com/GoogleContainerTools/distroless
FROM docker.io/library/alpine:3.19
ARG target_arch

# Tini
RUN apk add --no-cache tini

# Engine
COPY target/${target_arch}/release/kamu-engine-datafusion /opt/engine/bin/kamu-engine-datafusion

ENV RUST_BACKTRACE=1
ENV RUST_LOG=debug
EXPOSE 2884/tcp
ENTRYPOINT ["/sbin/tini", "--", "/opt/engine/bin/kamu-engine-datafusion"]
