# syntax=docker/dockerfile:1

FROM rust:1.93-bookworm AS builder
WORKDIR /app

# The repository Cargo configuration selects mold for Linux builds. Keep the
# container builder aligned with CI so release builds have the configured
# linker available.
RUN apt-get update \
    && apt-get install -y --no-install-recommends mold \
    && rm -rf /var/lib/apt/lists/*

# The build context is the Lix repository root. The reference server links the
# exact protocol and SlateDB implementation from the same revision.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --package lix-server \
    && cp /app/target/release/lix-server /usr/local/bin/lix-server

FROM debian:bookworm-slim AS runtime
LABEL org.opencontainers.image.title="Lix reference server" \
      org.opencontainers.image.description="Reference implementation of the Lix Server Protocol" \
      org.opencontainers.image.source="https://github.com/opral/lix"
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 lix-server \
    && mkdir -p /var/cache/lix-server \
    && chown lix-server:lix-server /var/cache/lix-server

COPY --from=builder /usr/local/bin/lix-server /usr/local/bin/lix-server

USER lix-server
EXPOSE 8080
CMD ["/usr/local/bin/lix-server"]
