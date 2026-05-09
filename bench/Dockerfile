# Multi-stage build that produces both the rustikv server and the
# redis-compare benchmark client.
FROM rust:1.91-slim AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
RUN cargo build --release --bin rustikv --bin redis-compare

FROM debian:bookworm-slim AS rustikv-runtime
RUN mkdir -p /data
COPY --from=builder /build/target/release/rustikv /usr/local/bin/rustikv
ENTRYPOINT ["/usr/local/bin/rustikv"]

FROM debian:bookworm-slim AS bench-runtime
RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/redis-compare /usr/local/bin/redis-compare
ENTRYPOINT ["/usr/local/bin/redis-compare"]
