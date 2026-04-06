FROM rust:1.73.0-bookworm AS builder

WORKDIR /app

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p vtapi -p vtbench

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=warn
ENV VT_BIND_ADDR=0.0.0.0:13000
ENV VT_STORAGE_MODE=disk
ENV VT_STORAGE_PATH=/var/lib/rust-victoria-trace

WORKDIR /app

COPY --from=builder /app/target/release/vtapi /usr/local/bin/vtapi
COPY --from=builder /app/target/release/vtbench /usr/local/bin/vtbench
COPY README.md /app/README.md
COPY docs/2026-04-06-otlp-ingest-performance-report.md /app/2026-04-06-otlp-ingest-performance-report.md
COPY docs/production-release-guide.md /app/production-release-guide.md

VOLUME ["/var/lib/rust-victoria-trace"]
EXPOSE 13000

CMD ["vtapi"]
