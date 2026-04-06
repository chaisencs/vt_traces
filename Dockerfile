FROM rust:1.73.0-bookworm AS builder

WORKDIR /app

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p vtapi -p vtbench

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tzdata \
    && groupadd --system vt \
    && useradd --system --gid vt --home-dir /app --shell /usr/sbin/nologin vt \
    && mkdir -p /app /var/lib/rust-victoria-trace \
    && chown -R vt:vt /app /var/lib/rust-victoria-trace \
    && rm -rf /var/lib/apt/lists/*

ENV RUST_LOG=warn
ENV VT_BIND_ADDR=0.0.0.0:13000
ENV VT_STORAGE_MODE=disk
ENV VT_STORAGE_PATH=/var/lib/rust-victoria-trace
ENV VT_STORAGE_SYNC_POLICY=data
ENV VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608
ENV VT_MAX_REQUEST_BODY_BYTES=8388608
ENV VT_API_CONCURRENCY_LIMIT=1024

WORKDIR /app

COPY --from=builder --chown=vt:vt /app/target/release/vtapi /usr/local/bin/vtapi
COPY --from=builder --chown=vt:vt /app/target/release/vtbench /usr/local/bin/vtbench
COPY --chown=vt:vt README.md /app/README.md
COPY --chown=vt:vt docs/2026-04-06-otlp-ingest-performance-report.md /app/2026-04-06-otlp-ingest-performance-report.md
COPY --chown=vt:vt docs/production-release-guide.md /app/production-release-guide.md

VOLUME ["/var/lib/rust-victoria-trace"]
EXPOSE 13000
USER vt:vt

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl --fail --silent http://127.0.0.1:13000/healthz >/dev/null || exit 1

CMD ["vtapi"]
