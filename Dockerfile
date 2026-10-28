# ---- build stage ----
FROM rust:1.82-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

# ---- runtime stage ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/crabcache /usr/local/bin/crabcache
EXPOSE 6379
VOLUME ["/data"]
ENTRYPOINT ["crabcache"]
CMD ["--bind", "0.0.0.0", "--port", "6379", "--dir", "/data", "--appendonly"]