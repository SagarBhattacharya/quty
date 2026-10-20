FROM rust:1.99-slim-bookworm AS builder

WORKDIR /usr/src/quty

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    clang \
    llvm \
    && rm -rf /var/lib/apt/lists/*

# Copy full source tree
COPY . .

# Use BuildKit cache mounts to retain cargo caches across rebuilds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/quty/target \
    cargo build --release -p quty-server && \
    cp /usr/src/quty/target/release/quty-server /usr/local/bin/quty-server

# ------------------------------------------------------------------------------
# Minimal Runtime
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 10001 quty && \
    useradd -u 10001 -g quty -s /bin/false -m quty

WORKDIR /home/quty
COPY --from=builder --chown=quty:quty /usr/local/bin/quty-server /usr/local/bin/quty-server

USER quty:quty
EXPOSE 6379

ENTRYPOINT ["quty-server"]
CMD ["--bind", "0.0.0.0:6379", "--shards", "64", "--max-memory", "512MB"]