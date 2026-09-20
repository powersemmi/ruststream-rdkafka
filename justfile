set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows each of them.
    cargo clippy --workspace --exclude ruststream-rdkafka-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-rdkafka-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-rdkafka-bench --all-targets --all-features
    cargo check -p ruststream-rdkafka-bench --all-targets
    cargo check --workspace --no-default-features
    # CI's stable leg denies rustdoc warnings, and broken intra-doc links are invisible to
    # every step above; running it here is what keeps a local pass from turning CI red.
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    # This recipe starts the stand, so a gated test that skips itself here is a fault, not a
    # developer without a cluster.
    KAFKA_TEST_URL=127.0.0.1:9092 \
    SCHEMA_REGISTRY_TEST_URL=http://127.0.0.1:8081 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate costs over the rdkafka client it wraps, and what the runtime costs on top: two
# scenarios, each run three times over - the client driven directly, this crate's own consumer,
# and the whole service - against the stand the tests use. On demand only: it takes about fifteen
# minutes and it wants the machine to itself. The page it feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" KAFKA_TEST_URL=127.0.0.1:9092 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-rdkafka-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean
    rm -rf dist wheels

ci: check test typo security
