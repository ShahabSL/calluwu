# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769

ARG SOURCE_DATE_EPOCH=0
ARG CALLUWU_RUNTIME_PLATFORM=linux/amd64

FROM --platform=${CALLUWU_RUNTIME_PLATFORM} rust:1.97.1-alpine3.23@sha256:c4a364ddbf684fe038e6fa6a4f25b30c8dc85247423e0e660676ece0d17be4a2 AS builder
ARG CALLUWU_SOURCE_REVISION
ARG CALLUWU_RUNTIME_PLATFORM
ARG SOURCE_DATE_EPOCH
ARG TARGETPLATFORM
ENV RUSTUP_TOOLCHAIN=1.96.0
WORKDIR /workspace

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY runtime/calluwu-core runtime/calluwu-core

RUN set -eu; \
    if [ "${CALLUWU_RUNTIME_PLATFORM}" != "linux/amd64" ]; then \
        echo 'CALLUWU_RUNTIME_PLATFORM must be linux/amd64 for Cloudflare Containers' >&2; \
        exit 1; \
    fi; \
    if [ "${TARGETPLATFORM}" != "${CALLUWU_RUNTIME_PLATFORM}" ]; then \
        echo "target platform must be ${CALLUWU_RUNTIME_PLATFORM}, got ${TARGETPLATFORM}" >&2; \
        exit 1; \
    fi; \
    rust_host="$(rustc -vV | sed -n 's/^host: //p')"; \
    if [ "${rust_host}" != "x86_64-unknown-linux-musl" ]; then \
        echo "builder must target x86_64-unknown-linux-musl, got ${rust_host}" >&2; \
        exit 1; \
    fi; \
    source_digest="$(find Cargo.toml Cargo.lock rust-toolchain.toml runtime/calluwu-core \
        -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum | sha256sum | cut -d ' ' -f 1)"; \
    source_revision="${CALLUWU_SOURCE_REVISION:-sha256:${source_digest}}"; \
    if ! printf '%s\n' "${source_revision}" \
        | grep -Eq '^(sha256:[0-9a-f]{64}|[0-9a-f]{40}|[0-9a-f]{64})$'; then \
        echo 'CALLUWU_SOURCE_REVISION must be a full immutable Git or SHA-256 revision' >&2; \
        exit 1; \
    fi; \
    case "${SOURCE_DATE_EPOCH}" in ''|*[!0-9]*) \
        echo 'SOURCE_DATE_EPOCH must be a non-negative integer' >&2; exit 1;; esac; \
    build_timestamp="$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ)"; \
    CALLUWU_SOURCE_REVISION="${source_revision}" \
    CALLUWU_SOURCE_DIGEST="sha256:${source_digest}" \
    CALLUWU_BUILD_TIMESTAMP="${build_timestamp}" \
    CALLUWU_BUILD_TOOLCHAIN="$(rustc --version)" \
    CALLUWU_BUILD_PROFILE="release-locked" \
    CALLUWU_BUILDER_IMAGE="docker.io/library/rust:1.96.0-alpine3.23@sha256:5dc2af9dd547c33f64d5fc1d299ab93b51f39eaa16c426c476b990ce6caf5b3e" \
    CALLUWU_RUNTIME_BASE_IMAGE="scratch" \
    SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
    cargo build --locked --release --package calluwu-core --bin calluwu-runtime; \
    if readelf -l target/release/calluwu-runtime | grep -q 'Requesting program interpreter'; then \
        echo 'calluwu-runtime must not require a dynamic program interpreter' >&2; \
        exit 1; \
    fi; \
    if readelf -d target/release/calluwu-runtime 2>/dev/null | grep -q '(NEEDED)'; then \
        echo 'calluwu-runtime must not require shared libraries' >&2; \
        exit 1; \
    fi; \
    touch -d "@${SOURCE_DATE_EPOCH}" target/release/calluwu-runtime

FROM --platform=${CALLUWU_RUNTIME_PLATFORM} scratch AS runtime

COPY --from=builder --chown=65532:65532 /workspace/target/release/calluwu-runtime /calluwu-runtime

USER 65532:65532
EXPOSE 8080
ENV RUST_LOG=calluwu_core=info,calluwu_runtime=info,tower_http=info
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/calluwu-runtime", "probe", "--address", "127.0.0.1:8080", "--timeout-ms", "1500"]
ENTRYPOINT ["/calluwu-runtime", "serve", "--bind", "0.0.0.0:8080"]
