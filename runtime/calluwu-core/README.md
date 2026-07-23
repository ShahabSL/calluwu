# Calluwu realtime runtime

`calluwu-runtime` is the portable Rust data plane used by warm Cloudflare
Container shards and local deterministic simulations. It multiplexes bounded
single-writer session actors; it does not execute management APIs or arbitrary
customer JavaScript.

## CLI

```sh
calluwu-runtime serve --bind 127.0.0.1:8080
calluwu-runtime serve --bind 127.0.0.1:0 --agent-manifest ./agent-manifest.json
calluwu-runtime probe --address 127.0.0.1:8080 --timeout-ms 1500
calluwu-runtime simulate --agent-manifest ./agent-manifest.json --input "hello" --events ./events.ndjson
```

`probe` performs one bounded, dependency-free HTTP/1.1 readiness check against
a numeric IPv4 or IPv6 loopback address. It requires the `GET /healthz` response
to be a complete `200 application/json` ready contract and rejects oversized,
malformed, inconsistent, or slow responses. The container image uses this
subcommand for its Docker health check; it cannot be redirected to an external
network address.

`simulate` intentionally maps valid local provider references (including
`project-default`) to deterministic scripted providers. It prints protocol
messages as NDJSON and needs no credentials. `serve --agent-manifest` prepares
one local WebSocket session and prints a `runtime.local.ready` NDJSON record
containing its URL and seven required headers. Local scripted modes reject
declared tools until an explicit local adapter is installed; they never
fabricate HTTPS, builtin, or JavaScript success.

Cloud admission uses `GatewayProviderResolver`. It accepts either an exact
credential-free `scripted/scripted-v1` deployment with no tools, or the checked
Cloudflare Workers AI set: Nova-3 STT, GPT-OSS 20B reasoning, and Aura-2 English
or Spanish TTS. Live calls reach the API's generation-fenced provider endpoints
with their per-session runtime credential. Local tool declarations are rejected by
cloud admission and fail closed at the private gateway; HTTPS/builtin tools route
to the private encrypted integration gateway. The runtime coordinator still enforces declaration,
concurrency, deadlines, cancellation, atomic history, and side-effect class;
durable reservation, replay, and commit-once ownership live at the API/gateway
boundary.

## Container contract

- `GET /health/live` — process liveness.
- `GET /health/ready` (and compatibility alias `/healthz`) — status, stable
  process `bootId`, and current load; returns 503 while full/draining.
- `GET /load` — boot ID plus active/prepared/max/available session capacity.
- `GET /build` — boot ID, version, protocol, immutable source revision,
  build timestamp/toolchain/profile, and pinned builder/runtime image identities.
- `POST /v1/sessions/admit` — trusted, bounded pre-admission of the immutable
  deployment manifest and per-generation event-ingest binding.
- `POST /v1/sessions/:sessionId/cancel` — trusted, idempotent,
  generation/tenant/ingest-fenced cancellation; waits for actor termination.
- `GET /v1/realtime` — WebSocket attachment to the prepared session.

The control plane must pre-admit with:

```json
{
  "organizationId": "org_...",
  "projectId": "prj_...",
  "deploymentId": "dep_...",
  "sessionId": "ses_...",
  "runtimeGeneration": 1,
  "manifest": {},
  "runtimeIngestUrl": "https://control.example/v1/runtime/sessions/ses_.../events",
  "runtimeIngestToken": "opaque-per-generation-secret"
}
```

Exact retries are idempotent. A different generation, manifest, or ingest
binding for the same session conflicts. A successful prepare returns
`{sessionId,runtimeGeneration,bootId}` with 201 (new) or 200 (exact replay), so
the allocator can atomically fence process replacement. Unattached preparations
expire after a 120-second lease (leaving margin beyond the API's 90-second
attachment window) and emit `session.failed`. The subsequent upgrade
carries the seven trusted `X-Calluwu-*` identity/generation/ingest headers and
atomically consumes the preparation; only one transport may attach. Containers
must not be directly internet-accessible; only the control-plane proxy may
supply these headers. Tokens, raw audio, transcripts, caller terminal reasons,
tool arguments, and tool results are never logged or placed in semantic event
payloads.

After `session.ready`, the client sends `session.start` and waits for the
`session.started` acknowledgement. Start, text, PCM audio, and `input.commit`
share an ordered lane, so WebSocket ordering is preserved. A full media lane,
odd-byte PCM, or partial-audio corruption fails closed. A normal caller hangup
sends `session.end`. The runtime drains the actor, emits
exactly one `session.completed`, durably flushes the accepted event batch, and
only then closes the WebSocket with code 1000. Code 1011 means the actor or its
terminal-event flush failed; clients must invoke the idempotent control-plane
cancel/reconciliation path. Clients must not race a code-1000 graceful path
against that endpoint; cancel is otherwise reserved for operator,
transport-loss, or abort semantics and emits `session.canceled`.

Outbound audio is one binary frame:

```text
CWU1 | JSON header length (u32 big-endian) | JSON header | raw PCM16LE
```

The JSON header is at most 4 KiB and includes `type`, the protocol envelope,
`responseId`, `epoch`, `sequence`, `encoding`, `sampleRateHz`, and `channels`.
The raw payload is at most 64 KiB. Control messages remain protocol-v1 JSON text
frames.

Every WebSocket write—including control/audio output, errors, pong, and close—
has a five-second deadline; timeout drops the transport and reclaims its session
capacity. Semantic events use a bounded ordinary spool plus a separately
reserved terminal slot. Producer sequences are allocated only after capacity is
secured, so overload cannot create gaps or starve the final session event.

The production Dockerfile pins its frontend and native-musl Rust builder by
immutable multi-platform digest, validates a fully static executable, and copies
only that executable into a `scratch` runtime stage. Reqwest's pinned
`rustls-tls` feature embeds WebPKI roots, and the runtime uses UTC rather than an
operating-system time-zone database, so the image needs no CA, libc, shell, or
package-manager files. It runs as numeric UID/GID 65532.

Cloudflare Containers require `linux/amd64`. Wrangler 4.112 and the documented
Docker command request that target even on an ARM development host; the
Dockerfile independently validates the requested target and native-musl builder,
so mislabeled or non-amd64 output fails closed. `/build.sourceDigest` is a
SHA-256 digest of the exact Cargo manifests, lockfile, toolchain file, Dockerfile,
and runtime source tree consumed by the image build. Unless an explicit full Git
or SHA-256 revision is supplied, `sourceRevision` uses the same digest; mutable
names such as branches and tags fail the image build.

## Verification

```sh
cargo fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo bench --package calluwu-core --bench latency
docker build --platform linux/amd64 -f runtime/calluwu-core/Dockerfile .
```
