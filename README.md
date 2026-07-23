# Calluwu

Calluwu is an open source toolkit and portable realtime runtime for building AI
voice agents. This repository contains the developer-facing SDK, versioned wire
contracts, browser WebRTC client, deterministic examples, and the Rust data
plane.

```typescript
import { Agent, cloudflareVoice } from "@calluwu/sdk";

export default new Agent({
  name: "customer-support",
  instructions: "You are a concise, helpful customer support agent.",
  ...cloudflareVoice(),
});
```

```bash
pnpm install
pnpm --filter @calluwu/sdk build
pnpm calluwu validate examples/customer-support/agent.ts
pnpm calluwu run examples/customer-support/agent.ts \
  --input "I need help changing my plan"
```

The checked-in example opts into `scriptedVoice()` so deterministic local runs need no cloud or
model credentials. Hosted agents use `cloudflareVoice()` (also the safe SDK default) and are
validated against the exact adapters installed in Calluwu Cloud.

## What is here

- `packages/sdk` — agent authoring, validation, local execution, deployment, and
  customer-scoped Cloud API client.
- `packages/types` — public agent, API, event, and realtime protocol contracts.
- `packages/webrtc-client` — dependency-free browser media lifecycle client.
- `runtime/calluwu-core` — portable bounded Rust streaming runtime.
- `examples/customer-support` — minimal end-to-end agent definition.

## Open source and Calluwu Cloud

The packages and runtime in this repository are Apache-2.0 licensed. Calluwu
Cloud is the optional hosted control plane, media edge, telephony, billing, and
managed operations product. Its service implementation is not part of this
repository and the Apache license does not imply that the hosted control plane
is self-hostable.

The SDK can validate and run deterministic agents locally without Calluwu
Cloud. Cloud deployment commands require a customer project and scoped API key.
Service initialization, tenant-wide recovery, carrier credentials, billing
operations, and other hosted-service owner capabilities are intentionally
absent from the public SDK.

## Requirements

- Node.js 22+
- pnpm 11+
- Rust 1.96+
- Docker 29+ only when building the runtime image

## Verification

```bash
pnpm install --frozen-lockfile
pnpm check
pnpm audit --prod
pnpm run licenses
cargo audit --deny warnings
```

See [`docs/README.md`](docs/README.md) for the component map and
[`CONTRIBUTING.md`](CONTRIBUTING.md) before proposing a change.
See [`docs/supply-chain.md`](docs/supply-chain.md) for the exact dependency license policy and
CycloneDX workflow artifacts.

## License and marks

Source code is available under the [Apache License 2.0](LICENSE). See
[`NOTICE`](NOTICE) for attribution and [`TRADEMARKS.md`](TRADEMARKS.md) for the
separate brand policy.
