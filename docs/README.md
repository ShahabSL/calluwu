# Calluwu open source components

This repository is the public developer and runtime layer of Calluwu.

| Component | Contract |
| --- | --- |
| `@calluwu/sdk` | Author, validate, bundle, run, and deploy agents; use customer-scoped Cloud APIs |
| `@calluwu/types` | Versioned agent, event, API, and realtime schemas |
| `@calluwu/webrtc-client` | Browser microphone, SFU signaling, reconnect, and cleanup lifecycle |
| `calluwu-core` | Bounded native session actors and streaming provider/runtime protocol |

Start with the root quickstart, then read the package and runtime READMEs:

- [`packages/webrtc-client/README.md`](../packages/webrtc-client/README.md)
- [`runtime/calluwu-core/README.md`](../runtime/calluwu-core/README.md)
- [`docs/cloud-boundary.md`](cloud-boundary.md)
- [`docs/release-integrity.md`](release-integrity.md)
- [`docs/supply-chain.md`](supply-chain.md)

The hosted Calluwu Cloud implementation is maintained separately. Public
schemas describe customer-facing compatibility; they do not expose
service-owner administration or grant access to hosted infrastructure.
