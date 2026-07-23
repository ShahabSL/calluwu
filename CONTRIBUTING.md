# Contributing

## Before opening a change

1. Preserve public API, event ordering, generation fencing, bounded-resource,
   and credential-handling invariants.
2. Add regression coverage for defects and failure-path tests for external
   boundaries.
3. Keep credentials, phone numbers, transcripts, audio, tool payloads, and
   customer data out of source, fixtures, snapshots, and logs.
4. Do not add unbounded queues, unconditional retries, silent provider
   fallbacks, or unaudited external side effects.
5. Update TypeScript/Rust contracts and fixtures together when a wire contract
   changes.

Run:

```bash
pnpm install --frozen-lockfile
pnpm check
pnpm audit --prod
pnpm run licenses
cargo audit --deny warnings
```

Use Conventional Commits. Keep changes cohesive, document public-contract
effects, and follow `CODE_OF_CONDUCT.md`.
