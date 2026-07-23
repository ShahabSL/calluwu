# Dependency licenses and software bills of materials

Calluwu treats a dependency license change as a release-contract change. Production Node and Rust
dependencies must resolve entirely to the permissive SPDX identifiers in
[`dependency-policy.json`](../dependency-policy.json). An unknown, missing, malformed, copyleft, or
otherwise unreviewed identifier fails CI and the package release gate.

## License gates

The Node gate runs the exact pnpm 11.9.0 command `pnpm licenses list --prod --json`, validates the
complete JSON shape, parses compound SPDX expressions, and requires every license term and
exception to be explicitly allowed. It never treats a failed pnpm scan or an empty graph as success.
pnpm 11.9 may report `ERR_PNPM_MISSING_PACKAGE_INDEX_FILE` even after a clean install for packages
excluded from the release-age gate. Only that exact error activates the reviewed fallback. The
fallback compares the complete production graph reported from the frozen lockfile with the installed
production graph, then reads and identity-checks every installed package manifest. A dependency
without manifest evidence fails unless it is one of the exact esbuild 0.28.1 non-host binary
packages listed in `dependency-policy.json`; those target packages are present in the lock graph but
pnpm installs only the host target. Graph drift, an unused override, a version change, a missing
license, or any other pnpm error fails the gate.

The Rust gate uses cargo-deny 0.19.4 with [`deny.toml`](../deny.toml). Development-only crates are
excluded, build dependencies are included, license-text detection requires 95% confidence, and
licenses not on the allowlist are denied by default. The wrapper checks the installed cargo-deny
version and refuses drift between `deny.toml` and `dependency-policy.json`.

Install and run the exact tools:

```bash
pnpm install --frozen-lockfile
cargo install --locked --version 0.19.4 cargo-deny
pnpm run licenses
```

The policy is intentionally narrower than “OSI approved.” Adding an identifier or exact fallback
override requires reviewing the exact license terms, how the dependency reaches a shipped artifact,
attribution obligations, and whether the policy remains appropriate for the public packages and
native runtime.

## SBOM artifacts

CI and package release workflows pin Trivy 0.72.0 and generate CycloneDX JSON source SBOMs from the
reviewed checkout and lockfiles. Runtime image jobs also generate a separate CycloneDX image SBOM.
GitHub stores these as workflow artifacts; generated SBOMs live under ignored `artifacts/` paths
and are not committed.

An SBOM is inventory evidence, not proof that package metadata is accurate and not a substitute for
the fail-closed license checks, vulnerability scans, provenance, or release review. Source SBOMs
describe the dependency material visible to Trivy in the checkout. Runtime image SBOMs describe the
built Linux image. Neither can enumerate dependencies loaded later by an external carrier, model
provider, browser extension, or customer integration.

Primary tool references:

- [pnpm licenses](https://pnpm.io/cli/licenses)
- [cargo-deny license checks](https://embarkstudios.github.io/cargo-deny/checks/licenses/)
- [Trivy CycloneDX output](https://trivy.dev/latest/docs/supply-chain/sbom/)
