# Public release integrity

The public repository is produced from the private development workspace by
`scripts/export-public.mjs`.

The exporter:

1. copies an exact, reviewed source-to-destination inventory;
2. rejects symlinks, special files, build outputs, secret-like content, and
   unexpected files inside public component trees;
3. rejects service-owner SDK commands and routes;
4. normalizes file modes and writes a deterministic SHA-256 manifest; and
5. validates that the destination contains no unlisted path.

Run it only into a new or empty directory:

```bash
node scripts/export-public.mjs /absolute/path/to/calluwu-public
```

Review `.public-export-manifest.json`, run the full checks in the exported
directory, and scan the result before publishing. The exporter is a boundary
control, not a replacement for human review, dependency scanning, or Git
history hygiene.

The exact dependency license gate and CycloneDX workflow artifacts are documented in
[`supply-chain.md`](supply-chain.md). A failed or incomplete dependency scan blocks release; it is
never converted into an empty or partial report.

## npm packages

The `publish.yml` workflow accepts only a tag that exactly matches the shared SDK, types, and WebRTC
package version. It rebuilds and tests the public repository, creates reviewed pnpm tarballs so
`workspace:*` dependencies become exact release versions, and invokes the npm CLI on GitHub-hosted
runners with OIDC trusted publishing. The `npm` GitHub environment should require the second
maintainer's approval.

The tag must identify the current reviewed `main` tip. The tag job independently repeats the
production dependency audits, license policy, full test/build checks, and blocking Trivy scan; it
does not rely on a previous branch workflow result. Package publication is ordered (`types`,
`webrtc-client`, then `sdk`) and resume-safe. Before each publish, the workflow compares the exact
local tarball SHA-512 integrity with any immutable version already present in npm. It skips only
byte-identical artifacts and fails closed on a mismatch.

Before the workflow can publish, an npm owner must create or claim `@calluwu`, perform the registry's
required one-time first publication for each package, enable 2FA, and configure each package's
trusted publisher as `ShahabSL/calluwu` with workflow filename `publish.yml` and the `npm`
environment. Revoke long-lived automation tokens after OIDC succeeds. Never push a release tag until
that external registry configuration and package ownership have been independently verified.
