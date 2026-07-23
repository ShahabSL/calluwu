# Project governance

Calluwu is a maintainer-led open source project. Maintainers are accountable for technical
direction, releases, security response, repository permissions, and the long-term coherence of the
public API.

## How decisions are made

Routine changes are decided through review. Changes to public contracts, security boundaries,
compatibility policy, licensing, or project governance need a written rationale and approval from a
maintainer who did not author the change. Significant architectural decisions should be recorded in
`docs/`.

Maintainers prefer evidence from specifications, tests, benchmarks, and operational behavior over
authority or vote counting. When consensus is not possible, the maintainers make the decision and
document the material tradeoffs.

## Maintainers

Maintainers are listed in `.github/CODEOWNERS`. Existing maintainers may invite a contributor after
sustained, trustworthy work across implementation, review, security, and community responsibilities.
Access may be reduced after extended inactivity or removed for security or conduct reasons.

No contributor is entitled to merge, release, package-registry, or infrastructure access solely
because of contribution volume.

## Releases and compatibility

The project uses semantic versioning for published packages. Until `1.0.0`, minor releases may
change unstable APIs with migration notes. Security fixes may be released without a public embargo
discussion. A release must be built from a reviewed commit and pass the repository's required
checks.

## Appeals and governance changes

A contributor may ask for reconsideration by opening an issue that identifies the decision, new
evidence, and requested outcome. Private security or conduct matters must use the private reporting
path. Governance changes follow the same review standard as other public-contract changes.
