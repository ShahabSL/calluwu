# Security policy

## Supported version

Until Calluwu publishes a stable release, only the current `main` branch is
supported. Security fixes are forward-applied; older pre-release commits may
not receive patches.

## Reporting a vulnerability

Do not open a public issue with exploit details, credentials, phone numbers,
transcripts, customer data, or private infrastructure information. Use this
repository's **Security → Advisories → Report a vulnerability** flow.

Include the affected commit and component, impact, a minimal safe reproduction,
and contact details. Use synthetic resource identifiers and redact all real
secrets and personal data. Do not test against Calluwu Cloud or another
person's deployment without explicit authorization.

## Public repository scope

Reports for the following components belong here:

- `@calluwu/sdk`
- `@calluwu/types`
- `@calluwu/webrtc-client`
- `runtime/calluwu-core`
- public examples and build/release tooling

Hosted-service, account, billing, telephony, or customer-data reports should use
the private security contact published by Calluwu Cloud. Do not include those
details in a public issue.

Security-sensitive changes require regression coverage and review by someone
other than the author.
