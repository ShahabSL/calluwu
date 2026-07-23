# Calluwu Cloud boundary

Calluwu Cloud is an optional hosted service for project management, durable
session coordination, media, telephony, metering, and operational recovery.
This repository provides its public developer clients and portable runtime, not
the hosted control-plane implementation.

Customer code uses scoped API keys and project-owned resources. The public SDK
does not contain service initialization, cross-tenant search, recovery-queue
controls, vendor credentials, carrier provisioning, billing reconciliation, or
other hosted-service owner operations.

Cloud commands fail explicitly when the hosted service or a required vendor is
not configured. Local deterministic execution never silently substitutes for a
requested live provider.
