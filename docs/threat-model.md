# Threat Model

- Duplicate requests: database uniqueness and provider idempotency.
- Unauthorized object access: merchant-scoped authorization on every payment.
- Webhook SSRF: destination allow-list, DNS/IP validation, redirect restrictions, egress policy.
- Secret leakage: secret manager/environment injection; no secrets in Git or logs.
- Event replay/tampering: authenticated broker, stable event IDs, schema validation, dedupe.
- Ledger mutation: append-only application role and audited compensating entries.
- Resource exhaustion: request limits, bounded queues, timeouts, rate limits.
- ML leakage/drift: temporal validation, versioned features, drift and false-decline monitoring.
- Sensitive data exposure: synthetic data, minimization, access controls, retention rules.

Do not claim PCI DSS compliance from this student project. Use sandbox tokens and avoid raw cardholder data.
