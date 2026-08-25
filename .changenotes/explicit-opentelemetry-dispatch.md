---
type: minor
---

Configure `OpenTelemetryTracingSink` with an explicit `tracing::Dispatch`.

The zero-argument constructor was removed. The captured dispatch must contain a `tracing-opentelemetry` layer and now governs both span enablement and creation, independent of ambient task or thread subscribers.
