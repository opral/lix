---
type: minor
---

Export Lix engine spans as standard OpenTelemetry Protocol protobuf.

The telemetry API accepts W3C parent context and exports OTLP requests with sanitized SQL query shapes. Hosts can batch and forward these spans to any OTLP backend; asynchronous delivery and flush failures are reported by `Lix.close()`.
