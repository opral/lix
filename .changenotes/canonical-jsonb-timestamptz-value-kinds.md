---
type: minor
---

Canonicalize Lix SQL value kinds across Rust, JavaScript, native bindings, and
the server protocol. JSONB values now use `Jsonb`/`jsonb`, and PostgreSQL
timestamp-with-time-zone values use `Timestamptz`/`timestamptz`. The former
`Json`/`json` and `Timestamp`/`timestamp` value kinds are removed without
compatibility aliases or dual wire decoding.
