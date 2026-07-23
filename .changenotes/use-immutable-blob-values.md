---
type: minor
---

Changed Rust `Value::Blob` payloads from `Vec<u8>` to a new immutable `Blob`
type backed by `bytes::Bytes`.

Blob values now clone in constant time and share their payload across query
parameters, exact `lix_file` writes, transaction staging, CAS encoding, and
result fan-out. Rust callers must convert owned vectors with `.into()` when
constructing blob values.
