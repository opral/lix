---
type: patch
---

Introduced schema-typed entity primary-key tuples and compact UUID storage.

Lix keeps canonical UUID strings at JSON and API boundaries, validates UUID
primary-key properties from their schemas, and stores UUID tuple components as
raw 16-byte values in RocksDB and SlateDB keys. UUIDv7 remains the generated,
time-ordered identity; deterministic plugin archive IDs use UUIDv5.

The tuple representation also supports integer, string, and byte components
with a versioned order-preserving encoding. This is a clean repository protocol
cut; older repositories must be recreated.
