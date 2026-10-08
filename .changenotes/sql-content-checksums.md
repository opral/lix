---
type: minor
---

Added SQL hashing and binary encoding functions for file verification.

Use `encode(sha256(content), 'hex')` to compare a file's stored bytes with a local
SHA-256 checksum without downloading the file. Checksums are computed on demand.
