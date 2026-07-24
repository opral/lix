---
type: minor
---

Added atomic binary batches for ordinary file upserts through the Lix engine,
Rust SDK, and server protocol.

Clients can send up to 1,024 raw file payloads in one request and receive one
commit with the standard execution response.
