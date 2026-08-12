---
type: minor
---

Lix now provides its canonical server protocol directly through the optional Rust `server-protocol` feature.

Server hosts can dispatch framework-neutral HTTP requests through `LixServerProtocol`, while authenticated account identity and idempotency scope are supplied as trusted host context instead of client-controlled handshake parameters.
