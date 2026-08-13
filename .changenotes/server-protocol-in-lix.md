---
type: minor
---

Lix now provides the canonical Lix Server Protocol directly through the optional Rust `server-protocol` feature.

Server hosts can dispatch framework-neutral HTTP requests through `LixServerProtocol`, while authenticated account identity and idempotency scope are supplied as trusted host context instead of client-controlled handshake parameters.
