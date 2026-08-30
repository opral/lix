---
type: minor
---

Changed connected sync clients to serve only authority-certified current state.

Protocol v5 binds each complete ordered repository event, including its cursor, immutable objects, ref updates, inline manifests, complete live row values, provenance, branch coordinates, and replica receipts. Connected mutations and history execute on the authority, local HOT reads wait for the authority publication they certify, and durable authority/replica storage fences prevent uncaptured local writes. Working Changes can read pinned before/after file content from the certified HOT epoch without hydrating arbitrary history.

The safe Rust API no longer exposes an eventual-consistency connected-cache builder. Rust applications should use the remote server protocol. The raw local cache runtime is reachable only through a doc-hidden unsafe bridge used by the JavaScript SDK's certified composite client; unsafe callers must uphold the same publication, routing, session-alignment, and terminal-close contract.
