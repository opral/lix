---
type: minor
---

Changed connected sync clients to serve only authority-certified current state.

Sync protocol v6 is a semantic hard cut from v5 and older clients that still permit replica-local writes; its existing JSON fields, paths, and operations are unchanged. JavaScript sync mutations and history execute on the authority, local HOT reads wait for a finite authority publication fence, and private replica receipts certify complete live values, provenance, and branch coordinates against the existing snapshot roots. Durable authority/replica storage fences prevent uncaptured local writes. The existing one-argument `lix_diff` reads Working Changes from the certified HOT epoch without hydrating arbitrary history; selected point-in-time file content remains server-first through the existing history surface.

The Rust and JavaScript public type and function surfaces are unchanged. Rust retains `ServerOptions::sync(...)` with `open_lix().with_server(...)` and its existing remote protocol client, while JavaScript retains `openLix({ server: { mode: "sync" | "remote", ... } })`. No SQL surface was added or removed. The semantic hard cut is intentional: raw Rust sync handles serve certified HOT reads and return `LIX_AUTHORITY_EXECUTION_REQUIRED` for mutations, transactions, observations, and history; server-first Rust applications use the existing protocol client. JavaScript `openLix` performs that authority routing internally.
