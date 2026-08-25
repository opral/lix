---
type: minor
---

Open a Lix Server Protocol authority with `open_lix().with_storage(storage).serve().await`.

Serving now owns the repository engine directly and retains one application session per successful handshake. The former `OpenLixBuilder::as_protocol_root()`, `LixServerProtocol::new()`, and `LixServerProtocol::with_options()` APIs were removed. Configure limits with `open_lix().serve().with_options(options).await`.
