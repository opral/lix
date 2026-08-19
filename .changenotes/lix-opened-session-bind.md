---
type: patch
---

Added a vendor-neutral `lix.opened` engine span when a client session binds to a Lix.

In-process `open_lix()` and protocol handshake session creation each emit one span. Protocol roots and cached runtimes opened with `as_protocol_root()` attach a telemetry sink without emitting. Hosts that mint a session against an already-open runtime call `Lix::bind_session()` or `lix::bind_session`.
