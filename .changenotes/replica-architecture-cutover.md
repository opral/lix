---
type: minor
---

Separate repository migration from ordinary opening, and make browser replica reads and shared identity admission reliable during concurrent synchronization.

This is a coordinated breaking upgrade: migrate every authority and local repository with the detached migration tools before admitting it to the current runtime, and upgrade clients and servers together to sync protocol 9 and storage format 81. Migration preserves source repositories and pending local work; dormant browser repositories migrate when their device returns. Ordinary opening no longer performs legacy migrations or remote SQL identity probes.

Custom JavaScript HTTP transports must implement the typed request and response contract. Replica conversion now belongs to the detached migration API. Buffered foreground reads have a 30-second deadline and a combined 64 MiB / 1,000,000-row result limit; oversized results and exhausted read progress return structured errors. Browser storage close now waits for physical ownership release when the last client disconnects.
