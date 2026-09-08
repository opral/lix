---
type: minor
---

Sync clients commit edits and checkpoints locally and upload them in the background.

Current-state reads and writes no longer wait for a server round trip. Durable
replicas reopen offline, and historical data is fetched on demand and cached.
The server remains authoritative: incompatible concurrent branch updates replace
pending local work without a merge-conflict workflow. Upgrade sync clients and
servers together for sync protocol version 7.
