---
type: patch
---

Resumable media uploads on the SlateDB backend are dramatically faster.

Writes that must cross a durability boundary — every part of a resumable file upload, and any statement acknowledged from a durable idempotency receipt — used to wait for SlateDB's periodic write-ahead-log flush timer, costing up to 100 ms of idle latency per commit no matter how small the payload. Lix now asks SlateDB to flush immediately instead of waiting for the timer. The durability guarantee is unchanged, and media ingest on SlateDB is over ten times faster.
