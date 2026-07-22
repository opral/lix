---
type: patch
---

Large file writes now reuse content hashes computed while preparing CAS chunks.

This removes a second hash of every chunk before staging the write without changing chunking or storage formats.
