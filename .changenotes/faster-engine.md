---
type: minor
---

Lix is substantially faster and more storage-efficient for large files and
workspaces.

v0.9 adds indexed and batched file operations, faster SQL reads and writes,
compressed native storage, lower-copy blob handling, and more efficient
tracked-state merges. Remote clients also transfer localized file and query
changes instead of repeatedly sending complete payloads.

This release changes the tracked-state and SlateDB physical formats. Existing
repositories created by older engine versions must be recreated.
