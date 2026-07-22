---
type: patch
---

Existing `lix_file` path upserts no longer rewrite file descriptors when the
incoming metadata is unchanged, avoiding workspace-wide namespace validation
for byte-only overwrites.
