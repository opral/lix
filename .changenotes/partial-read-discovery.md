---
type: patch
---

Partial replicas batch cold-read dependency discovery at the authority instead of fetching one missing native pointer and restarting the read repeatedly. Reads remain local after verified immutable inputs are hydrated, preserving pending local edits. Sync protocol 20 requires matching client and server versions.
