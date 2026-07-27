---
type: patch
---

Accelerated experimental Git history replay with a RocksDB-native, Git-text-aware import path.

The replay command now keeps Git object readers alive across the selected first-parent history, preserves Git file modes and object identities, and records empty commits without collapsing history.
