---
type: patch
---

Fixed Undo and Redo of plugin-backed files failing with a duplicate `lix_binary_blob_ref` primary key, and restored support for applying file additions and deletions from history.

Applying historical file changes now replaces the historical blob reference with the plugin's newly rendered reference. References and deletion records for other files remain intact.

Complete file restoration replays its historical plugin state and bytes together, while preserving source-change validation and restrictions on direct writes to engine-managed plugin metadata.

Optimized SQL writes now enforce those same reserved-metadata restrictions.
