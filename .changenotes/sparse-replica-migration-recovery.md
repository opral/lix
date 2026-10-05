---
type: patch
---

Fixed sparse-replica checkpoint upload, history reopening, and recovery of older local repositories.

Checkpoint preparation now fetches missing authoritative changes and preserves conversation details when history is opened on a fresh replica. Migration retains original local data and resumes interrupted conversion or lost server acknowledgements. Browser repositories keep verified offline access after migration and owner teardown.
