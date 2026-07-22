---
type: patch
---

Remote file writes now locate unchanged regions in cached request blobs with word-sized comparisons, reducing client CPU time for localized edits.
