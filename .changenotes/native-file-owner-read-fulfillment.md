---
type: patch
target: lix
---

Fix native partial directory metadata reads after creating a plugin-managed file. Read fulfillment now accepts the reserved executable-owner dependency only alongside a directly selected canonical row for the same file and physical branch. Malformed owners and unrelated keys, files, directories or branches remain rejected.

Directory metadata preparation now uses the same directory-only path index as query execution, avoiding unrelated file descriptors and executable owners during cold folder listing.
