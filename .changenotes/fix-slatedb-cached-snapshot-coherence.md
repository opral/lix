---
type: patch
---

Fixed a rare stale read on SlateDB storage after a concurrent commit.

A reader that picked up a cached storage snapshot could miss a write that had already been committed, returning the previous value from either a point read or a range scan. It required a commit to land at the same moment another reader acquired its snapshot, and it cleared on reopen, but while it lasted a live session could observe out-of-date data.
