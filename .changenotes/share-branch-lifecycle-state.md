---
type: patch
---

Share immutable state when creating branches and refresh inherited schema catalogs without rebuilding unchanged branch-local rows.

Preserve private checkpoint before-images, untracked rows, and local overrides across stale-base refreshes, and invalidate cached catalogs when newly inherited schemas become visible.
