---
type: patch
---

Avoid invalidating and warming unchanged schema catalogs when publishing checkpoints, three-way merges, or data-only inherited-base refreshes.

Invalidate inherited catalogs when their visible definitions actually change, preserving local overrides, tombstones, and collection-generation fences.
