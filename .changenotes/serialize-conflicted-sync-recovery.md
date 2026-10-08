---
type: patch
target: lix
---

Resume partial-replica uploads from a fresh authority descriptor after a rejected publication or expired serving lease. Keep the exact pending upload until inclusion is proven, prevent local edits or progress on another branch from replaying stale coordinates ahead of recovery, and keep expired serving roots fenced until durable publication adopts a fresh lease.
