---
type: patch
---

Improved parameterized `lix_key_value` observer reads by resolving exact keys
through the live-state point reader instead of constructing a DataFusion plan.
Active-branch overrides and global fallback retain their existing behavior.
