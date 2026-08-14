---
type: patch
---

Faster repeated SQL reads: a pooled query session is now reused across statements instead of being rebuilt for each one.

The engine used to re-register its per-statement SQL functions, re-install the `information_schema` views and deep-copy its function registries on every statement. Those are now installed once per session, and repeated statement shapes reuse the prepared scan plan directly. Point and multi-key reads get noticeably cheaper; results are unchanged.
