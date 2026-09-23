---
type: minor
---

Schema-backed `INSERT … SELECT` reads use the current transaction snapshot, so a statement can derive row values and scope from rows already visible to that transaction. Foreign-key diagnostics explain when a primary-key target exists only globally, and row-reference diagnostics identify global-only exact-identity matches, while preserving exact-scope enforcement. Delete and merge optimizations account for same-scope live row-reference sources before using collection-generation deletes.
