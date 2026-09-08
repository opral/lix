---
type: patch
---

Fixed CSV edits and reopen operations losing cells, formatting, or the stored dialect.

CSV now preserves UTF-8 BOMs, literal quote spelling, empty final cells, and missing final line endings when rows move. Multiline and adjacent file edits reconcile the correct rows, and unsupported NUL bytes are rejected before unreadable state is stored.
