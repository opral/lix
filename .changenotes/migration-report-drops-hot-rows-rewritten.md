---
type: minor
---

`MigrationReport` no longer carries the `hot_rows_rewritten` field.

The field belonged to the retired pre-v72 migration chain and always reported zero on every path the v75 migration supports. Callers logging or persisting migration reports should drop the field; `changes_rewritten` and `commit_members_rewritten` remain.
