---
type: minor
---

Added optional origin keys for tagging Lix writes.

`lix.execute(sql, params, { originKey })` in JavaScript and `execute_with_options(sql, params, options)` in Rust stamp the change records a write produces. The key is exposed as `origin_key` on `lix_change` and as `lixcol_origin_key` on state, file, and history surfaces; writes without an origin key stay `NULL`.
