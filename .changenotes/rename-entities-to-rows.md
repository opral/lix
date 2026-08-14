---
type: minor
---

Renamed Lix's public entity vocabulary to rows.

Plugins now map files to rows, schema-defined SQL surfaces expose rows, and
row identity is represented by `row_pk` / `lixcol_row_pk`. This is an
intentional breaking hard cut: the former entity-named APIs and columns are no
longer available.
