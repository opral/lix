---
type: minor
---

Support `INSERT ... SELECT` into registered tables and consume insert sources incrementally. Improve indexed joins, wide `IN` predicates, reference checks after updates, and small-limit reads without scanning unrelated rows in ordinary HOT collections.

Breaking: `information_schema.columns.data_type` now reports logical types such as `JSONB`, `ROW_REF`, and `TIMESTAMPTZ` directly. The redundant `lix_value_kind` column is removed. Repository format v82 rebuilds derived indexes during migration; sparse replicas migrate offline without fetching missing data.
