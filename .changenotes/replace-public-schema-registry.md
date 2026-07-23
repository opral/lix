---
type: minor
---

Replace the raw `lix_registered_schema`, `lix_registered_schema_by_branch`, and
`lix_registered_schema_history` SQL surfaces with the read-only `lix_schema`
catalog and writable `lix_schema_definition` authoring surface. Schema keys are
derived from definitions, the exact key `lix` and every `lix_*` key are
reserved, and generated SQL surface-name collisions are rejected.
