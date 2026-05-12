# Diagram

```text
┌─────────────────────┐
│ commit_store.commit │
├─────────────────────┤
│ key: commit_id      │
│                     │
│ commit header       │
│ - change_id         │
│ - parent_commit_ids │
│ - change_pack_count │
│ - membership_count  │
└────┬─────────────┬──┘
     │             │
     │             │ optional commit_id + pack_id
     │             │ not written by this probe
     │             ▼
     │   ┌──────────────────────────┐
     │   │ commit_store.change_pack │
     │   ├──────────────────────────┤
     │   │ key: commit_id + pack_id │
     │   │                          │
     │   │ authored changes[]       │
     │   │ - change_id              │
     │   │ - schema_key             │
     │   │ - entity_id              │
     │   │ - snapshot_ref           │
     │   │ - metadata_ref           │
     │   └──────────────────────────┘
     │
     │ commit_id
     ▼
┌──────────────────────────┐
│ tracked_state.delta_pack │
├──────────────────────────┤
│ key: commit_id           │
│                          │
│ tracked row deltas[]     │
│ - schema_key             │
│ - entity_id              │
│ - file_id                │
│ - deleted                │
│ - change_locator         │
│ - snapshot_ref ──────────┼──────┐
│ - metadata_ref ──────────┼──┐   │
└──────────────────────────┘  │   │
                              │   │
                              │   │ also acts as authored
                              │   │ change source for this
                              │   │ tracked write path
                              │   │ json_ref
                              ▼   ▼
                    ┌───────────────────┐
                    │  json_store.pack  │
                    ├───────────────────┤
                    │ key: commit_id    │
                    │    + pack_id      │
                    │                   │
                    │ packed small JSON │
                    │ payloads          │
                    └───────────────────┘

                    ┌───────────────────┐
                    │  json_store.json  │
                    ├───────────────────┤
                    │ key: json_ref     │
                    │                   │
                    │ direct large JSON │
                    │ payload           │
                    └───────────────────┘
```

Write path:

```text
INSERT / UPDATE / DELETE
        │
        ▼
┌─────────────────────┐
│ normalize SQL rows  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ write JSON payloads │───► json_store.pack
└──────────┬──────────┘     json_store.json
           │
           ▼
┌─────────────────────┐
│ create commit       │───► commit_store.commit
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ write row deltas    │───► tracked_state.delta_pack
└─────────────────────┘


Not written by this probe:

tracked_state.tree.root
  key:   commit_id
  value: tracked_state root hash
         |
         v

tracked_state.tree.chunk
  key:   content hash
  value: materialized tracked-state tree chunk

tracked_state.tree.root.by_file
  key:   commit_id
  value: by-file index root hash

commit_store.membership_pack
  key:   commit_id + pack_id
  value: adopted/shared change locators
```

## Behavior 100 Inserts

One SQL statement:

```sql
INSERT INTO json_pointer (...) VALUES (... 100 rows ...)
```

Physical writes:

```text
commit_store.commit       +1
tracked_state.delta_pack  +1  contains 100 inserted row deltas
json_store.pack           +1  contains JSON payloads
```

Measured diff:

```text
commit_store.commit       added=1  bytes=205
tracked_state.delta_pack  added=1  bytes=13,287
json_store.pack           added=1  bytes=34,513
```

## Behavior 100 Updates

100 SQL statements by primary key:

```sql
UPDATE json_pointer SET value = ... WHERE path = '<pk>'
```

Physical writes:

```text
commit_store.commit       +100
tracked_state.delta_pack  +100  one updated row delta per commit
json_store.pack           +99   small updated JSON payloads
json_store.json           +1    large updated JSON payload
```

Measured diff:

```text
commit_store.commit       added=100 bytes=20,500
tracked_state.delta_pack  added=100 bytes=22,723
json_store.pack           added=99  bytes=27,168
json_store.json           added=1   bytes=110,035
```

## Behavior 100 Deletes

100 SQL statements by primary key:

```sql
DELETE FROM json_pointer WHERE path = '<pk>'
```

Physical writes:

```text
commit_store.commit       +100
tracked_state.delta_pack  +100  one tombstone row delta per commit
```

Deletes do not write JSON payloads in this probe.

Measured diff:

```text
commit_store.commit       added=100 bytes=20,500
tracked_state.delta_pack  added=100 bytes=22,592
```

## Optimization Axes

Use `optimization_log11.md` as the running decision log. The benchmark surface
from `optimization_log8.md` is copied into the dedicated `log11_physical`
benchmark target.

```text
write
  write_root_all_rows
  write_delta_10pct_updates
  write_tombstone_10pct_deletes

exact-read
  get_many_exact_keys
  get_many_missing_keys
  exists_many_exact_keys

scan/projection
  scan_keys_only
  scan_headers_only
  scan_full_rows
  prefix_scan_schema
  prefix_scan_schema_file_null

diff/changed-key
  changed_keys_update_10pct
  changed_keys_delta_chain_10x1pct

delta-chain materialization
  materialize_delta_chain_10x1pct

storage-size
  json_pointer_crud_storage
  log11_physical_tracked namespace byte accounting

layering/dependency direction
  commit_store should not depend on tracked_state

payload packing
  json_store.pack vs json_store.json threshold behavior
```

Missing later axes, if needed:

```text
multi-parent merge costs
commit graph traversal costs
membership_pack writes
direct commit_store.change_pack writes
tracked_state.tree.root / tracked_state.tree.chunk materialization
large JSON payload threshold sweeps
```
