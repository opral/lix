# fd2 correction acceptance contract

## Immutable anchor

```text
base/head: cd91b9b90f7f468158b4df154adbed9551eb5d60..fd2be256d763f17e9f127d4c984e36fba191cb82
head tree: 20110ca5e3c33d34217630fff0a2b784b545317a
base..head full-index binary SHA-256: 1a410542cff54e3b1c83a5cfb2cdea568dc9f1f71fc0c3f8598e8936d944a277
base..head stable patch ID: c275ab15f3306c503e6830afee2a66bacf1fb974
```

The production head is not modified by this package. A candidate package
successor must retain fd2 as an ancestor and may change only this report
directory.

## Caller-owned reader proof

For each of SQL checkpoint, filesystem working diff, and ordinary working diff:

- locate the balanced tuple passed to `scan_row_source`;
- resolve the tuple element containing `self.forktree_reader.clone()`;
- resolve the destructured closure parameter at that same tuple index;
- require every chronology receiver (`checkpoint_history_from_head` or
  `latest_checkpoint_for_branch`) to be that parameter;
- require filesystem `load_rows`'s first reader argument to be that parameter;
- reject local `ForkTreeReadFacade::new`, `begin_read`, `query_source.store`,
  tracked-state/branch-control readers, compatibility/fallback/cache/second
  authority tokens in the scoped consumer functions.

The negative fixtures distinguish a different tuple reader, an independently
constructed reader, a fresh read, a wrong chronology receiver, and a legacy
tracked-state route. A field-name mention without balanced tuple/parameter
identity is insufficient.

## Plugin registry proof

`file_history_owner_schema_keys` must obtain the authenticated registry entry
with `state.plugin_registry.get(owner.plugin_key())` and fail closed when it is
absent. The scope must contain no `unwrap_or`, `unwrap_or_else`,
`owner.schema_keys()`, compatibility fallback, cache, or alternate authority.
The executable `fixtures/registry_model.py` mutates one valid authenticated
fixture for every negative case and rejects each mutation; it separately
asserts that a present authenticated empty registry is valid.

The registry model cases are:

| Case | Required result |
| --- | --- |
| authenticated valid entry | `VALID` |
| authenticated present-empty registry | `VALID_EMPTY` |
| missing registry | `FAIL_CLOSED_MISSING` |
| wrong-kind registry row | `FAIL_CLOSED_WRONG_KIND` |
| malformed registry payload | `FAIL_CLOSED_MALFORMED` |
| substituted registry identity | `FAIL_CLOSED_SUBSTITUTED` |

The model is an executable discriminator for the source contract; it is not a
production reader or runtime acceptance test.

## Stop conditions

Any identity mismatch, fallback/compatibility route, registry substitution,
new cache/authority, scope widening, incomplete manifest, or nonzero
`rustfmt --check`/`git diff --check` result is RED. No compiler or adapter
qualification is allowed before this source gate is GREEN and independently
reviewed.
