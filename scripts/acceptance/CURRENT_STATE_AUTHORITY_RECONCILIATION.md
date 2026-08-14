# Current-state authority reconciliation

## Terminal verdict

**SOURCE/AUTHORITY APPROVE** for exact candidate
`c9e97a1724c29d5ace366410d5271c0e9926cdd8`, tree
`87e3024d549308b2878be19d5ad5799072887d3d`.

This is a reachability verdict, not a token-presence verdict. The candidate has
one current durable Schema-v1 writer and decoder. Remaining JSON and
`StateCell::Value`/`Null` vocabulary is confined to authenticated history,
plugin/public projection, explicit fail-closed guards, or tests.

The independently found global-row branch-owner mismatch in transaction
validation is real but separate: it is a `StateRowSource::Global` propagation
defect and does not introduce JSON or a second durable carrier.

## A — reachable current durable authority

### Writer

```text
RawWriteBatch
  -> normalize_raw_write_row_in_place
  -> TransactionCatalog::plan_for_key
  -> native_row::encode
  -> PreparedStateRow.native_row
  -> staged_cell / transaction commit StateCell::NativeRow
  -> encode_current_state_packs
  -> encode_current_state_value(tag 3)
  -> authenticated ForkTree current-pack object
```

Bootstrap follows the same `NativeRow -> current pack` path. The only
production current-pack encoder callers are `forktree/bootstrap.rs`,
`forktree/current_pack.rs` itself, and `transaction/commit.rs`.

`encode_current_state_value` rejects `StateCell::Value` and `StateCell::Null`.
Live rows can therefore persist only as `NativeRow`; deletion persists as a
tombstone.

### Decoder

```text
authenticated ForkTree current-pack object
  -> decode_current_state_value
  -> tag 3 NativeRow (tag 2 tombstone)
  -> authenticated_current_cell_for_history semantic-digest comparison
  -> native_row::decode
  -> layout, branch, owner and typed-body validation
```

Tags 0 and 1 fail closed as the removed JSON and whole-row-null encodings.
There is no fallback decoder. The retained `LIXFCV\0\x01` magic is one envelope
marker, not a compatibility route: old payload tags are rejected.

### SchemaPlan identity concern

The durable row does not need to persist a forgeable `SchemaPlan` object ID.
Equivalent authentication is complete:

1. normalization resolves the trusted transaction catalog plan before native
   lowering;
2. the authenticated StateKey supplies the schema and typed PK;
3. `layout_id` binds schema key, ordered columns, scalar types, nullability and
   PK layout;
4. `owner_digest` binds branch, schema, typed PK and file owner;
5. `semantic_digest` is checked against the authenticated ChangeRecord; and
6. the body is decoded by the one fail-closed typed value-layout codec.

An owner, layout, schema, PK, file, body or same-size semantic substitution
therefore fails. Defaults and constraints intentionally are not physical
layout identity; they are applied by the trusted plan before encoding.

## B — explicitly deferred JSON boundaries

- **History:** authenticated `ChangeRecord::snapshot` remains a `JsonSlot`.
  `logical_history_cell` may materialize `StateCell::Value`/`Null` for
  historical row, merge and blob consumers. This value cannot enter a current
  pack because the current encoder rejects it.
- **Plugin:** historical plugin registry and plugin wire payloads retain JSON
  semantics by contract.
- **Public/system projection:** `lix_commit` and `lix_commit_edge` originate
  from authenticated commit topology. `commit_row_snapshot_json` currently
  creates a transient public projection value; `commit_projection_row`
  immediately native-encodes the in-memory row for SQL/Arrow consumption and
  never calls the current-pack writer. This is an avoidable projection copy,
  not durable whole-row JSON or a second authority.
- **Declared `jsonb`:** JSON semantics remain only inside columns whose schema
  type is `jsonb`; ordinary scalar columns use native typed slots.

## C — dead/test/rejection vocabulary

- `StateCellRef` and direct `StateCell::Value`/`Null` constructors in
  `forktree/tests.rs` and trailing `cfg(test)` modules are fixtures.
- Production matches on removed current cells are corruption/rejection arms.
  Their presence is necessary to fail closed and is not evidence of a writer.

## Candidate-parametric gate

```sh
python3 scripts/acceptance/test_reconcile_current_state_authority.py
python3 scripts/acceptance/reconcile_current_state_authority.py \
  --root /candidate \
  --expect-head HEAD \
  --output /tmp/current-state-authority.json
```

On exact `c9e`, helper tests are **2/2 PASS** and all **13/13** authority
predicates pass. The gate checks the reachable writer/decoder call shapes,
closed current-pack writer set, removed-tag rejection, topology-derived commit
projection, and equivalent plan/layout/owner/semantic binding.

The gate is production-read-only. This branch adds only the verifier, its unit
tests, and this report. It is ready to run unchanged against Ryzen III's
corrected immutable composition.
