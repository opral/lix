# No-JSON current-state acceptance gate

## Verdict

**BLOCK** for the native carrier at `0edb80ab9d091b697b455f694685414b42654ac9`
and for its key-order composition at
`792eeda59cea7c420d5f6abaeb3a0a312a08f484`.

This branch is test/report-only. It changes no production source. The gate is
candidate-parametric and must return `APPROVE` before a durable Schema-v1
carrier is accepted.

## Required authority predicates

1. Current Schema-v1 rows have one typed native tuple carrier. Declared
   `jsonb` is a scalar column only.
2. Dynamically registered Schema-v1 rows resolve an authenticated catalog
   `SchemaPlan` before lowering. The durable tuple carries the trusted plan
   identity and decode verifies that same plan. Missing or mismatched plans
   fail closed; there is no JSON fallback.
3. `lix_commit` and `lix_commit_edge` current SQL surfaces derive directly
   from authenticated commit topology. They must not pass through the removed
   whole-row JSON/`StateCell::Value` representation. Conversion at the final
   SQL/public boundary is not a second durable authority.
4. There is no whole-row `snapshot_content` JSON authority, serializer,
   deserializer, old JSON `StateCell`, legacy key codec, fallback reader, dual
   writer, or compatibility path.
5. State keys have one codec ordered schema -> typed PK -> owner/file in both
   directions.
6. Every built-in current-state schema uses exactly the seven canonical public
   types: text, uuid, int8, float8, boolean, jsonb, and timestamptz.
7. Every remaining production JSON use is assigned to history, plugin wire
   semantics, or a genuine declared-jsonb/public conversion owner. Unowned
   JSON fails the gate.

## Exact candidate results

| Candidate | Tree | Result | Findings |
|---|---|---:|---:|
| `91d059332bb00df0aaa4fad5babb6f7018175e25` | `49173b0580aa328f03e1417af0907a8dc7d1b2de` | BLOCK | 452 |
| `0edb80ab9d091b697b455f694685414b42654ac9` | `350633e52bb40ec6a37a8b76d685ce5afd6b3329` | BLOCK | 450 |
| `792eeda59cea7c420d5f6abaeb3a0a312a08f484` | `93fee1f254091e2e55345405deaa7c62a8637675` | BLOCK | 450 |

The concrete `0edb` and `792e` blockers are:

- `dynamic-schema-native-plan`: `packages/lix/src/native_row.rs` accepts a bare
  relational schema and `NativeRowCell` carries no trusted `SchemaPlan`
  identity. Decode therefore cannot prove the dynamically registered plan
  that authorized the bytes.
- `system-current-native-row`:
  `packages/lix/src/sql2/providers/entity.rs` still calls
  `commit_row_snapshot_json` for the current `lix_commit` projection.
- `carrier-authority`: the old string `StateCell::Value` and legacy current-row
  encoding remain alongside `NativeRow`.
- `no-compat-carrier` / `whole-row-json` / `json-owner`: old JSON carrier,
  whole-row materialization, and unowned JSON residues remain reachable.

Machine-readable inventories:

- `evidence/no-json-current-state-91d.json`
- `evidence/no-json-current-state-0edb.json`
- `evidence/no-json-current-state-792e.json`

## Gate evidence

- Deterministic source-gate fixtures: **7/7 PASS**. They prove rejection of a
  string state cell, wrong key order, unowned JSON, unknown schema type,
  untrusted dynamic plan, JSON-backed system StateCell, and JSON-backed
  `lix_commit` projection.
- `cargo check -p lix --lib --all-features --message-format short`: **PASS** on
  exact `91d` (217 inherited warnings, no errors).
- Public seven-type Schema-v1 smoke: **1/1 PASS** on exact `91d`.
- Candidate-parametric public semantic oracle: **3/3 PASS** on exact `91d`:
  Memory, RocksDB cold reopen, and SlateDB cold reopen. Each registers a
  scalar-only schema and a declared-jsonb schema dynamically, verifies both,
  and reads derived `lix_commit` topology.
- `cargo fmt --all -- --check` and `git diff --check`: **PASS**.
- The broad lib-test build on `91d` is not claimed: inherited stale
  `server_protocol` test diagnostics prevented a runnable lib-test binary.
  The production all-features lib check and the dedicated E2E binary are green.

## Exact commands

```sh
python3 scripts/acceptance/test_no_json_current_state_gate.py
python3 scripts/acceptance/no_json_current_state_gate.py \
  --root /candidate --expect-head HEAD --output evidence.json
cargo fmt --all -- --check
git diff --check
cargo check --workspace --all-targets --all-features
cargo test -p lix --test schema_v1_public_smoke --all-features -- --nocapture
cargo test -p lix --lib --all-features \
  immutable_objects_and_typed_state_codecs_fail_closed -- --nocapture
cargo test -p lix --lib --all-features \
  current_state_pack_round_trips_and_rejects_identity_substitution -- --nocapture
cargo test -p lix_e2e --test no_json_current_state_acceptance \
  --features 'storage-benches slatedb' -- --nocapture
```

The next carrier successor must make both named carrier predicates green in
addition to eliminating every compatibility/JSON authority finding. Runtime
CRUD alone is deliberately insufficient.
