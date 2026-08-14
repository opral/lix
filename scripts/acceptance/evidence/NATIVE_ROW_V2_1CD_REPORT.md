# NativeRow v2 candidate replay — 1cd96dac

## Identity

- Candidate ref: `origin/codex/schema-forktree-carrier-c9e-key-order-owner-domain-v2-91d`
- Candidate head: `1cd96dacfa89ee63e0296548cac85ce312df6147`
- Candidate tree: `046e124ce934a0f5600f02cdf03617f38a99e1b3`
- Candidate parent: `8fd62af4240e381170a14b578058eb5d4c3a9883`
- Acceptance package: `53179498c1d3d9436ed90eb2cb71e1f4cbc6b0d1`

## Verdict

**BLOCKED for terminal replay qualification; source/authority and O(1)
branch-create profile are approved.** No semantic authority defect was found.
The blocker is that the exact candidate's lib-test target does not compile, so
the required corruption, child/grandchild-root, mask/unmask/tombstone/re-add
and cold-reopen tests cannot be independently executed on this object.

## Source and authority

- `LIXFCV\0\x02` is the sole current-state magic. V1 occurs only in a negative
  test and is rejected by the v2 decoder.
- `NativeRowCell.global` is the stable Global/Local domain. The unfortunately
  retained field name `owner_digest` is not a branch-owner attestation: both
  its producer and verifier call `state_identity_digest(global, schema_key,
  typed EntityPk, file_id)`. No branch UUID is an input.
- Typed layout, canonical StateKey, semantic digest and typed body are checked.
  Current-pack validation binds `NativeRowCell.global` to the pack's root
  domain. Tree key, pack object/ordinal and history page/ordinal retain the
  authenticated chain.
- Current-state v2 encode and decode both reject `StateCell::Value` and
  `StateCell::Null`; tombstone and NativeRow are the only accepted current
  cells.
- `publish_new_branch_selector` copies `source_commit.local_state_root` and
  stages selector/topology/catalog objects only. It performs no state-row or
  current-pack rewrite. Reapplying this rule to a child implies identical
  source/child/grandchild local roots until an edit path-copies a root.
- Initial frozen gate output was a token-level false positive for
  `owner_digest`, the deliberate v1 rejection test, and the word `legacy` in
  unrelated code. Call-chain reconciliation clears all three.

## Branch-create profile

Temporary harness adaptation only; candidate worktree was restored clean.
The adaptation updated removed benchmark APIs and did not alter production.

| Backend | N | branch ms | settled byte delta | settled object delta |
|---|---:|---:|---:|---:|
| RocksDB | 1,000 | 2.819 | 36,640 | 2 |
| RocksDB | 10,000 | 2.482 | 38,464 | 2 |
| RocksDB | 50,000 | 3.425 | 43,509 | 2 |
| SlateDB | 1,000 | 2.928 | 21,002 | 6 |
| SlateDB | 10,000 | 3.315 | 22,906 | 6 |
| SlateDB | 50,000 | 3.901 | 27,972 | 6 |

Object delta is exactly constant with N on both adapters. Byte growth from 1K
to 50K is 6,869 bytes Rocks and 6,970 bytes Slate, while inherited state grows
50x. There are zero benchmark-observed state/current-pack row rewrites; the
source publication plan independently proves none can be staged. This supports
`O(1)` branch creation in inherited row count.

Raw profile log SHA-256:
`e5a659e88a7518f1d7b1c814673c67257fa64c483ba5a703a38e50146c603979`.

## Exact test blocker

Both `cargo test -p lix --lib --all-features ...` and the narrower default
feature lib-test target fail before running a test. First/default-target
diagnostics include:

- `session/undo_redo.rs:610`: `Value::Json(JsonValue::String(..))` type mismatch.
- `catalog/snapshot.rs:3491/3497`: removed `DefaultValuePlan::Timestamp` and
  `DefaultValuePlan::Cel` referenced by tests.
- `schema/tests.rs:94`: `open_session()` missing required branch argument.
- `sql2/providers/entity.rs:3290`: non-exhaustive `Timestamptz` match.

The all-features target additionally fails in unrelated `server_protocol`
tests (ambiguous `RemoteCapacityBackend::StagedWave` and unavailable
`tracing_subscriber`). The exact candidate therefore cannot supply terminal
runtime evidence for the required private controls despite production lib and
release benchmark compilation succeeding.

## Reachable JSON inventory

### A — current durable state

- SQL/provider `JsonValue` → transaction normalization/staging →
  `native_row::encode` → typed `BodyValue` sequence → `NativeRowCell` →
  `encode_current_state_packs` → `encode_current_state_value` v2.
- Whole-row JSON in this path is transient SQL normalization only. Its bytes do
  not persist as the current row envelope.
- Declared `jsonb` columns persist as the semantically required
  `BodyValue::Jsonb` cell inside the typed body. Scalar-only schemas bypass that
  cell kind.
- `StateCell::Value/Null` current ingress is fail-closed. No reachable current
  durable whole-row JSON writer/decoder remains.

### B — authenticated history/change and public boundaries

- `ChangeRecord.snapshot/metadata` use `JsonSlot` and are encoded in the
  authenticated `ChangeObjectV1` semantic payload; `change.snapshot_content`
  is history semantics. This is the explicitly deferred native-history cut,
  not current-state authority.
- `authenticated_current_cell_for_history` compares history JSON's semantic
  digest with the NativeRow digest. `logical_history_cell` constructs
  `StateCell::Value/Null` only for historical projections.
- Plugin manifests, layouts, guest payloads and registry projections use JSON
  as required public/plugin wire semantics. Historical plugin registry reads
  still consume history JSON and explicitly reject an ownerless NativeRow.
- File/directory planner, path-index and visibility code parse logical snapshots
  produced after authenticated NativeRow/history decode. These are transient
  public/filesystem DTO boundaries, not durable current-row encoding.
- SQL DataFusion/Arrow conversion, predicates, errors, result metadata and
  public history tables use transient JSON projection. They are semantically
  required API boundaries.

### C — test/dead vocabulary

- Remaining direct constructors of `StateCell::Value/Null` in ForkTree,
  provider, staging and validation test modules are fixtures for history,
  rejection and overlay behavior.
- Unused plugin layout/CEL helpers reported by the compiler are dead compiled
  vocabulary, not a current durable authority.

No reachable A-class JSON authority defect was found.
