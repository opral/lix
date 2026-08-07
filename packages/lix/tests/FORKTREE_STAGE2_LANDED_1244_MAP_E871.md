# Landed #1244 to ForkTree Stage-2 deletion map

Status: plan/test evidence only. Exact source range:
`b5e78190f49cab5de7bb19b6f967706c214363b6..e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7`.
Current tree: `ce241a0af016cadcb0c21d2d754eb3d4291cf79c`.
Canonical full-index binary diff SHA-256:
`43831571eb93e573a201ea11e8d14263423bc8b07bfe5690f7b6b1109b6c6f7c`.
Stable patch ID: `7475b4618a333d1e76c2dbb201a15031150e4d9d`.

#1244 adds no durable space. It hard-cuts the existing branch-control and
plugin-checkpoint encodings and advances the old-layout repository protocol to
V61. Stage2 preserves the public fail-closed behavior but deletes every one of
those physical encodings before its first runnable compile.

## Exact changed-path disposition

| Path | Stage2 disposition |
|---|---|
| `packages/lix/src/branch/control.rs` | Delete file and `BRANCH_HEAD_CONTROL_SPACE`. `BranchSelectorV1`/`BranchSnapshotV1` authenticate branch identity, selected roots and ref-change edge. A present malformed/hash/domain/binding mismatch fails before serving. Delete `branch.head_control.v10`, `LBC1`, digest context and raw exact-token codec. |
| `packages/lix/src/transaction/plugin_checkpoint.rs` | Delete file and `PLUGIN_CHECKPOINT_SPACE`. Represent the current derived checkpoint, when retained, as one typed immutable object edge from the selected plugin/materialization state, bound to branch ID, file ID, generation, blob hash and semantic root. Delete `plugin.current_checkpoint.v2`, `LPC3` and digest codec. |
| `packages/lix/src/transaction/context.rs` | Preserve error propagation: a present corrupt selected checkpoint fails the public actor operation. Only a completely authenticated generation/blob/root mismatch is a cache miss. Consume the typed object edge; never swallow corruption or fall back to old bytes. |
| `packages/lix/src/init.rs` | Keep one ForkTree hard-cut repository marker in `REPOSITORY_PROTOCOL_SPACE`; delete and reject `immutable-physical-commit-state.v61`. No migration or dual decoder. |
| `packages/lix/src/storage_bench.rs` | Replace physical corruption helpers with typed selector/object corruption hooks, then remove v10/v2 names and old-space inventory. Preserve RocksDB/SlateDB cold-reopen assertions. |
| `packages/rs-sdk-tests/tests/e2e.rs` | Preserve public plugin actor corruption rejection and branch lifecycle behavior through typed owner tests; remove direct old-space corruption and names. |
| `packages/server-protocol/src/lib.rs` | Preserve current public session/wire behavior. Replace the test gate that names `branch.head_control.v10` with an owner-level publication/read gate; server code must not know ForkTree physical spaces/codecs. |
| `packages/engine-benchmarks/tests/corruption_recovery_qualification.rs` | Re-express branch selector, plugin checkpoint object, selected state/object/chunk corruption through the Stage2 benchmark hook; remove old names and raw-space reconstruction. |
| `packages/engine-benchmarks/examples/storage_layout.rs` | Update only to the new non-authoritative accounting surface; no old-space catalog. |
| `packages/engine-benchmarks/Cargo.toml`, `Cargo.lock` | Test dependency additions are not authority. Reconcile only after the production hard cut compiles. |

## Preserved semantic oracle

1. Branch selector bytes are domain-separated and bound to repository/branch;
   malformed, truncated, substituted or digest-mismatched selected bytes fail
   open/point/range/branch operations before output.
2. A selected plugin checkpoint object authenticates owner IDs, generation,
   blob identity, semantic root, lengths, runtime and authority bytes.
3. Present corrupt checkpoint data is an error, never a cache miss. An
   authenticated selection mismatch may miss and instantiate normally.
4. Branch deletion removes the selected checkpoint edge atomically; shared
   immutable objects survive until their final selected/retained edge releases.
5. Flush/drop/cold reopen produces the same result on RocksDB and SlateDB.
6. The repository protocol is a hard cut: V61 old-layout bytes are rejected,
   never decoded or migrated.

The Stage2 residue oracle names the old space constants, modules, v10/v2
names, magic/domain strings, V61 marker and benchmark bridges. Zero findings is
required before C1.
