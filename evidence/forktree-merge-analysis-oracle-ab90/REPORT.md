# TrackedState merge-analysis migration oracle

Status: TEST/REPORT ONLY. This package contains no production change, no
adapter change, no benchmark result, and no compatibility implementation. It
freezes the source RED and a pure semantic oracle for the next compiler-driven
merge-analysis migration.

## Immutable provenance

The source under test is exact `ab90fc51e148611f5fdacde173dd6789ab22ab88`
(tree `5bcf259918f86e5b439c1bc50a3e198f87826adc`), parent
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` (tree
`820fe560da3bbd2b00b788b0b1759c409048cd6e`). The production source diff is
limited to:

* `packages/lix/src/live_state/forktree_reader.rs`
* `packages/lix/src/sql2/entity_batch.rs`
* `packages/lix/src/sql2/providers/entity.rs`

The parent-to-head full-index binary diff SHA-256 is
`d99895bd6f522164134047caf4d0783185ee9cf51a4781d58c8ae14a020d2a32`; stable
patch ID is `011bdf4c46feee356b8657cfd2074cefa074777f`.

The historical fail-closed prerequisite is bound by immutable evidence, not
by a production dependency: oracle `448624a557bca2c341f4a1820b79222a5691613a`
(tree `b618b0f60e614d76c1afdb04280807d197bda8a2`) and its report-only SQL
binding `97a7116d00206954b581cf43937cc5db6c23f30b` (tree
`457a3919903169ca1edd2fe81df8b81e70b06d37`, parent 448). The 97a
parent-to-head full-index diff is
`08fee7a84860b27836468f63eff9f6c000538c08947820a26bfbba1e54328cdf` and
patch ID `497130c0f7744d7e42a7cb0866bb044f8c9f1209`.

Run the immutable source calibration from this worktree:

```text
bash evidence/forktree-merge-analysis-oracle-ab90/source_verifier.sh --expect-red
```

It invokes the self-contained prerequisite verifier and confirms both exact
trees before checking the call graph. It intentionally exits zero with an
EXPECTED RED result.

## Source result: RED

The merge path has two historical authorities over one opening read:

1. `packages/lix/src/session/merge/branch.rs:166-190` and `:285-317` obtain
   merge base from `commit_graph_reader_on_opening_read`, then invoke
   `with_opening_tracked_reader`.
2. `packages/lix/src/session/merge/analysis.rs:46-111` still accepts
   `&mut TrackedStateStoreReader<S>`. It calls `diff_commits` for source and
   target (`:57-68`), loads `load_change_payloads` for equality fallback
   (`:80-88`), and sends the result to the tracked-state `plan_merge`.
3. The same branch code separately constructs a
   `ForkTreeReadFacade` (`branch.rs:173`, `:295`) for derived file conflicts,
   historical state rows, and plugin registry metadata. Those paths use
   `load_state_rows_at_commit` and `load_plugin_registry_at_commit` against
   the facade (`branch.rs:590-597`, `:622-634`, `:922-930`,
   `:1043-1053`).

`transaction/context.rs:832-837` proves that the facade can retain the
transaction's `opening_read` without opening a second view. However,
`transaction/context.rs:7407-7415` still constructs a legacy tracked reader
for merge analysis. Therefore the current code has one physical opening read
but two semantic reader/facade paths; it has not established one ForkTree
merge-analysis owner.

The inherited prerequisite remains RED. In the exact 413 source,
`forktree/serving.rs` returns `Ok(None)` when the CommitCatalog entry is
missing, while `forktree/view.rs` maps that result into the historical row
vector. A valid absent key and a missing commit/root can therefore collapse to
the same result. The prerequisite script checks these exact source facts and
does not reinterpret them as valid absence.

### Authority and call map

| Responsibility | Current path on ab90 | Required landing owner | Required invariant |
| --- | --- | --- | --- |
| Commit chronology / merge base | `branch.rs` → `CommitGraphReader` over opening read | typed ForkTree graph view | CommitObject parent list is sole chronology; first-parent floor and checkpoint bridge are preserved; recovery markers are not chronology |
| Target/source/base historical state | `branch.rs` → `analysis.rs` → `TrackedStateStoreReader::diff_commits` | ForkTree merge view over the retained read | authenticated commit, root, key scope, and typed absent/null/tombstone/value distinction |
| Payload equality fallback | `analysis.rs` → tracked reader `load_change_payloads` | ForkTree commit-member/change owner, or a pure algorithm fed by authenticated records | no global legacy scan, fallback reader, cache, or second durable owner |
| Three-way merge semantics | `tracked_state/merge.rs::plan_merge` and `merge_payload_fallback_ids` | pure semantic primitives under the ForkTree owner, or deleted after equivalent in-owner implementation | disjoint changes merge; same identity/value disagreement conflicts exactly |
| Plugin registry metadata | `branch.rs` → `ForkTreeReadFacade` → `load_plugin_registry_at_commit` | existing semantic registry owner, called on the same retained view | missing/malformed/wrong-generation registry fails closed; no registry cache or duplicate authority |
| File/derived conflict rows | `branch.rs` → facade `load_state_rows_at_commit` | existing ForkTree historical state owner | same parsed-file/file-history identity and BlobId semantics; no empty-success on missing roots |
| Stale publication | transaction opening head and atomic preconditions in `context.rs:839-920`, publication around `:1593-1797` | existing transaction publication owner | head change rejects publication; analysis must never refresh or bypass the opening view |
| Read lifetime | `context.rs:825-837` facade plus graph reader on opening read | one `ForkTreeReadFacade`/typed merge view | all merge-base, historical scans, registry reads, and conflict inputs share one retained coherent read |

The graph reader currently uses the same opening read object, which is a
lifetime property, not yet a single semantic authority. The future owner must
move the graph query into the typed ForkTree merge view or prove the graph
reader is an internal subcomponent with no alternate chronology input. It must
not consult `CheckpointRecoveryRef`, reachability queues, branch-control
serving context, or a rebuilt legacy scan.

## Pure oracle contract

`model.rs` is a standalone Rust test model. It does not import Lix or write a
repository. It covers the following discriminators:

* disjoint target/source edits produce a merged snapshot;
* same-identity divergent edits produce exactly one conflict;
* `Null`, `Tombstone`, `Value`, and authenticated absence remain distinct;
* a branch bridge has ordinary commit parents `[H, C]`, generation above both,
  and merge base `C`, while the later target `T` is not used as a parent or
  chronology source;
* common plugin registry generation/schema metadata is accepted and a missing
  or mismatched registry fails closed;
* missing CommitCatalog, missing root, wrong-kind root, and malformed root are
  corruption, while a valid absent key is absence;
* stale target publication rejects a changed expected head and accepts only an
  equal head;
* cold-reopen use retains exactly one read identity and rejects another read,
  fallback read, or cache read.

The model is deliberately not a production semantic claim. The future adapter
tests below are the qualification gates that must connect each model case to
authenticated persisted objects.

## Exact correction boundary

The next production slice may touch only the following paths, and only as
needed to move the existing semantic owner and delete the superseded reader:

1. `packages/lix/src/session/merge/analysis.rs`: replace the
   `TrackedStateStoreReader` parameter/import and its historical diff/payload
   calls with a typed ForkTree merge-view operation. Preserve
   `MergeAnalysis`, marker filtering, stats, and public conflict semantics.
2. `packages/lix/src/session/merge/branch.rs`: pass one retained typed view to
   merge-base, analysis, historical rows, and plugin registry loading. Remove
   `with_opening_tracked_reader` calls after the compiler proves no remaining
   merge caller; do not alter stale-publication or commit publication ownership.
3. `packages/lix/src/forktree/view.rs`: add only the typed merge-analysis
   surface needed to authenticate graph/topology, historical state, and
   commit-member/payload inputs against its existing `read`.
4. `packages/lix/src/forktree/serving.rs`: make the historical commit/root
   distinction fail closed and expose only typed validated records to the
   view. No raw `Stored*` leakage, fallback, or permissive `None`.
5. `packages/lix/src/tracked_state/diff.rs` and `merge.rs`: only move or
   retain pure semantic comparison/planning helpers. Any reader-bound wrapper
   or duplicate payload authority is deleted in the same wave.
6. `packages/lix/src/tracked_state/context.rs` and
   `packages/lix/src/transaction/context.rs`: only the compiler-driven
   removal of merge-only reader wrappers and their now-unused imports/callers;
   preserve unrelated unique transaction/SQL responsibilities and the
   opening-read publication fence.

No adapter, storage space, persisted format, selector, checkpoint/recovery,
GC, CAS, cache, compatibility reader, migration, or second writer is in
scope. The plugin registry remains its existing semantic owner; this slice
does not move or duplicate it.

## Acceptance gates

The future correction is acceptable only if all cases pass in order, with the
same test body and only the adapter feature changed:

1. **Merge base and chronology.** Read target/source heads from the retained
   selector. Authenticate every commit and parent. Prove a branch created from
   historical `H` with serving checkpoint `C` uses ordinary graph parents
   `[H, C]` on its first ordinary commit, generation greater than both, and
   merge base `C`; never use later target `T`, recovery-ref rows, or queue state
   as merge chronology. Verify first-parent/checkpoint floors and 65 rotations.
2. **Historical state scans.** Scan base, target, and source roots through one
   view. A valid commit+root+absent key is authenticated absence; missing
   CommitCatalog, missing root, wrong-kind root, malformed catalog/root, or
   incomplete member closure is an error. `Null`, `Tombstone`, and `Value`
   remain separate.
3. **Merge semantics.** Disjoint identities merge without conflict. Divergent
   same-identity values conflict once with stable identity. Plugin registry
   metadata is common and authenticated across base/target/source; missing,
   malformed, wrong-kind, or mismatched metadata fails closed.
4. **Publication race.** Change the target branch after analysis and before
   publication. Existing transaction CAS/preconditions reject stale
   publication; no retry opens another read and no stale result is published.
5. **Reopen and retention.** Flush, drop, and cold reopen on a retained
   repository. Repeat merge-base, historical scans, plugin metadata, and
   checkpoint-floor assertions. Delete a branch only after its graph/control
   owner is gone and verify the existing GC/reachability owner retains or
   releases exactly the authenticated roots; the merge reader must not create
   a parallel retention fact.
6. **One-read/residue.** Instrument the adapter or test facade to count read
   identities. Every merge operation has one coherent read, zero refreshes,
   zero fallback/cache reads, and no second historical reader. Source residue
   must show no `TrackedStateStoreReader` in `session/merge/analysis.rs`, no
   merge call to `with_opening_tracked_reader`, and no legacy raw getter or
   compatibility export.

The final adapter tests must include both the successful and failure rows, not
only an aggregate “merge failed” assertion, so a missing persisted object
cannot be mistaken for a legitimate absent key.

## Future adapter commands

These are qualification commands, not run by this report package. Each cell
is single-threaded, uses an isolated target/TMP directory, and is capped at
20 minutes. The test name is the required future harness contract.

```text
# Pure model, no Lix build
rustc --edition 2021 --test \
  evidence/forktree-merge-analysis-oracle-ab90/model.rs \
  -o /root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/model-test
/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/model-test --nocapture

# Current source calibration (expected RED on ab90)
bash evidence/forktree-merge-analysis-oracle-ab90/source_verifier.sh --expect-red

# A corrected candidate must add a successor verifier whose positive residue
# checks replace this expected-RED calibration; this package does not pretend
# that ab90 passes its own migration gate.

# Memory
timeout 20m env CARGO_BUILD_JOBS=1 \
  CARGO_TARGET_DIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/target-memory \
  TMPDIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/tmp-memory \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_migration \
  --features storage-benches \
  merge_analysis_migration_memory -- --exact --nocapture --test-threads=1

# RocksDB
timeout 20m env CARGO_BUILD_JOBS=1 \
  CARGO_TARGET_DIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/target-rocks \
  TMPDIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/tmp-rocks \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_migration \
  --features storage-benches,rocksdb \
  merge_analysis_migration_rocksdb -- --exact --nocapture --test-threads=1

# SlateDB
timeout 20m env CARGO_BUILD_JOBS=1 \
  CARGO_TARGET_DIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/target-slate \
  TMPDIR=/root/repos/lix-evidence/forktree-merge-analysis-oracle-ab90/tmp-slate \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_migration \
  --features storage-benches,slatedb \
  merge_analysis_migration_slatedb -- --exact --nocapture --test-threads=1
```

The Memory, RocksDB, and SlateDB tests must use the identical deterministic
history and print: read/view identity; commit IDs, parent arrays, generation,
merge base and checkpoint floor; target/source/base row digests; plugin
registry fingerprints; conflict identities; stale-publication result; each
corruption/absence result; cold-reopen result; and read/fallback/cache counts.
No broad scale or performance matrix is part of this assignment.

## Deletion residue and stop rule

The compiler-driven cut is complete only when the following residue is gone
or is independently proven pure/unique:

* `analysis.rs` no longer imports or accepts `TrackedStateStoreReader`, calls
  `diff_commits` on it, or loads payloads through it.
* `branch.rs` no longer invokes `with_opening_tracked_reader` for merge
  analysis. Its ForkTree facade calls remain the sole historical/plugin path.
* `transaction/context.rs` removes `with_opening_tracked_reader` only after
  `rg` and compiler diagnostics show no other caller; it must retain
  `opening_read`, stale-head checks, and atomic publication preconditions.
* Reader-bound `tracked_state/context.rs` methods (`diff_commits`,
  `load_change_payloads`, and test-only `plan_merge`) are deleted or moved
  under the ForkTree merge owner in the same cut. Pure `tracked_state/diff.rs`
  and `merge.rs` algorithms may remain only if they have no legacy storage
  reader, fallback, cache, or alternative authority.
* No reexport, adapter wrapper, compatibility reader, empty-success branch,
  recovery/queue chronology lookup, or second registry/root/commit authority
  is introduced.

If any required historical input is missing or malformed and the candidate
returns an empty result, it is a BLOCKER. If a candidate needs a synthetic
anchor, persisted index, new selector, or second read to pass, stop and
reject it as outside this slice.
