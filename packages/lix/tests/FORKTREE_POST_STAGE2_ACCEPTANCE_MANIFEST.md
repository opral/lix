# ForkTree post-Stage2 acceptance manifest

## Status and provenance

This is a test-only execution package. It does not authorize or implement the
Stage2 cut and it changes no production, SQL, storage, adapter, or ForkTree
owner source.

- Exact control/main: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`
- Control tree: `9a705d36392e88d8f5f363b2b23d373deec3321d`
- Landed cursor contract: `StorageRead::begin_scan -> ScanCursor::next_page`
- Approved unwired Stage1: `138b55e1de90806c380ad27b2b349f4c66a1387f`
- Typed Stage1 application oracle: `5a6a2cb037668c8dc6256d9b0975d0b39068f07a`
- Public version-control harness: `ae3b9bf13676a79e01b25e5d1a2cc624517326e9`
- DataFusion range/projection harness: `2a0e8512bb37c9da2050c99c366e5ac05bb01553`
- Corrected no-lease model successor: `ee402a098a991f7e91eb9c62e2cefe960f8e547e`
- Bounded-GC oracle: `73f191fbb960bdb9bb647f63dc909fba606a5c40`
- 64 MiB family oracle: `c2042c0e447950e261a2ca8674e49549acca8078`
- Native multimedia-shape oracle: `d8ddc071cc4ef05874df947787f2212812dd2564`

The no-lease source is listed as a required post-cut oracle, not as a launch
approval. Its independent approval remains a separate launch gate.

## Harness reuse rule

No benchmark algorithm is copied into this package. The runner expects a
disposable control checkout and a disposable candidate checkout carrying the
same test-only harness sources. Production integrations may provide only the
minimal test bridge needed to reach the existing public or sealed typed owner
API. They may not add a model row overlay, a direct SQL mutation loop, a raw
object-space escape, a legacy scan wrapper, or a second root.

The rejected `forktree_direct` SQL comparator is forbidden. SQL acceptance
must call the existing Lix binder, `SessionContext::execute_batch`, statement
savepoints, `SqlWriteExecutionContext`, `RETURNING`/`ON CONFLICT`, and the
concrete transaction commit boundary. A candidate without that test-only
physical-target bridge is not runnable and cannot be performance-qualified.

### Permitted test-only wiring deltas

- The typed application test remains byte-identical; the sealed owner keeps
  its typed `storage_bench` entry point while raw descriptors and sweep
  construction remain inaccessible.
- The public version-control harness keeps its fixtures, public calls, result
  digest, and accounting. Only its obsolete scan syntax may be mechanically
  ported to the landed `begin_scan`/`next_page` API; no `ScanPlan`, resume
  wrapper, cache, or alternate reader may be introduced.
- The DataFusion harness keeps its query fixtures, provider plans, result
  digests, projection set, and counters. Its iterator binds one live
  `StorageRead` and uses the same landed cursor API.
- The multimedia harness keeps the frozen payload generators, edit offsets,
  external BLAKE3/SHA-256 oracle, and lifecycle assertions. Legacy CAS-space
  accounting is replaced only by the ForkTree owner's typed object/chunk
  accounting facade; raw space access is forbidden.
- The SQL harness adds only the cfg-only physical-target selection described
  in the accepted #1260 implementer contract. Public parsing, planning,
  transaction orchestration, and result production are unchanged.

## Focused admission sequence

Every build or runtime cell is independently wrapped in `timeout 20m`. A
timed-out cell is a rejection, not a partial pass. Fixture creation is outside
timed operation samples but remains inside the cell cap.

1. Static/source identity and warnings-denied builds.
2. Typed owner Memory oracle, then RocksDB and SlateDB application oracles.
3. Public 1K SQL and version-control gates on RocksDB, then SlateDB.
4. Corrected no-lease crash/recovery and bounded-GC conformance.
5. Honest 10K DataFusion range/projection gate. The 50K gate is allowed only
   when both 10K adapters pass correctness and critical-resource thresholds.
6. 64 MiB image/audio/archive/video lifecycle gates. The 512 MiB generic
   authenticated-blob scale cell is allowed only after both 64 MiB adapters
   pass. Shape-specific audio/archive/video mutations remain at their frozen
   honest sizes (16/32/64 MiB); they are not relabeled as 512 MiB evidence.

There is deliberately no command that launches the whole matrix.

## Correctness requirements

- Exact ordered rows, SQL result metadata, statement labels/indexes, NULL,
  tombstone, rollback, `RETURNING`, `ON CONFLICT`, and stale-writer outcomes.
- One coherent selector-pair view. Local value wins, local tombstone suppresses
  global, and local absence falls through to global.
- One raw-UUID CommitCatalog and one unified ChangeCatalog. Equal current rows
  do not imply an empty historical diff: the selected Change/Commit identity
  count and digest must match the control.
- Exact branch, diff, merge, history, undo/redo, checkpoint, recovery, reopen,
  retention, and final-release results.
- Upload completion moves receipt reachability to file reachability atomically;
  abort and final reference release reclaim only dead objects.
- Shared chunks and retained roots survive. Final dead logical object/byte
  reclamation is 100%; physical LSM tombstone/compaction lag is reported
  separately.
- Publication-first rejects stale GC. A committed deletion page rotates the
  authoritative progress fence and rejects a stale publisher; retry restages
  missing immutable bytes and survives cold reopen.
- Cursor cancellation, malformed page, backend error, or decode error poisons
  the live cursor. A fresh view restarts only at
  `Excluded(last_authenticated_delivered_key)`. No persisted reader lease is
  introduced.
- Every visited object, child edge, owner/back-edge, size, domain, hash, and
  chronology is authenticated before dependent output. Corruption fails
  closed.
- No old `StorageRead::scan`, `ScanOptions`, `ScanPlan`, `ScanPlanCursor`,
  resume cache, SQLite route, tracked/changelog/branch-control/working-diff,
  legacy upload/CAS/GC authority, compatibility reader, migration, or dual
  writer is reachable.

## Performance and resource thresholds

Control and candidate use the same compiler profile, harness bytes, fixture
seed, adapter, host, and repetition count. Report medians and raw samples.

Required per timed phase:

- wall latency and process CPU;
- allocated bytes and allocation calls;
- cold and peak RSS;
- logical begin-read/write, point/batch/scan calls, keys/rows, read bytes,
  puts/deletes/commits, and written bytes;
- SlateDB physical objects/bytes/requests and RocksDB exposed counters;
- authenticated objects/nodes/chunks read and written;
- logical and post-flush/post-close/settled-compaction disk bytes;
- object/chunk sharing, amplification, retained/reclaimed objects and bytes.

Acceptance thresholds:

- no correctness, authentication, crash, reopen, or ownership regression;
- no unaccepted critical wall/CPU/allocation/RSS/backend-work/disk regression
  above 5% against exact `a12`;
- at least one meaningful resource or latency improvement above 10% before
  widening beyond the focused gate;
- zero query writes for OLAP;
- production DataFusion wall/resource regression at most 5%. The accepted
  model-only Slate 50K constant read tradeoff (6 objects versus 5, joins 12
  versus 10) remains disclosed and does not waive this production threshold;
- O(1) branch/checkpoint/undo selector movement, point O(log_F N), ordered
  range O(log_F N + output), sparse diff/merge proportional to visited changed
  paths plus output/conflicts, and bounded cursor/page memory;
- blob ingest/read/edit remains streaming with O(chunk/window) owner memory,
  no full-payload arena, fixed 1 MiB leaves/F64/Q8, exact external bytes, and
  no second chunker or locator authority;
- native frozen multimedia shape assertions and at least 93.75% unchanged-byte
  reuse for localized accepted shapes. The known fixed-block video prefix
  insertion exception must be reported rather than hidden;
- 512 MiB is gated by 64 MiB and must remain below the 20-minute cell cap.

RocksDB post-flush tombstone retention is not called live-data amplification:
report both immediate disk and explicit settled compaction. The frozen 1K
history evidence identified only an 8.218% perfect-elimination ceiling and an
88.742% compacted win, so no geometry redesign is admitted from that term.

## Exact focused commands

Use the companion runner. It is dry-run by default.

```text
packages/lix/tests/forktree_post_stage2_acceptance/run.sh list
packages/lix/tests/forktree_post_stage2_acceptance/run.sh verify
EXECUTE=1 packages/lix/tests/forktree_post_stage2_acceptance/run.sh run <cell>
```

The smallest required cells are:

```text
build-core
owner-memory
owner-rocks
owner-slate
sql-core
sql-rocks
sql-slate
vc-build
vc-rocks-1k
vc-slate-1k
vc-corrupt-rocks
vc-corrupt-slate
recovery-build
recovery-rocks
recovery-slate
recovery-adversarial
bounded-gc-conformance
olap-build
olap-rocks-10k
olap-slate-10k
media-build
media-image-rocks-64
media-image-slate-64
media-audio-rocks-64
media-audio-slate-64
media-archive-rocks-64
media-archive-slate-64
media-video-rocks-64
media-video-slate-64
blob-rocks-64
blob-slate-64
blob-rocks-512
blob-slate-512
```

The SQL adapter tests exercise the current public owner. The first runnable
candidate must additionally expose the same tests through the cfg-only
ForkTree physical-target swap described above; otherwise `sql-rocks` and
`sql-slate` are correctness controls only and the candidate remains blocked.

For exact A/B, invoke each admitted cell once with `CHECKOUT` and
`CARGO_TARGET_DIR` pointing at an exact-a12 disposable control overlay and
once with them pointing at the first runnable Stage2 disposable candidate
overlay. Harness fixture/digest code must be byte-identical between overlays;
only the permitted wiring deltas above may differ.

## Prior frozen evidence (routing, not substituted results)

- Typed Stage1 dual-adapter application result:
  `ffa367b9844051c952bfbee0d067cdde5cefe8a2125f9845152600d1694a70cc`.
- Public Stage2 acceptance report:
  `a4a83731d429017b44edf094156fe14e79a53fbe4151339f3acc8a1d9a34edce`;
  manifest `6619663d4532f9f2700dfe821f3b92d0056658073584aff187b6a48516b2514d`.
- Honest DataFusion package manifest:
  `6edb673f9b478cd651ea2079fa9d6aef490beb8bac67308c9d25a31f15f3e9f3`.
- Bounded-GC result:
  `f0d1ee2af5f3ef0f99e985198df057728034e0deaf7d60ceb7fdf1d8f7c15fa0`.
- Multimedia-shape report:
  `1d7b145115296c536a82bb70d2125e80c38efb4fc672cf3d97843488ff2afd26`.

These hashes prove provenance and expected semantics only. No old binary or
measurement is accepted as a post-cut result.

## Frozen source identities

| Ref | Path | Git blob | SHA-256 |
|---|---|---|---|
| `5a6a2cb0` | `packages/rs-sdk-tests/tests/forktree_stage1_application_oracle.rs` | `46c332dfa7336b5235bb91c093c69d08717002c5` | `4ee9af6e431baaa3e5fa00cea661d73e6d363e253c0bed02427bd944a15a7729` |
| `ae3b9bf1` | `packages/engine-benchmarks/benches/forktree_stage2_acceptance.rs` | `3ce64689087141b3406b10a89489efcddd3a05b3` | `9946101204ff7a4c3e02757fd8b28ae94c2e23af25730753b6fbe49b5ecf66db` |
| `ae3b9bf1` | `packages/engine-benchmarks/benches/forktree_stage2_acceptance/cursor_contract.rs` | `9889f7d5ee098eb3cca5966eb5ec9c0fe596f8a9` | `c111248de73144e1ef02fafc22a2b53dfddc8b2f5dced92301a3984a3b269729` |
| `2a0e8512` | `packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs` | `9e24b8ba730ee23e32b383e153b30849cb777936` | `e7c3ae66a5eab937b555ecb72a9c275d27021d3d590762111e3a8f5e4132e737` |
| `ee402a09` | `packages/engine-benchmarks/benches/forktree_stage2_recovery_no_lease.rs` | `26f9a98579df55ac1cc7ef6c6ef2e1c076c1d017` | `df365937ab40bf13d44c8f304257e11b87cb18cf9650d5ebb8a31ff224121059` |
| `ee402a09` | `packages/engine-benchmarks/tests/forktree_stage2_recovery_no_lease_adversarial.rs` | `0302aa2b15c815fba018f04cf5ec97427a36c024` | `63fd10d5e0334f3efb996d23501b3c8dd8ffb9e70a4a6b67aa804fc63aee3857` |
| `c2042c0e` | `packages/engine-benchmarks/tests/large_payload_read_qualification.rs` | `f22de8c86598b77c45123003e7d6e8aafc2537a2` | `1d44dc723aa9d502f004442dfe7b0e38e6b74a9321882f4544c066d9dbb616f8` |
| `d8ddc071` | same path, native shape successor | `66531a8b4f30927fad59a052b9a03e3a07400726` | `3b4f2f0da6501139cb611633cf83c040f4a99b8fd302b139536af7a9ea2c2bbe` |
| `73f191fb` | `packages/lix/tests/forktree_bounded_gc_oracle.rs` | `d8d8d7a70433cdf5f04ee7d9adbfe66914d4d07d` | `12f8b4819520d3f86487940b6a374012c84afc73eda3291333f4f0c8c0c4d004` |

## Conflict-sensitive production owners

The harness overlay must not edit these owners. A failure is attributed before
any source response.

- ForkTree sealed owner: `packages/lix/src/forktree/**`.
- SQL/batch authority: `packages/lix/src/sql2/**`,
  `packages/lix/src/session/execute.rs`, and
  `packages/lix/src/session/transaction.rs`.
- Cursor/storage authority: `packages/storage/src/**`,
  `packages/rocksdb-storage/**`, and `packages/slatedb-storage/**`.
- Legacy authorities scheduled for hard deletion: tracked-state/current/hot,
  changelog, branch control, working diff, binary CAS, media upload, and
  repository GC owner modules and their 42 spaces.
- CLI routing: normal local open is RocksDB; explicit replay may select
  SlateDB. SQLite/FileStorage compatibility must not return.

Any test-only bridge is cfg-only, typed, owner-local, and absent from normal
production reachability. The post-cut residue oracle must remain zero before
these runtime cells are admitted.

## Architecture decisions preserved

- One authenticated immutable object space and one selector plane.
- Object identity is independent of physical extent placement; any future
  locator is rebuildable and non-authoritative.
- Handle-lifetime `StorageRead` snapshots are reader pins. There is no durable
  ReaderLease authority.
- A global commit-version/GC-watermark remains the sole ordering fence, while
  exact owner selectors define semantic conflicts. A global-only mismatch may
  internally retry already-authenticated immutable bytes after one coherent
  reread; an owner mismatch is stale.
- Online history-independent packing is not required. Sorted bulk ingest is a
  reproducible-root boundary; divergent equal states may have different roots
  and can degrade to O(N) cold diff. Preserve an offline same-authority O(N)
  canonical snapshot seam, but no second online format/index.
- Ordered range/OLAP uses one coherent read, one authenticated batch per
  level, deduplicated value packs, and projection before full-row allocation.
- Cold diff batches authenticated sibling/child forests and deduplicates value
  packs per call; it does not add a cache, index, or locator authority.
