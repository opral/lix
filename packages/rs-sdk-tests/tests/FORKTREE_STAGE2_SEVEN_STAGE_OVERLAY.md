# ForkTree Stage 2 seven-stage landing overlay

Status: test/report-only, R1-bound, R5 source-approved but transport-pending,
reader frontier blocked, dormant, and disposable. This package does not
modify production source, merge artifact branches, or execute a runtime cell.
The `run` mode is fenced on purpose: it requires an explicitly compile-green
candidate. `verify` validates the immutable R1 binding, while runtime still
requires an explicitly compile-green candidate.

## Exact compatibility anchor

The overlay records the exact topology/semantic frontier, which is now blocked
and is not a promotable execution anchor:

```text
base head:       1f742a382c755399b8a49ab536c4f6dc55fffdd8
base tree:       860a047b98eaa38368a3d889497628e244c2e0ec
base parent:     7c9b1060bc396dfa54efcc6c888e37894a7cfb04
base parent tree:ee96c5b64912b8fa8bb15fb7c31916244a255523
parent..base diff (full-index binary):
  18a7df6d37fce9809b2214f5b1530204b1a2dd4cf19760aa876ec7856249dbc7
remote ref:      origin/codex/forktree-stage2-milestone5c-topology-semantic-bridge
```

The current-main anchor remains `822c204ce0670969ca71045bc74f9ca25fde8093`
with tree `fac3f2b713683be17c34515062dd72edc8feed95`. The SQL change provider
at `1f742...` can silently omit missing authenticated `CommitRecord` entries,
so this object and its broad descendants are blocked. A candidate may become
eligible only after `R5_CORRECTED_FRONTIER_BINDING.tsv` names a narrow reviewed
correction and the candidate descends from that corrected object.

## Seven stages and source mapping

The machine-readable order and commands are in
`FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv`. Stage 1 combines the P0 publication
cut with deletion/residue, so the former broad/comparator prerequisites are not
part of landing. Stages 2--5 exercise the production transaction, version
control, parsed-file, and authenticated BlobRef owners. Stages 6--7 are bound
to the R1 checkpoint/GC landing oracle.

Every stage is RocksDB first, then SlateDB, with a fresh database/evidence path
and an isolated target. Each build or process is independently capped at 20
minutes. A timeout before test execution is a host/compiler boundary, never a
pass. Stop at the first focused failure; do not widen to point-read, OLAP,
broad version-control, multimedia, 512 MiB, or comparator/scaling work.

The production mapping is intentionally semantic:

| Stage | Required sole owner | Required proof |
|---|---|---|
| 1 | ForkTree publication + deletion/residue facades | no direct publication boundary, no legacy spaces/cursors, no raw mutation, compile/fmt/Clippy |
| 2 | ForkTree path-copy transaction owner | 65-row delete, cold remaining state, no missing object |
| 3 | ForkTree transaction/public SQL facade | atomic batch, savepoint/rollback, idempotency, RETURNING/ON CONFLICT |
| 4 | ForkTree branch/history/merge owner | branch/diff/merge/history/undo-redo, corruption, cold reopen |
| 5 | ForkTree parsed-file and BlobRef owner | exact bytes, authenticated BlobId/size/domain/hash/range, cold reopen |
| 6 | R1 checkpoint/recovery selector/object owner | 3-row H/C bridge, 64 rotations, merge-base C, recovery and reclaim |
| 7 | R1 GC/publication/upload owner plus external W5/R7 reachability contract | both race orders, upload completion/abort, corruption, reopen, reader pins, complete roots, final release |

No stage may add a second writer, compatibility reader, fallback format,
raw-object SPI, or physical-serving authority.

## R1 binding

`R1_CHECKPOINT_GC_BINDING.tsv` is bound to exact immutable ref
`origin/codex/forktree-stage2-checkpoint-gc-landing-v4`, head
`f01b08a2db1bd71650eec11123adec26b5222dcc`, tree
`165efdd6ca58c84d737249c41718001823e20ffb`, parent
`9bace2186664fc77877aa24abae6e516855313a1`, full-index diff
`5675e2b0ada7ce8e54b5f5746f1845f3f8a834bbc3f6aef7cc6ad435d937f83f`, and
patch ID `e7f44903bddf8acd7fa5a4eb38895ed8c3340995`. It contains exactly the
two test/report paths named in the binding. The external report is recorded by
SHA-256 `2ab06208bb46aeee5b4cd853a6957b19647b48aabd5cc7ee5d865ba3c3c41290`.
The verifier checks the remote object, parent, both embedded blob hashes, and
the external report when `R1_REPORT_PATH` is supplied. Runtime remains gated
on candidate compile-green status and the reviewed R5 correction.

The stage-7 reachability contract is additionally bound to the external,
report-only W5/R7 artifact in `W5_R7_GC_REACHABILITY_CONTRACT.tsv`. Its exact
contract SHA-256 is
`9b0aa1f080a082685df1cdbd905bbf90064840b9858159f099d394d7ecf1afb8`; the
companion `SHA256SUMS` SHA-256 is
`cea56dd052eb8d64a41bd52feebf5a39623a233d3c8037e0bc5b792e76190e88`.
The immutable package is report-only and no-run blocked by inherited d6b
production symbols. Its exact head is
`6487170dfa11b24411dbbd73e3c003439072df09`, tree
`94eefb7de3260a8c8a3217805a5372cb8670157c`, full-index diff
`b12d49fbb8f991459ca9a9e6513f26f392ce642c9b25e95efc1be44ecb166345`, patch
`3b8ef7eeec6cb3b6edbc5f5b1d5226f79615a247`, report
`fd47899844bafc72fb47c254f77c74b91d4d40f43d0bb2a54d043823892b6a35`, and
manifest `ea5a278b81d23136e276b29e350752b8c25ce656ba375864362fc2ab0d60ee4c`.
It supplies the one epoch-fenced authority, 64+suffix and one-debt/no-spin,
H/S/C chronology, reader-pin, complete-root, final-reference, corruption, and
cold-reopen requirements for stage 7, but does not authorize runtime.

## R5 correction hold

`R5_CORRECTED_FRONTIER_BINDING.tsv` preserves the blocked `1f742...` identity
and records source-approved frontier `d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768`
(tree `641654079f60fcd1c9ff9ccbbd06d3edcabe4096`, parent `1f742...`, diff
`be940f41...`, patch `1902f4c9`) with status
`source-approved-awaiting-immutable-transport`. R2 and R4 approvals are
recorded, but its ref and report are intentionally unbound. The overlay
verifier still rejects materialization and runtime until the exact immutable
R5 ref/head/tree/report hashes are complete.

## Overlay scope

## Reader frontier hold

`READER_FRONTIER_BINDING.tsv` records pending reader frontier
`9f3c703e953440cde1d60b1511467c4337648c8f` / tree
`51a0026c0c3eced6fdaa5e5ed4824111377f086c`, parent d6b, diff prefix
`6000f34f`, patch prefix `3890dad2`, and expected compile frontier 185 errors /
7 warnings. It is blocked because derived/history scans can return empty
current-state success and `load_exact_batch` still acquires legacy
TrackedHead/control state. Its transport ref is unbound; d6 remains the last
approved base, and this frontier remains metadata-only and cannot enable
runtime.

The overlay consists only of this manifest, its TSV, the R1 binding, the R5
correction hold, the W5/R7 binding, the reader frontier binding, and the dormant
verifier script. Materialization creates a disposable detached
candidate worktree and places these files under `.stage2-acceptance-overlay/`;
it does not patch production paths or copy oracle implementations into them.
The candidate's own production diff is recorded, not rewritten. Artifact refs
are verified by exact head/tree/path/hash before any future materialization.

The post-landing point-read, OLAP, broad VC, multimedia, 512 MiB, and detached
comparator rows remain follow-ups only. They cannot block the seven-stage
landing package.
