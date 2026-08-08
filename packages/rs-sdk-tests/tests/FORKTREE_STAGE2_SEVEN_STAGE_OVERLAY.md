# ForkTree Stage 2 seven-stage landing overlay

Status: test/report-only, dormant, and disposable. This package does not
modify production source, merge artifact branches, or execute a runtime cell.
The `run` mode is fenced on purpose: it requires an explicitly compile-green
candidate and a complete immutable R1 binding. Until both exist, `verify` may
only return provenance plus a HOLD for checkpoint/GC.

## Exact compatibility anchor

The overlay is prepared for the exact topology/semantic successor:

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
with tree `fac3f2b713683be17c34515062dd72edc8feed95`. The candidate must be a
descendant of the exact `1f742...` object, or the coordinator must record a
replacement lineage before qualification. The overlay never assumes that a
mutable branch tip is equivalent to this object.

## Seven stages and source mapping

The machine-readable order and commands are in
`FORKTREE_STAGE2_SEVEN_STAGE_OVERLAY.tsv`. Stage 1 combines the P0 publication
cut with deletion/residue, so the former broad/comparator prerequisites are not
part of landing. Stages 2--5 exercise the production transaction, version
control, parsed-file, and authenticated BlobRef owners. Stages 6--7 are held
for the R1 checkpoint/GC landing oracle.

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
| 7 | R1 GC/publication/upload owner | both race orders, upload completion/abort, corruption, reopen, final release |

No stage may add a second writer, compatibility reader, fallback format,
raw-object SPI, or physical-serving authority. Missing R1 identity is a
provenance hold, not permission to use the existing a12 checkpoint or GC
references as a substitute.

## R1 binding

`R1_CHECKPOINT_GC_BINDING.tsv` is deliberately unbound until R1 publishes an
immutable ref. Existing checkpoint and GC oracle refs remain historical matrix
rows; they are not silently relabeled R1. The binding must supply exact
`ref`, `head`, `tree`, `parent`, source/report hashes, and the two case names
for RocksDB and SlateDB. The verifier checks that identity and requires the
candidate to expose the named typed facade before enabling stages 6--7.

## Overlay scope

The overlay consists only of this manifest, its TSV, the R1 binding placeholder,
and the dormant verifier script. Materialization creates a disposable detached
candidate worktree and places these files under `.stage2-acceptance-overlay/`;
it does not patch production paths or copy oracle implementations into them.
The candidate's own production diff is recorded, not rewritten. Artifact refs
are verified by exact head/tree/path/hash before any future materialization.

The post-landing point-read, OLAP, broad VC, multimedia, 512 MiB, and detached
comparator rows remain follow-ups only. They cannot block the seven-stage
landing package.
