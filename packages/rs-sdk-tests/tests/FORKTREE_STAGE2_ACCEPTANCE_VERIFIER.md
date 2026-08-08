# ForkTree Stage 2 acceptance-ref verifier

Status: frozen test/report-only preparation for the first immutable runnable
Stage 2 head. No candidate artifact has been applied and no build has run.

The verifier worktree carries ten core acceptance rows plus immutable preflight
transport/discovery records, and its readiness metadata is anchored to exact
current main
`822c204ce0670969ca71045bc74f9ca25fde8093` (tree
`fac3f2b713683be17c34515062dd72edc8feed95`) and the latest held
catalog-boundary frontier
`7c9b1060bc396dfa54efcc6c888e37894a7cfb04` (tree
`ee96c5b64912b8fa8bb15fb7c31916244a255523`, parent
`34f2dacad4a0126a58d015f27ed75c2142547dd5`). The prior 34f2 frontier remains
preserved as its immediate parent; the held frontier report SHA is
`8fe40b5fe63895a132293151e126394161552bf95a4106e02c01bb068c2b17ff` and its
stable patch ID is `1e9b50c12a8db7a3db9024e28ff2d06b5a0dbb0d`. This is a ten-row acceptance-matrix
readiness successor
`7678fb1cd4bad261c5a667c5916645bfb731b944`, tree
`98390511cdc0b2591a1813b16b681928807a6232`. At creation the workspace
filesystem had 49 GiB available. Cargo targets and databases will remain under
this workspace filesystem and every future cell is capped at 20 minutes.

## Verifier

forktree_stage2_acceptance_verify.sh fetches the ten immutable artifact
branches into refs/stage2-acceptance-verifier/*, without checking them out or
applying them. It reproduces exact current-main/frontier anchors, every
expected commit, tree, canonical full-index binary diff, and the immutable P0
transport plus external target-discovery file hashes. The external delete report
and point-read report/manifest/binary hashes are recorded as provenance but
correctly marked non-embedded. The point-read report and manifest are not
mounted on this reviewer host; their supplied hashes and author-reported 3/3
manifest verification are not presented as a local file check.

Invocation:

    timeout 20m packages/rs-sdk-tests/tests/forktree_stage2_acceptance_verify.sh .

The latest read-only run completed successfully. Captured stdout SHA-256 is
`96e6e33bd8b550ad0c0a1bb758e3281cd0455b468ec78d4868e5e27c24712e5e` and
stderr is empty (SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).

The OLAP diff is reproduced with `GIT_ATTR_SOURCE` bound to its exact head so
the committed binary attributes govern the 42,863-byte canonical stream. The
same-object worktree text rendering is documented in the matrix but is not the
verifier identity.

The verifier separately checks the approved non-runnable readiness lineage
without counting it as an acceptance row. The topology owner head is
`af7899f41c489fe763ce1a64c5468083570979e2`, tree
`da097bd739b50629ea39b155d4fa9efc870654e0`, parent
`2e0cea1b91558179e6ed90847bc8b04b23de246f`. It verifies focused and
`a12` lineage diffs. Approval of this object does not authorize a build or
artifact application.

It retains the later blocked BlobRef predecessor
`08f8dd5cf20842f79996fae9eb7b0924f074a084`, tree
`19c8706d6bc3d1dbe9217b4f8386b19c66f027a8`, whose exact parent is the
approved topology head. The verifier reproduces the focused and `a12` lineage
diffs and all five changed source blob IDs. This object remains immutable blocker
evidence: same-size manifest identity substitution is not rejected by its range
path.

The approved reader base is the two-reviewer source-approved successor
`54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`, tree
`5a8da9f8b11d83bf8216e266beaf4042cee84068`, parent `08f8dd5c...`.
The verifier reproduces its focused and `a12` lineage diffs and all three changed
source blob IDs. It remains non-runnable and does not authorize a build or
artifact application.

The latest source/static-approved readiness base is the narrow 5A2 successor
`a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`, tree
`d8326da2b1d38bd51b8ac7229d00684a6865bce2`, parent `5c4cae81...`.
The verifier reproduces its focused and `a12` lineage diffs and its two changed
transaction blob IDs. Static approval covers intent-before-view/plan,
unsupported-cohort zero-write rejection, true empty no-op behavior, and runtime
state in the sole ForkTree plan. It remains compile-red/non-runnable and does
not authorize an artifact application or runtime claim.

The later ordinary-writer milestone
`5c4cae810324a34c0adbbb5a1a0be5fba5348054`, tree
`16741cdf6efce6bccdcf469406be1e1bce9b5f37`, is separately identity-pinned as a
blocked frontier with its three changed source blobs. It discards deterministic
runtime sequence state; can drop ref-only/selected-history intent while still
publishing untracked/epoch work; and errors on true empty commits instead of
preserving no-op behavior. It also remains compile-red and retains independent
upload/checkpoint/history/multi-branch/reachability publication families, so it
does not supersede `54e90dbf...` as readiness base.

The scanner discrepancy is resolved. Exact scanner ref `1dbbf3d...`, source
SHA `f71e91fc...`, and frozen binary SHA `40d02e20...` produce the same 166
semantic records for 5A and 5A2. The canonical sorted finding-only stream SHA is
`86010e7dad821c8cc89858dcbf1a55cb9a234ea2eeab6d43ef08247e4ede61aa`.
Raw baseline stdout SHA `6f4013da...` includes the footer and final LF; audit
stdout+stderr SHA `3891a486...` adds only the expected terminal audit error.
The scanner acceptance identity is the source/binary plus normalized semantic
set, not the redirection-dependent presentation. Independent reconciliation
report SHA is `1f90f530b02743ffda50b56646499759119e69590a11f0b3eabe4a71b9b3a251`.

The immutable P0+W1a transport is provenance-bound: ref
`origin/codex/forktree-stage2-p0-w1a-acceptance-a1cf`, head
`d03d03a4925b51c7d43801bf256b9c9b37f53f67`, tree
`d5016a33f069c5151944eff1f11eca650d8fd872`, parent a1cf, parent diff
`e568ad37ff4531958780b6530124c91dccb9df4f572c0c41d4e75918605447d9`, and
manifest `73cd9f5d...`; contract `cfd25a60...`, cases `77af0924...`, verifier
`35dfbedc...`, freeze report `77a07625...`, and transport report
`db5a29e0...` are checked from the five transport files. It is the first source gate for
the next runnable candidate. It must remove direct publication commit entry
points and prove ordered single-branch history/selected members use exactly one
read, plan, prepare, and commit before any residue or runtime artifact is run.

The immutable external target-discovery package is also checked: ref
`origin/codex/forktree-post-stage2-acceptance-manifest`, head
`1ad4c879b1d8339bca7fdc414fdee36305ce9a69`, tree
`bc9728cfa0b761d98c0639479c80c65cbad5e4a9`, a12 parent, diff
`1bf97fb4037add7d5ecf3d046359e6ab92188f3740532c155e5bd885a0fa841d`, patch
`7389880b6907b20c3d97971883b352105e15a816`, and its three file hashes. It is
discovery-only and does not qualify a candidate result.

Result: PASS for 10/10 core acceptance refs, exact current-main/frontier
anchors, immutable P0 transport and external target-discovery identities,
latest source/static readiness `a1cf8f7f...`, two retained blocked frontiers,
13/13 readiness source blobs, and the embedded acceptance files. The frozen
machine-readable output is FORKTREE_STAGE2_ACCEPTANCE_REF_VERIFICATION.tsv.

## Runnable-head boundary

Ryzen-V has not advertised a first runnable immutable SPI head. The latest
approved 5A2 milestone remains non-runnable, so the next candidate must be
an explicitly compile-green writer/SPI descendant of
`a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`. Ordinary commit lowering alone
does not qualify it: readiness also requires independent R2 atomicity approval,
H2 deletion/residue approval, and zero independent transaction/upload/GC
ForkTree publication points. Until then this worktree remains provenance-only.

On the first runnable immutable head, apply only the minimum landing gate in
this order:

1. Run the P0+W1a source gate, then materialize exact test/report artifacts in
   a fresh detached candidate worktree. Never merge historical artifact refs.
2. Run deletion/residue, semantic delegation, CLI-routing, cursor, compile,
   fmt/diff, and warnings-denied Clippy. Stop on any residue, compatibility
   route, dual writer, missing facade, compile, or lint failure.
3. Run production-owner 65-row batch-1 delete on RocksDB, then SlateDB.
4. Run SQL transaction/publication smoke on RocksDB, then SlateDB.
5. Run core branch/diff/merge/history/undo-redo on RocksDB, then SlateDB,
   including parsed-file/public-file callers, corruption, and cold reopen.
6. Run the parsed-file plus large-BlobRef identity cells from the immutable
   discovery runner: `vc-rocks-1k`, `vc-slate-1k`, `blob-rocks-64`, and
   `blob-slate-64`, each with fresh paths and exact candidate binding.
7. Run three-row checkpoint/recovery on RocksDB, then SlateDB.
8. Run sealed GC/publication on RocksDB, then SlateDB, including both race
   orders, upload completion/abort, corruption, reopen, and final release.

Stop at the first focused blocker. The point-read A/B threshold, OLAP
10K/50K/500K, broad retained-history/version-control, broad multimedia shapes,
512 MiB blobs, and detached comparator/scaling artifacts are post-landing
follow-ups, not prerequisites. They retain their existing rows, exact source
identities, fresh paths, and 20-minute per-cell cap.

A pass by the detached delete benchmark model is not production acceptance.
The delete sequence must be bound to the production ForkTree owner. Likewise,
the no-lease model is a discriminator; the sealed 20-check GC/publication
facade is the candidate-facing authority gate.

The point-read model is also reference-only: its independent authenticated
encoding and medians do not qualify the production reader. Candidate runs use
fresh nonexistent database paths and the exact frozen harness=false/profile
bench invocation recorded in the acceptance matrix.
