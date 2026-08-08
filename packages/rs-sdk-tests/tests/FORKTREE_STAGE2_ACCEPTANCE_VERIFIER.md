# ForkTree Stage 2 acceptance-ref verifier

Status: frozen test/report-only preparation for the first immutable runnable
Stage 2 head. No candidate artifact has been applied and no build has run.

The verifier worktree carries ten-row acceptance-matrix readiness successor
`7678fb1cd4bad261c5a667c5916645bfb731b944`, tree
`98390511cdc0b2591a1813b16b681928807a6232`. At creation the workspace
filesystem had 49 GiB available. Cargo targets and databases will remain under
this workspace filesystem and every future cell is capped at 20 minutes.

## Verifier

forktree_stage2_acceptance_verify.sh fetches the ten immutable artifact
branches into refs/stage2-acceptance-verifier/*, without checking them out or
applying them. It reproduces every expected commit, tree, canonical full-index
binary diff, and 27 embedded source/report hashes. The external delete report
and point-read report/manifest/binary hashes are recorded as provenance but
correctly marked non-embedded. The point-read report and manifest are not
mounted on this reviewer host; their supplied hashes and author-reported 3/3
manifest verification are not presented as a local file check.

Invocation:

    timeout 20m packages/rs-sdk-tests/tests/forktree_stage2_acceptance_verify.sh .

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

The externally frozen P0+W1a package is also provenance-bound: manifest
`73cd9f5d...`, contract `cfd25a60...`, cases `77af0924...`, verifier
`35dfbedc...`, and freeze report `77a07625...`. It is the first source gate for
the next runnable candidate. It must remove direct publication commit entry
points and prove ordered single-branch history/selected members use exactly one
read, plan, prepare, and commit before any residue or runtime artifact is run.

Result: PASS for 10/10 acceptance refs, latest source/static readiness
`a1cf8f7f...`, two retained blocked frontiers, 13/13 readiness source blobs, and
27/27 embedded acceptance files. The frozen
machine-readable output is FORKTREE_STAGE2_ACCEPTANCE_REF_VERIFICATION.tsv.

## Runnable-head boundary

Ryzen-V has not advertised a first runnable immutable SPI head. The latest
approved 5A2 milestone remains non-runnable, so the next candidate must be
an explicitly compile-green writer/SPI descendant of
`a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`. Ordinary commit lowering alone
does not qualify it: readiness also requires independent R2 atomicity approval,
H2 deletion/residue approval, and zero independent transaction/upload/GC
ForkTree publication points. Until then this worktree remains provenance-only.

On the first runnable immutable head:

1. run the frozen P0+W1a source gate and stop unless direct publication commit
   is unnameable and W1a uses one read/plan/prepare/commit;
2. create a fresh detached candidate worktree and isolated target;
3. materialize only exact test/report files, never merge their historical
   branches wholesale;
4. run fmt/diff and the static residue/facade/CLI/cursor gate;
5. stop on any nonzero legacy authority;
6. run the production-bound 65-row delete on RocksDB, then SlateDB;
7. bind and source-map the actual public point/BlobRef seam, then run the 1K
   point-read gate on RocksDB and SlateDB; require the frozen counts/digest,
   cold reopen, four fail-closed corruption cases, greater than 10% meaningful
   paired improvement, and no critical regression greater than 5%;
8. stop before SQL and 10K/50K scaling if the point gate fails;
9. run SQL RocksDB then SlateDB;
10. run checkpoint RocksDB then SlateDB;
11. run no-lease and sealed GC/publication RocksDB then SlateDB;
12. run OLAP 10K RocksDB/SlateDB plus corruption, then 50K RocksDB/SlateDB,
    then 500K RocksDB/SlateDB; a SlateDB six-versus-five or twelve-versus-ten
    physical-object residual hard-blocks without the exact narrow hash-bound
    manager waiver;
13. only then run broader version-control and multimedia gates.

A pass by the detached delete benchmark model is not production acceptance.
The delete sequence must be bound to the production ForkTree owner. Likewise,
the no-lease model is a discriminator; the sealed 20-check GC/publication
facade is the candidate-facing authority gate.

The point-read model is also reference-only: its independent authenticated
encoding and medians do not qualify the production reader. Candidate runs use
fresh nonexistent database paths and the exact frozen harness=false/profile
bench invocation recorded in the acceptance matrix.
