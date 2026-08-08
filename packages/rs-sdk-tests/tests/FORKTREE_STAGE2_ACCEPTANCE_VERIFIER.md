# ForkTree Stage 2 acceptance-ref verifier

Status: frozen test/report-only preparation for the first immutable runnable
Stage 2 head. No candidate artifact has been applied and no build has run.

The verifier worktree carries ten-row acceptance-matrix readiness successor
`c7674c6d0dd5b995f10a016e281e5106b703de50`, tree
`62329238fb6a7b35c9d4bda61ee93e464ecda16d`. At creation the workspace
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

The latest approved readiness base is the two-reviewer source-approved successor
`54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`, tree
`5a8da9f8b11d83bf8216e266beaf4042cee84068`, parent `08f8dd5c...`.
The verifier reproduces its focused and `a12` lineage diffs and all three changed
source blob IDs. It remains non-runnable and does not authorize a build or
artifact application.

Result: PASS for 10/10 acceptance refs, latest approved readiness
`54e90dbf...`, one retained superseded blocker, 8/8 readiness source blobs, and
27/27 embedded acceptance files. The frozen
machine-readable output is FORKTREE_STAGE2_ACCEPTANCE_REF_VERIFICATION.tsv.

## Runnable-head boundary

Ryzen-V has not advertised a first runnable immutable SPI head. The latest
approved BlobRef milestone remains non-runnable, so the next candidate must be
an explicitly compile-green writer/SPI descendant of
`54e90dbf2bcf55c74de0be6ea4b217dc02cec89c`. Until then this worktree remains
provenance-only.

On the first runnable immutable head:

1. create a fresh detached candidate worktree and isolated target;
2. materialize only exact test/report files, never merge their historical
   branches wholesale;
3. run fmt/diff and the static residue/facade/CLI/cursor gate;
4. stop on any nonzero legacy authority;
5. run the production-bound 65-row delete on RocksDB, then SlateDB;
6. bind and source-map the actual public point/BlobRef seam, then run the 1K
   point-read gate on RocksDB and SlateDB; require the frozen counts/digest,
   cold reopen, four fail-closed corruption cases, greater than 10% meaningful
   paired improvement, and no critical regression greater than 5%;
7. stop before SQL and 10K/50K scaling if the point gate fails;
8. run SQL RocksDB then SlateDB;
9. run checkpoint RocksDB then SlateDB;
10. run no-lease and sealed GC/publication RocksDB then SlateDB;
11. run OLAP 10K RocksDB/SlateDB plus corruption, then 50K RocksDB/SlateDB,
    then 500K RocksDB/SlateDB; a SlateDB six-versus-five or twelve-versus-ten
    physical-object residual hard-blocks without the exact narrow hash-bound
    manager waiver;
12. only then run broader version-control and multimedia gates.

A pass by the detached delete benchmark model is not production acceptance.
The delete sequence must be bound to the production ForkTree owner. Likewise,
the no-lease model is a discriminator; the sealed 20-check GC/publication
facade is the candidate-facing authority gate.

The point-read model is also reference-only: its independent authenticated
encoding and medians do not qualify the production reader. Candidate runs use
fresh nonexistent database paths and the exact frozen harness=false/profile
bench invocation recorded in the acceptance matrix.
