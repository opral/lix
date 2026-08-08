# ForkTree Stage 2 acceptance-ref verifier

Status: frozen test/report-only preparation for the first immutable runnable
Stage 2 head. No candidate artifact has been applied and no build has run.

The verifier worktree carries acceptance-matrix successor
`ae484a2d42172ee9670fb0926546eb3b4ce1dfe9`, tree
`1e08be3f85fe7cc76458e3008b9de907b89c95e8`. At creation the workspace
filesystem had 49 GiB available. Cargo targets and databases will remain under
this workspace filesystem and every future cell is capped at 20 minutes.

## Verifier

forktree_stage2_acceptance_verify.sh fetches the nine immutable artifact
branches into refs/stage2-acceptance-verifier/*, without checking them out or
applying them. It reproduces every expected commit, tree, canonical full-index
binary diff, and 24 embedded source/report hashes. The external delete report
and point-read report/manifest/binary hashes are recorded as provenance but
correctly marked non-embedded. The point-read report and manifest are not
mounted on this reviewer host; their supplied hashes and author-reported 3/3
manifest verification are not presented as a local file check.

Invocation:

    timeout 20m packages/rs-sdk-tests/tests/forktree_stage2_acceptance_verify.sh .

Result: PASS for 9/9 refs and 24/24 embedded files. The frozen
machine-readable output is FORKTREE_STAGE2_ACCEPTANCE_REF_VERIFICATION.tsv.

## Runnable-head boundary

Ryzen-V has not advertised a first runnable immutable SPI head. Its latest
observed milestone is still compiling a topology test and is not eligible for
artifact application. Until a head is explicitly frozen as runnable, this
worktree remains provenance-only.

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
11. only then run broader version-control and multimedia gates.

A pass by the detached delete benchmark model is not production acceptance.
The delete sequence must be bound to the production ForkTree owner. Likewise,
the no-lease model is a discriminator; the sealed 20-check GC/publication
facade is the candidate-facing authority gate.

The point-read model is also reference-only: its independent authenticated
encoding and medians do not qualify the production reader. Candidate runs use
fresh nonexistent database paths and the exact frozen harness=false/profile
bench invocation recorded in the acceptance matrix.
