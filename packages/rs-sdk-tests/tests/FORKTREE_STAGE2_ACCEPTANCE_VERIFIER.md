# ForkTree Stage 2 acceptance-ref verifier

Status: frozen test/report-only preparation for the first immutable runnable
Stage 2 head. No candidate artifact has been applied and no build has run.

The verifier worktree is rooted at acceptance-matrix commit
796ae1faa91e99b2c02571509500505d65b655b2, tree
1320b090f86640ac469b59755c9f76ebd625db62. At creation the workspace
filesystem had 49 GiB available. Cargo targets and databases will remain under
this workspace filesystem and every future cell is capped at 20 minutes.

## Verifier

forktree_stage2_acceptance_verify.sh fetches the eight immutable artifact
branches into refs/stage2-acceptance-verifier/*, without checking them out or
applying them. It reproduces every expected commit, tree, canonical full-index
binary diff, and 22 embedded source/report hashes. The external delete report
hash is recorded as provenance but correctly marked non-embedded.

Invocation:

    timeout 20m packages/rs-sdk-tests/tests/forktree_stage2_acceptance_verify.sh .

Result: PASS for 8/8 refs and 22/22 embedded files in 4.5 seconds. The frozen
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
6. run SQL RocksDB then SlateDB;
7. run checkpoint RocksDB then SlateDB;
8. run no-lease and sealed GC/publication RocksDB then SlateDB;
9. only then run broader version-control and multimedia gates.

A pass by the detached delete benchmark model is not production acceptance.
The delete sequence must be bound to the production ForkTree owner. Likewise,
the no-lease model is a discriminator; the sealed 20-check GC/publication
facade is the candidate-facing authority gate.
