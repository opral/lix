# Independent review: SQL file/directory history and checkpoint migration oracle

Verdict: **BLOCKER — TEST/REPORT-ONLY oracle incomplete for the requested
acceptance scope.** The package correctly proves that the exact production
frontier is still RED; this verdict is not a production-source approval and
does not request or make a production edit.

## Immutable provenance

Reviewed oracle:

* head `25016a99c5356045cdd9f70e928a08b512544ff3`
* tree `970437dcfb089c4c28f90d57d207324a442065f6`
* parent/base `97a7116d00206954b581cf43937cc5db6c23f30b`
* base-to-head full-index binary diff SHA-256
  `df04eaf2df9db418b6a83ca65f1c24da00f27eeb405a58f9fe16e718fc8ab7a4`
* stable patch ID `817c65c92ed3f24f1784ebb2d484b10b38672f24`

The oracle's intended SQL source anchor is exact 413:

* head `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
* tree `820fe560da3bbd2b00b788b0b1759c409048cd6e`
* parent `11442c1e0023e20307a7231d88cd557bc704fd13`
* e166-to-413 full-index diff SHA-256
  `70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329`

Its historical fail-closed prerequisite is 97a:

* head `97a7116d00206954b581cf43937cc5db6c23f30b`
* tree `457a3919903169ca1edd2fe81df8b81e70b06d37`
* parent/oracle `448624a557bca2c341f4a1820b79222a5691613a`
* parent-to-head full-index diff SHA-256
  `08fee7a84860b27836468f63eff9f6c000538c08947820a26bfbba1e54328cdf`

Reviewed package file hashes, as recorded by the candidate's manifest:

```text
CHECKLIST.md 05290b6ab60a689d5f60051cd0b6d9f1e6a1b3246876de4b4466424a77ce5c73
FUTURE_COMMANDS.md 2002d138e9bca71a1e12e31b8c18333f1a0949454287ee3a1af43e3fde69f018
REPORT.md 3ce63a5cd35578f6485591c113728dcd3626477f50589671fedb0f4925f6da0a
source_verifier.sh 773ef78b7fab4d2154a8d2b747e013d69156ced26e76e6ded6e0fdc335b91f3a
model.rs f5341f59b9a57ccd9a1c9649d765282d8dd8634e1fa86dd6cbbaf5626e3e3576
```

## Checks reproduced

The candidate source verifier was run read-only:

```text
bash evidence/forktree-checkpoint-history-oracle-413/source_verifier.sh --expect-red
```

It passes its provenance checks and exits with the expected RED. The RED is
causal and correct:

* `packages/lix/src/checkpoint.rs:95-187` still reads checkpoint markers with
  `TrackedStateStoreReader` and walks chronology with a separate
  `CommitGraphReader`.
* `packages/lix/src/session/checkpoint.rs:73-104` and
  `packages/lix/src/sql2/providers/checkpoint.rs:148-169` still create legacy
  tracked readers.
* `packages/lix/src/sql2/providers/file_history.rs:993-1057,1202-1223`
  reconstructs each observed commit through `TrackedStateStoreReader`; plugin
  discovery also has a separate tracked-reader route around `:1569-1600`.
* `packages/lix/src/sql2/providers/directory_history.rs:386-416` uses a
  separate tracked reader for observed directory state.
* `packages/lix/src/sql2/providers/filesystem_working_diff.rs:147-172,235-257`
  combines a graph reader and legacy tracked diff/scan readers.
* `packages/lix/src/forktree/serving.rs:679-687` returns `Ok(None)` for a
  missing CommitCatalog entry, while `forktree/view.rs:301-320` maps that
  result into the same optional row shape used for a valid absent key. The
  prerequisite correctly keeps this RED rather than calling it absence.

The source also confirms useful existing semantics that must be preserved:
file-history output is ordered by file ID, as-of commit, depth, and observed
commit (`file_history.rs:777-788`); directory-history output has an explicit
directory/event ordering (`directory_history.rs:345-361`); and ForkTree state
lowering distinguishes value, NULL, and tombstone (`forktree/view.rs:304-309`).
These are source facts, not runtime qualification.

## Blocking gaps in the oracle package

The package's `model.rs` has six tests covering first-parent checkpoint
history, recovery-vs-chronology, 65 rotations, cell/error classification,
undo/redo floor, and retention roots. It does not contain executable
discriminators for the requested SQL file/directory migration:

1. There is no retained-read identity/counter model proving one read across
   point, scan, diff, checkpoint, file history, and directory history. The
   source report requires one view, but the model cannot fail a second read,
   retry, fallback, or cache acquisition.
2. There is no file/directory history output model for stable identity/event
   ordering, path reconstruction, projection, or post-resolution `LIMIT`.
   The source has sort calls, but no oracle assertion protects their public
   result.
3. There is no executable historical diff model covering disjoint and
   same-identity changes, changed/unchanged file and directory descriptors,
   NULL versus tombstone, or plugin-registry metadata observed at base and
   endpoints.
4. Missing CommitCatalog, missing root, wrong-kind root, malformed catalog,
   and valid absent key are represented only by a four-way enum function. No
   test binds those outcomes to a file-history scan, directory-history scan,
   checkpoint lookup, or working-diff output.
5. Cold reopen, physical corruption, and no-fallback behavior appear only as
   future prose/commands. No frozen executable test or source counter proves
   that reopen preserves the same result and error boundary.

`FUTURE_COMMANDS.md` names one aggregate test per backend, but does not name
or freeze the required point/scan/diff/checkpoint/file/directory subcases or a
read-counter assertion. Therefore a later implementation could satisfy the
six pure tests while silently returning empty rows for a missing root, taking a
second reader for plugin discovery, changing event order, or treating NULL as
tombstone.

## Required report-only correction

Before this oracle can be APPROVED, add only test/report artifacts (no
production change) that:

* model one `CoherentView`/`StorageRead` identity and reject any second view,
  retry, fallback, cache, or legacy `TrackedStateStoreReader` acquisition;
* include file and directory history vectors with path ancestry, event order,
  projection, post-order `LIMIT`, changed/unchanged rows, NULL, tombstone,
  and value;
* include historical point, scan, diff, checkpoint, and working-diff cases
  for valid absence versus missing/malformed/wrong-kind CommitCatalog, commit,
  and root objects;
* include plugin registry metadata at base/target/source observed commits,
  cold reopen, and corruption fail-closed outcomes;
* freeze separate Memory, RocksDB, and SlateDB test names that print view/read
  counts and semantic digests before any runtime qualification.

The existing production RED source gate, public semantics, and no-compatibility
deletion boundary should remain unchanged. No production build or adapter run
was performed for this review.
