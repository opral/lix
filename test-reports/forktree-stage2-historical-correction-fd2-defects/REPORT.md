# Frozen fd2 historical correction oracle

Status: **baseline oracle frozen; exact fd2 source is BLOCKED as expected**.
This is a test/report-only artifact. No production source, Cargo manifest,
adapter, PR, or main branch was changed.

## Immutable provenance

The oracle is rooted at the exact blocked source:

```text
base/source: fd2be256d763f17e9f127d4c984e36fba191cb82
base tree:   20110ca5e3c33d34217630fff0a2b784b545317a
parent:      cd91b9b90f7f468158b4df154adbed9551eb5d60
```

The first immutable source-payload commit containing the six oracle files is
`b493056059136ac1a394c912c80416d3d4b7fde4`, tree
`3cd8bc54db5004bfeb0605fa07cb61ada33b71f8`. Its base-to-payload full-index
binary diff SHA-256 is
`d2125ba574d94088460a83dcae6ffb00ce3938b19ed74f484d1fad6109a9f495`; stable
patch ID is `1a1b6e09ca9b0ddea155a23c174134ac981c0870`.

The final evidence commit is the immutable child that adds this report; its
exact head/tree and final base-to-head diff hash are emitted after the commit
and are part of the handoff alongside this report hash.

## Defect calibration

`bash verify_source_contract.sh audit` exits 1 on exact fd2 with this exact
semantic result:

```text
STATUS=BLOCKED_EXPECTED_RED
DEFECT=descriptor_tombstone_rejected
DEFECT=blob_validation_projection_gated
RELATED=blob_reference_cardinality_unchecked
PRESERVED=one-retained-ForkTree-history-view-and-fail-closed-chronology
```

The captured calibration log SHA-256 is
`cae2be6540521df3fe15a854af2ba5a14945826a3dbb15a2dca0dd83ca1136fd`.

The first defect is structurally located in
`filesystem_working_diff.rs::scan_descriptors`: a valid `row.deleted` branch
returns an error saying the descriptor is tombstoned. The corrected contract
must authenticate the tombstone and return logical absence, allowing a
live-to-tombstone filesystem diff to emit `removed`.

The second defect is structurally located in
`file_history.rs::load_file_history_rows`: `validate_file_history_materialization`
and the BlobRef/payload lookup occur only in
`needs_data && descriptor.name.is_some()`. A metadata-only projection can
therefore bypass live-file authentication. The corrected contract validates
exactly one authenticated BlobRef and its payload before that projection gate;
zero, multiple, substituted, or missing-payload cases fail closed. A valid
descriptor tombstone is exempt because it is not live content.

## Executable evidence

The model was compiled without the production crate:

```text
rustc --edition=2021 --test correction_model.rs -o /root/repos/forktree-fd2-correction-model
/root/repos/forktree-fd2-correction-model
```

It passes 5/5 tests. The model covers four descriptor corruption classes,
live-to-tombstone removal, exact-one/zero/multiple/substituted BlobRef cases,
missing and digest-mismatched payloads, metadata-only validation, tombstone
payload exemption, exact checkpoint marker identity, missing parent, and cycle
failure. The rebuilt model binary SHA-256 is recorded in the final handoff.

The verifier uses balanced Rust function extraction, not global token presence.
Its `corrected` mode is dormant on fd2 and requires structural
tombstone-to-absence handling plus pre-projection exact-one BlobRef checks.

## Preserved fd2 invariants

The source checks retain the fd2 historical migration contract: checkpoint and
working-diff functions use one caller-owned ForkTree history reader;
`checkpoint_history_from_head` and `diff_state_rows_between_commits` remain the
chronology source; local historical facade construction and tracked-state diff
fallbacks are forbidden in those migrated function scopes. The pure model
keeps exact marker/first-parent chronology and fail-closed missing/cyclic
ancestry. No wrapper, cache, compatibility path, second authority, or writer
was introduced.

## Scope and dormant adapter order

The immutable payload changes exactly six paths, all below
`test-reports/forktree-stage2-historical-correction-fd2-defects/`; the package
does not modify any production path. `SHA256SUMS` contains the exact payload
file hashes and has SHA-256
`8467e0e986a96e7e9372200b312bb10b7612d629d999962de5c42bd5c3f4e0b7`.

For a corrected compile-green successor only, run the model/source gate first,
then fresh single-threaded Memory, RocksDB, and SlateDB cells. Each cell is
capped at 20 minutes and must check tombstone removal, metadata-only and
content projections, exact BlobRef/payload authentication, cold reopen, and
fail-closed corruption. No adapter runtime is claimed by this fd2 package.
