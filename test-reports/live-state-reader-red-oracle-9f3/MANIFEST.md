# Immutable package manifest

Package: `live-state-reader-red-oracle-9f3`

This is a test/report-only successor made from the exact target snapshot. No
production file is changed and no Cargo target is created.

## Provenance

```text
target head: 9f3c703e953440cde1d60b1511467c4337648c8f
target tree: 51a0026c0c3eced6fdaa5e5ed4824111377f086c
target parent: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
```

## Package files

| File | Purpose | Expected direct check |
| --- | --- | --- |
| `READER_RED_ORACLE.md` | source proof, red cases, correction contract | read-only |
| `reader_red_oracle_model.rs` | dependency-free focused model/public test | intentionally not wired or run on 9f3 |
| `verify_source_contract.sh` | exact-head source verifier | exit `1` with the old-head red findings |
| `OLD_9F3_RED_OUTPUT.txt` | captured verifier output | exit status `1` |
| `MANIFEST.md` | provenance and command manifest | read-only |

## Commands actually run

```text
git show <target>:packages/lix/src/live_state/{context,derived,forktree_reader,reader,types}.rs
git diff --check
bash -n test-reports/live-state-reader-red-oracle-9f3/verify_source_contract.sh
test-reports/live-state-reader-red-oracle-9f3/verify_source_contract.sh \
  "$PWD" 9f3c703e953440cde1d60b1511467c4337648c8f
```

The verifier was expected to return `1`; that is the frozen red result, not a
tool failure. No `cargo`, native build, benchmark, adapter, database, or
runtime command was run.

## Scope guard

The package may be handed to R5/R2/R4 as an oracle only. It does not authorize
production edits, a new durable owner, a compatibility reader, a fallback, a
cache, a storage format, or a PR/merge. Any future correction must first make
the source verifier's five red findings disappear while retaining the two
explicit fail-closed control guards, then run the focused Memory/RocksDB/Slate
semantic tests named in `READER_RED_ORACLE.md`.
