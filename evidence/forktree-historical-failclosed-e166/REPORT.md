# Historical point/scan fail-closed oracle — e166

Status: TEST/REPORT ONLY. This package contains no production correction and
does not claim runtime qualification.

## Frozen source

The source under test is exact accepted e166:

| item | value |
|---|---|
| head | `e1666edd0b4d814a88d985086ecc5a477b5d32e6` |
| tree | `c680bd7e7f7b70cd784676515839af2dcbbc7917` |
| parent | `3def82e48ed74ab3d914867767e3bf06def3ffc2` |
| parent..head full-index binary diff SHA-256 | `b335b411f70b1c141f9d442f9d84cf2106f1fa864c82c58a726b3fdcdade6de6` |
| stable patch ID | `c93b5840be35f51e670486b0dabd48a239b712a4` |

The report-only branch adds only the files in this evidence directory. No
production source, adapter, format, reader, writer, cache, or compatibility
path is changed.

## Source map

The relevant path is:

1. `forktree/serving.rs:756-777` authenticates the selector, repository-root
   object, and root envelope. Missing selector/object and malformed/wrong-kind
   root bytes fail through `required_full_value` or typed decode.
2. `forktree/serving.rs:668-719` loads the CommitCatalog entry for the
   requested commit, decodes the commit object, validates catalog/topology and
   retained members, then calls `state_point_on_read` on the passed read.
3. At `serving.rs:679-688`, a missing CommitCatalog entry returns `Ok(None)`.
4. `forktree/view.rs:288-323` maps the point result with `rows.push(value.map(...))`,
   so that missing commit is returned to callers as the same empty slot as a
   valid absent key.
5. `forktree/serving.rs:1235-1261` correctly returns `Ok(None)` for a missing
   key after a validated local/global root lookup, and rejects a global
   tombstone.
6. `forktree/view.rs:304-309` preserves tombstone, JSON null, and value as
   distinct `HistoricalStateRow` states.
7. The point and exact-batch functions accept a caller-owned `read`; their
   source bodies do not begin a second read, retry, or consult a fallback/cache.

## Calibrated RED

Run the source-only verifier from the repository root:

```bash
bash evidence/forktree-historical-failclosed-e166/source_verifier.sh --expect-red
```

Expected result is process exit `0` with a RED report. Running without
`--expect-red` exits `1` after printing the same defect, making accidental use
as an acceptance verifier visible.

The discriminating matrix is:

| fixture | required result | e166 source result | status |
|---|---|---|---|
| valid commit + valid root + absent key | authenticated absence | `Ok(None)` | green |
| missing CommitCatalog commit | corruption/error | `Ok(None)` | **RED** |
| missing root object | corruption/error | object load error | green source path |
| wrong-kind/substituted root | corruption/error | typed decode/tree error | green source path |
| malformed catalog/root | corruption/error | typed decode error | green source path |
| valid tombstone | tombstone | distinct cell | green source path |
| valid JSON null | null | distinct cell | green source path |
| valid JSON value | value | distinct cell | green source path |
| all calls | same retained read, no fallback/retry/cache | passed read only | structural green |

The RED is not that e166 lacks corruption decoding. The RED is the specific
authority ambiguity: a missing CommitCatalog commit is converted to `None`
before the state root is authenticated, and exact-batch materialization then
looks like a legitimate missing row. A correction must make commit/root
absence a typed error while retaining `None` only for a missing key under a
validated commit/root.

## Pure model

`model.rs` is a dependency-free model of the required post-correction result.
It distinguishes the four valid cell outcomes, maps every missing/malformed/
wrong-kind root or catalog to `Corruption`, and rejects a trace containing a
second read, retry, fallback, or cache. It is intentionally not a substitute
for the adapter oracle.

The model's required invariant is:

```text
validated commit + validated root + absent key = Absent
anything before commit/root validation = Corruption
```

The exact pure-model and future Memory/RocksDB/SlateDB commands are frozen in
`FUTURE_COMMANDS.md`.

## Acceptance boundary

Do not accept a future correction unless all three adapters pass the identical
matrix after cold reopen. The scan path must fail before emitting an empty
batch for missing CommitCatalog/root data. It must use one retained read and
must not add a legacy reader, fallback scan, retry, cache, persisted marker,
format change, or second authority.
