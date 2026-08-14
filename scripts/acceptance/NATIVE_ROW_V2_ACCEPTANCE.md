# NativeRow v2 acceptance package

This package is candidate-parametric and contains no production changes. It
rejects the branch-UUID-bearing `LIXFCV` v1 row rather than preserving a
compatibility reader.

## Durable contract

- `LIXFCV v2` is the sole current-row wire format.
- Durable identity is `global|local domain + schema key + typed PK + optional
  file owner + typed layout + semantic value`. Selected/source branch UUID is
  not durable row identity.
- The authenticated chain remains selector → branch snapshot → global/local
  root → ordered-tree key → current-pack object/ordinal → canonical row key,
  layout/domain identity and semantic digest.
- Logical branch identity comes from the retained view. Immutable branch
  creation shares the source local root and rewrites no state rows or current
  packs.
- v1, unknown version/domain, truncation/trailing bytes, same-size key/body
  substitution, global/local transplant, cross-root/cross-branch graft and pack
  domain mismatch fail closed. There is no fallback, dual decoder/writer or
  embedded branch-owner self-attestation.

## Replay

```sh
python3 scripts/acceptance/test_native_row_v2_acceptance.py
python3 scripts/acceptance/native_row_v2_acceptance.py \
  --root /path/to/immutable-candidate --expect-head "$HEAD" \
  --output /tmp/native-row-v2/source-authority.json
scripts/acceptance/run_native_row_v2_branch_profile.sh \
  /path/to/immutable-candidate /tmp/native-row-v2
```

The profiler runs N=1K/10K/50K on RocksDB and SlateDB. It records per-space
logical row/key/value deltas, settled filesystem objects/bytes and public
branch latency. Candidate-private controls prove byte-identical child and
grandchild local roots, no state/current-pack rewrites, and a fresh cold-reopen
read. Any branch-create delta that grows with inherited row count is a blocker.
