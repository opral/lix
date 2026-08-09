# Frozen oracle commands

Run from the repository root, with no production source edits:

```sh
rustc --edition=2024 -D warnings \
  packages/rs-sdk-tests/examples/authenticated_splice_corruption_oracle.rs \
  -o /tmp/authenticated-splice-corruption-oracle-a33
/tmp/authenticated-splice-corruption-oracle-a33
```

Expected model output is two valid `changed=1 reused=63 cold_reopen=pass` lines,
eight corruption lines with `a33_accepts=true oracle_rejects_before_write=true`
and `rollback=pass`, followed by
`oracle=authenticated_splice_unchanged_child_closure status=pass`.

The future production adapter gate must run the same named cases against a
fresh database for each adapter, using the existing a33 focused test harness:

```sh
timeout 1200s cargo test -p lix --lib --features all-simulations \
  -- forktree::tests::authenticated_splice_unchanged_child_corruption \
  --exact --nocapture
```

Run Memory first, then RocksDB and SlateDB with flush/drop/cold reopen. For
each corruption case assert zero state/selector/receipt/object publication
writes and unchanged reopen digests. Do not claim adapter/runtime acceptance
from this model-only package.
