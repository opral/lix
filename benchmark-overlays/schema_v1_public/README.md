# Schema-v1 public qualification overlay

This is a benchmark-only workspace member. Its workload source is applied
byte-for-byte to both comparator checkouts; it does not modify Lix production
code or call engine-private benchmark hooks.

```sh
cargo run --release -p lix-schema-v1-public-qualification -- \
  verify rocksdb all 100 10 1
cargo run --release -p lix-schema-v1-public-qualification -- \
  run slatedb oltp 1000 10 5
```

Arguments are `mode backend suite N D samples`, where suite is `oltp`, `vcs`,
`olap`, `file`, or `all`. `verify` runs one untimed correctness pass regardless
of `samples`. File payload size is controlled by `LIX_BENCH_PAYLOAD_BYTES`
(default 1 MiB); VCS history depth by `LIX_BENCH_HISTORY` (default 10).
