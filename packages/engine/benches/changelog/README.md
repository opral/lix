# Changelog Benchmarks

Dedicated microbenchmarks for the changelog physical layout.

Run locally:

```sh
cargo bench -p lix_engine --features storage-benches --bench changelog_scorecard
```

Smoke compile all registered changelog benches:

```sh
cargo bench -p lix_engine --features storage-benches --no-run
```

Profile workloads:

```sh
cargo bench -p lix_engine --features storage-benches --bench changelog_cpu_profile
cargo bench -p lix_engine --features storage-benches --bench changelog_visible_profile
```

Single-segment shapes:

```text
1c_1ch        1 commit, 1 change
1c_100ch      1 commit, 100 changes
1c_1000ch     1 commit, 1,000 changes
100c_1000ch   100 commits, 1,000 changes
```

Corpus shapes:

```text
10seg_10c_100ch       10 physical segments, 10 commits, 100 changes
100seg_100c_1000ch    100 physical segments, 100 commits, 1,000 changes
1000seg_1000c_10000ch 1,000 physical segments, 1,000 commits, 10,000 changes
                       heavy profile, limited operations
```

Payload shapes:

```text
no_payload            logical change refs only
small_inline          64-byte inline payload per change
large_inline          8 KiB inline payload per change
external_refs_only    snapshot/metadata refs without inline bytes
```

Key-layout shapes:

```text
clustered_keys              nearby StateRowIdentity ranges
random_keys                 deterministic pseudo-random identities
reused_keys_across_commits  same key space reused across many commits
```

Projection shapes:

```text
commit projections  header, body, full
change projections  physical_location, logical, segment
```

Lookup batch-size shapes:

```text
m_1     one change_id lookup
m_10    ten change_id lookups
m_100   one hundred change_id lookups
m_1000  one thousand change_id lookups
```

Reference-system coverage:

```text
Dolt/FDB-style scale:       many-row, many-segment, and batch-size lookup shapes
Sapling-style log/index:    raw segment append vs indexed append, one-segment incremental append
                            into an existing corpus, segment iteration, index lookup, prefix scan,
                            lag/empty/stale/corrupt rebuild
Neon-style layer shapes:    same-segment vs scattered-segment reads and live/dead GC
Neon ingest shape:          payload size, key layout, concurrent reads
DataFusion/DuckDB shape:    projection-sensitive reads that catch over-hydration
Git-like packing boundary:  logical commit/change ids across relocatable physical segments
Segment-local lookup:       decoded segment directory/index lookup, matching Dolt table-index and
                            Neon layer-map style search benches
Inline payload placement:   payload_ref -> offset/len -> inline bytes resolution, matching
                            frame/span lookup patterns in log and block containers
```

Benchmark backends are local to `benches/changelog/backends/` so changelog
benchmarks stay isolated from the broader storage benchmark harness:

```text
mem_unit         in-memory ordered map
sqlite_tempfile  SQLite temp file
redb_tempfile    redb temp file
rocksdb_tempdir  RocksDB temp directory
```
