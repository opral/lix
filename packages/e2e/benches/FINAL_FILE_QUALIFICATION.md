# Final file and large-payload qualification contract

This TEST/BENCH-only overlay freezes the final file qualification workload on
production base `91d059332bb00df0aaa4fad5babb6f7018175e25` (tree
`49173b0580aa328f03e1417af0907a8dc7d1b2de`). The identical overlay is intended
to be cherry-picked onto the future carrier-composed production head. It does
not alter Lix production code or storage formats.

## Public semantic gates

The retained `lix_e2e` tests are the parsed-file correctness oracle:

```text
v3_markdown_byte_roundtrip_rocksdb_lifecycle_and_17_file_batch
v3_markdown_byte_roundtrip_slatedb_lifecycle_and_17_file_batch
v3_markdown_certified_open_sparse_successor_history_and_reopen
v3_markdown_same_paragraph_branch_merge_composes_word_edge_inserts
```

Together these exercise public parsed Markdown create/read/update, typed plugin
projection, batch publication, branch visibility, semantic merge, history,
checkpoint, and cold reopen. Tests must remain byte-identical between base and
candidate.

The existing public `large_binary_multimedia_qualification.rs` workload is
registered without changing its source. For both RocksDB and SlateDB at 64 MiB
and 256 MiB it exercises:

- resumable public file upload and exact full read;
- authenticated middle range read;
- one localized overwrite and public `lix_diff` query;
- wrong-base corruption/substitution rejection with no commit advance;
- authenticated append with bounded writes and chunk reuse;
- checkpoint, branch snapshot/read, and merge preview;
- flush/drop/cold reopen and full digest verification;
- main deletion while a shared branch reference retains the payload.

Each operation emits wall time, CPU ticks, allocation bytes, process RSS HWM,
logical backend read/write calls, keys and bytes, backend committed bytes, disk
delta, semantic digest, sharing, and final settled disk. Five isolated-process
samples produce p50/p95 summaries. Temporary databases are deleted only after
their terminal metrics and digests are recorded.

## Frozen execution order

1. Verify exact harness head/tree, parent production head/tree, changed-path
   allowlist, workload SHA, binary SHA, Rust and Cargo versions.
2. `cargo fmt --all -- --check` and `git diff --check`.
3. Build the unchanged large-file bench in release mode.
4. Run the four exact parsed-file tests above with `--exact`, `--nocapture`, and
   `--test-threads=1`.
5. Run the 5 x 2 x 2 large-file matrix through
   `run_final_file_qualification.sh`.
6. Require every cell to emit a terminal result and verify SHA256SUMS.

No operation may be relabeled if unsupported. The carrier candidate is not
qualified until this same overlay and command set complete against its exact
immutable head.
