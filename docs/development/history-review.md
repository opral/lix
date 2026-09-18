# History and review development

This long-running development branch tracks the engine side of the coordinated
Atelier and Lixray history work. Keep its pull request in draft while collecting
cold-replica evidence. No engine behavior changes are included in this baseline.

## Current evidence

The related loading investigation is documented in
[opral/lixray#416](https://github.com/opral/lixray/issues/416). Its capture showed
sequential native dependency hydration and repeated admission, not evidence that
file contents were materialized. Do not interpret request count alone as content
reads or as click-to-render latency.

Current `sql2/providers/mainline.rs` stops when selected commit IDs have been
visited and supports a finite position bound. `sql2/providers/diff.rs` projects
file content only when requested; its existing
`file_content_filters_precede_storage_reads_for_diff_and_history` regression
checks that metadata projection does not open blob bytes.

The companion Atelier change requests an initial window of 20 checkpoints, one
older log endpoint, and file metadata only for the selected checkpoint IDs.
Older windows require an explicit user action. Lixray gates directory change
markers on an active review and adopts the Atelier spacing fixes.

## Qualification before further engine changes

- Capture a fresh partial replica with dense and sparse file history, including
  more than 20 checkpoints, and compare authority, cold replica, and warm offline
  results.
- Record native metadata/object requests, admission calls, SQL attempts, and bytes
  separately. Verify no content-blob reads for the timeline's metadata projection.
- Verify the first window excludes older diff plans, and loading another window
  preserves ancestry order, rename/deletion paths, and the previous endpoint.
- Measure release WASM with realistic network delay and OPFS. Do not claim a
  production latency improvement from in-memory tests.

Any engine changes must use canonical in-memory behavior tests and the simulation
and doctest checks required by `packages/lix/AGENTS.md`.
