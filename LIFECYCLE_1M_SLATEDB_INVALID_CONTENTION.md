# 1M SlateDB lifecycle run: INVALID_CONTENTION

The paired 1M SlateDB lifecycle attempt on 2026-08-05 is invalid for performance
claims. Candidate and baseline were launched concurrently on the same host:

- candidate: `0f3428c5943e63840d55199915b2f29402067f72`, session `48779`, PID `46504`
- baseline: `a199befdad3e2b877e4618880d307e9f067f0f21`, session `43544`, PID `46505`
- workload: `worker slatedb 1000000 sparse 1`
- candidate worktree: `/private/tmp/lix-lifecycle-4e05`
- baseline worktree: `/private/tmp/lix-lifecycle-main-a199`

No lifecycle JSON report was emitted. Both workers were interrupted with SIGINT
at exit code 130. Wall time, CPU time, RSS, compaction, storage growth, and all
other performance results from this attempt are `INVALID_CONTENTION` and must
not be used as evidence.

Retained diagnostic attribution only: samples taken while the workers were
running showed SlateDB `TableStore::read_blocks_using_index`, SST block decode /
decompression, cache population, and size-tiered compaction activity, with the
Lix file-path write executor on the active stack. These observations are not a
qualified regression or optimization signal.

Required follow-up: rerun baseline and candidate sequentially, alternating one
worker at a time, with host admission recorded immediately before each sample
and no other benchmark/build/compaction workload active.
