# W1b-1 merge-analysis correction oracle v2

This is a test/report-only successor. It contains no production source, Cargo
manifest, adapter, benchmark, runtime, PR, or merge change.

The package is anchored to exact e1af for deterministic five-RED calibration,
but its verifier accepts a future candidate commit/ref as an argument. It
checks the whole candidate production diff against the fixed W1b-1 allowlist,
extracts balanced Rust functions, and verifies the operation-owned reader
argument and aliases rather than relying on token presence alone.

## Required structural contract

- transaction/context.rs::forktree_read_facade must wrap the already-retained
  opening_read and must not acquire a new read.
- Each merge-analysis caller must acquire exactly one facade from the
  transaction, pass that exact alias to analysis::analyze, and avoid a second
  read, graph construction, raw store, fallback, cache, or alternate authority.
- analysis.rs::analyze must accept the facade/view as an explicit typed
  argument, use it for the merge reads, and reach a CoherentView through the
  facade rather than constructing a fresh reader.
- Legacy TrackedStateStoreReader, diff_commits, with_opening_tracked_reader, and
  equivalent authority tokens are forbidden in the merge-analysis closure.
- The reader-only slice returns authenticated owner rows/conflict groups and a
  deterministic result digest; publication/prepare/commit remains outside this
  slice and is asserted absent from the standalone model.

The fixture suite includes one positive candidate shape and five negative
candidate shapes. The source gate must accept the positive fixture and reject
each negative fixture before it evaluates the real candidate.

## Independent gates

    bash test-reports/w1b1-merge-analysis-e1af-v2/verify_source_contract.sh
      <candidate-worktree> <candidate-commit-or-ref>
      e1af471b9ab0f598dafa7c2ddec7867667c81740

    rustc --edition=2024 --test -D warnings
      test-reports/w1b1-merge-analysis-e1af-v2/merge_analysis_oracle.rs
      -o <isolated-target>

    <isolated-target> --nocapture --test-threads=1

Runtime, adapters, production compilation, PR publication, and merge approval
are deliberately outside this package.

