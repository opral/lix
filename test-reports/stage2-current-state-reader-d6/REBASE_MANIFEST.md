# Current-state reader acceptance package rebinding

Status: TEST/REPORT-ONLY immutable package; no production edits.

- Package base/head: d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768
- Package base tree: 641654079f60fcd1c9ff9ccbbd06d3edcabe4096
- Package parent: 1f742a382c755399b8a49ab536c4f6dc55fffdd8
- Previous package semantics: exact frozen 1f742 reader checklist/oracle; only provenance is rebound.
- d6 correction gate: exact CommitRecord fail-closed verifier is preserved and must PASS on d6; its historical 1f742 red result remains recorded in COMMIT_RECORD_FAIL_CLOSED_RED_CONTROL.md.

Permitted successor production paths remain only live_state/context.rs, derived.rs, mod.rs,
reader.rs, types.rs, visibility.rs and tracked_state/context.rs, diff.rs,
row_materialization.rs, types.rs. The next reader candidate must not change
any other production path, including sql2/providers/change.rs beyond the already
approved d6 correction.

Review commands:

git diff --check d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768 HEAD
bash test-reports/stage2-current-state-reader-d6/verify_source_contract.sh d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768 HEAD $PWD
bash test-reports/stage2-current-state-reader-d6/verify_commit_record_fail_closed.sh $PWD
cargo fmt --all -- --check
cargo check -p lix --lib

The runtime R1-R19 oracle is unchanged and remains test/report-only until an
immutable reader successor is published.

File SHA-256 values:

- CURRENT_STATE_READER_CHECKLIST.md: `5bb3323edb4174c576f26bfd4ecada217bae1953e2c4a5038e83ece49b719261`
- CURRENT_STATE_READER_ORACLE.md: `39de558ffb3af292f64a85df0de670b2a47848ee00541051ce01857324f92f0b`
- verify_source_contract.sh: `ba6c372b74bc80e958122d7593d642e151fa3164bb7dd5fc5da1d4ccedfa80be`
- COMMIT_RECORD_FAIL_CLOSED_RED_CONTROL.md: `c98d254f3d2192a3af3d4aebd44e8f62e4f2aa0a6de67e80b0cc511ec37f8198`
- verify_commit_record_fail_closed.sh: `d63d202d64a6ea28e797aea5a948f6b7a3b42087bd8a6228cd81653b6788d788`
