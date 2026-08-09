# Frozen retained-view identity oracle manifest

This is a TEST/REPORT-only package. It is based on immutable hot-pack oracle
`5b94b5b80e5093219dec02c0913930501655e3c3` (tree resolved by Git at review
time). No production source, adapter, mutable R2 state, PR, or merge was
changed.

## Files and source hashes

| file | SHA-256 |
|---|---|
| `model.rs` | `2280606befe8669a38b491d19eac00f89b67078e6f2a5f302b87072f78b17b0e` |
| `source_gate.py` | `87d2af7e476976622875ade2a5f145d163bf2ed290e094e2ba09f06e2c13ab84` |
| `gate.sh` | `4553b418aecb5410b4449fb76bcd444760beb5621cf61aa047ec4542b91e6dea` |
| `CONTRACT.md` | `c3ed637f1770ca4ec6029c910dce7515dd1745c5524b5c4e88fa47d2e36a0c44` |
| positive fixture | `5ddf9c139254251166d915ac41db6c7a7fe16eabb62592ed25492294db8e9aff` |
| negative second-read fixture | `a53d9fa40aaf50d1e15d3a22d1a4c584aa95b3494745ceaf920d118d6ece1662` |
| negative missing-root fixture | `c69c3063fc9b55cc8529f9b11d3d3416a27c1c4b163202e515ba7539161e138f` |

## Bounded gates

- `bash -n scripts/retained_view_identity/gate.sh`: PASS.
- `python3 -m py_compile scripts/retained_view_identity/source_gate.py`: PASS.
- `rustc --edition 2024 -D warnings model.rs`: PASS; binary SHA
  `5c970abf6954276e62a8e4a3b14613dbfe6a58ac1f5ca9e2615af5380c66d167`.
- Model output: `retained_view_identity_model=GREEN same_view=3
  cross_view_rejections=7 failed_install_no_index=true`; log SHA
  `0a7d1c1370fc9a3669dc09efc420f7c51e7b210a3eb0a194e0a3f145cf22919e`.
- Positive source fixture: GREEN; log SHA
  `b396301804a8e56002c068b47be4da5b792f9ec723b2bf5e0e178c94d2063d85`.
- Negative second-read fixture: RED for reader-local `begin_read` and absent
  unknown-domain failure; log SHA
  `c715b41e6cced0ff1d46edfa0b3a6176c185ed0e9e97381088951a20b6e50cfe`.
- Negative missing-root fixture: RED for omitted `branch_root`, no shared
  owner, and absent unknown-domain failure; log SHA
  `8fcf1a2232f36c55fe7b2ab897c740809f533f8881364fc93724714ebfc9e064`.
- Candidate-parametric calibration on base=candidate=`5b94b5b80e5093219dec02c0913930501655e3c3`: expected RED, runner exit 0;
  source log SHA `c93b797cda4d8d5c5e7d2bfd43f4c5dc3a0bc2c81e0dda0e2f166d36065fa8b4`.
  The calibrated failures are missing storage-read epoch/branch-root/
  snapshot-commit binding, no named token type, no shared raw+packed owner,
  and no explicit unknown-domain branch.
- Runner command output SHA-256 is recorded in the external handoff after
  the final immutable commit; the runner requires base RED explicitly and
  requires future candidate GREEN explicitly.

The source gate masks comments and string literals before structural checks,
rejects paths outside `packages/lix/src/forktree/`, reader-local acquisition,
fallback/compatibility/static cache authority, and installation before proof.
It is a necessary pre-filter, not a substitute for manual call-graph and
Memory/RocksDB/SlateDB corruption/reopen qualification.
