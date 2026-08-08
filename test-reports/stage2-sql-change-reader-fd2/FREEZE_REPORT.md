# SQL changelog-reader oracle freeze — direct successor

This immutable package is a TEST/REPORT-only direct successor to blocked
`3221833f879b6e2cc965039c0c3cabdd0709e83e`, rooted at
`fd2be256d763f17e9f127d4c984e36fba191cb82` (tree
`20110ca5e3c33d34217630fff0a2b784b545317a`). Production was not edited.

The package freezes the smallest next SQL compiler closure: replace the
provider's raw storage/tracked-state/changelog/commit-graph ownership with
one caller-owned ForkTreeReadFacade over the operation's retained
CoherentView/StorageRead. The direct ChangeCatalog and derived CommitCatalog
surfaces must authenticate domain, kind, key, embedded identity, ordering,
membership, and back-edges before SQL materialization. Authenticated absence
is the only absence result; missing or malformed required records, wrong-kind
or wrong-domain substitutions, and duplicate logical IDs fail closed before
sort/output/limit.

The exact call graph, allowed future paths, source-negative checks, executable
fixture model, balanced structural proof, and replay order are in `README.md`,
`SQL_CHANGE_READER_CASES.tsv`, `fixtures/`, `verify_source_contract.sh`, and
`verify_contract_v2.py`. `RED_CALIBRATION.md` is unchanged and records the
same deterministic expected RED against fd2 and inherited compiler log hashes.

The successor closes the prior oracle-quality blocker: fixture validation now
parses and executes every case, and the source gate tokenizes Rust while
skipping comments/strings, balances delimiters, scopes function definitions
and calls, binds both provider calls to the same retained reader expression,
and proves both caller constructors consume `self.read_store` without a
fresh `begin_read`.

Static/model evidence on the fd2 anchor: all ten fixture cases emit
`MODEL-PASS`; the model-only gate rejects an isolated empty `wrong_kind.json`
with `RED-11` (log SHA-256
`540ddd68b8010edc14803c8b1f2106a04193f39fad0b41c4f6c7cf8262587b54`). A
synthetic candidate with balanced calls and retained-read constructors emits
`STRUCTURAL-PASS`; changing only the exact-lookup reader to `other_reader`
emits `STRUCTURAL-RED`. The real fd2 source remains intentionally structural
RED, while the historical wrapper output remains exactly
`74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5`.

This ref contains no production implementation, runtime claim, PR, merge, or
compatibility/fallback path. Apply artifacts only to a disposable candidate
after explicit implementation authorization and independent source review.
