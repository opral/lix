# SQL changelog-reader oracle freeze

This immutable package is a TEST/REPORT-only handoff rooted at
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

The exact call graph, allowed future paths, source-negative checks, fixture
matrix, and replay order are in `README.md`,
`SQL_CHANGE_READER_CASES.tsv`, `fixtures/`, and
`verify_source_contract.sh`. `RED_CALIBRATION.md` records the deterministic
expected RED against fd2 and the inherited compiler log hashes.

This ref contains no production implementation, runtime claim, PR, merge, or
compatibility/fallback path. Apply artifacts only to a disposable candidate
after explicit implementation authorization and independent source review.
