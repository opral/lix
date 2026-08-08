# SQL changelog one-read/one-facade correction oracle report

Status: `TEST/REPORT-ONLY / EXPECTED RED`

This immutable package is anchored directly to c899. It adds no production
source, performs no build or adapter/runtime execution, and claims no SQL
semantic result. The purpose is to catch the exact source/read-authority
defects identified by the c899 second review before a future production child
can reach compiler/runtime qualification.

## Provenance and anchors

```text
base/head:       c8992e070a9a988a695bdb77f9a49e214431a5bc
base tree:       3f154834ae88b69a71c373b28af13eebc4e42c4b
parent:          e1af471b9ab0f598dafa7c2ddec7867667c81740
parent..base diff: 854629db1d8d4d3f07aee09154863b60ab095e6714eae84a9e3c2dec1fecb594
base patch-id:   6452320dc67df83314b0d64ffbc628717881e488

v4 oracle:       d36e1fe34c4bf93c01478b876a5f73a8dccd33de
v4 tree:         7fcaeb5f965d5c8b641e5e82a741c75cc18365b6
v4 exact RED:    74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5
checklist SHA:   53e65da95f8127dd2c7f785c1792b174085853e42f84672a0d3ec6fcc88ff677
f1dc package:    f1dcfb94d7ad46dfa237e0518acc58780b45ed25
```

The accepted f1dc runtime package remains dormant. Its reported c899 compiler
frontier is 133 errors / 9 warnings; this oracle intentionally stops before
compiler, Memory, RocksDB, or SlateDB work.

## Oracle contents

The package contains one Python structural verifier, one positive fixture, and
two discriminating negatives:

```text
verify_one_facade.py
fixtures/valid_shared_reader.rs
fixtures/two_facades.rs
fixtures/separate_history_graph_reader.rs
```

The verifier compares `c899..HEAD` with rename awareness and rejects every
path outside this package. It uses balanced function/struct extraction and
call-argument checks, rather than only global token counts.

## Contract enforced

The SQL read-session boundary must bind exactly one operation-owned
`ChangelogQuerySource` and exactly one operation-scoped `CommitGraphReader`
over the same retained `StorageRead`. That source identity or exact `Arc`
clone must flow into `lix_change`, `lix_diff`, history, and working-diff
providers. The one graph capability is explicitly allowed for this narrow
correction; W1a owns its later semantic replacement/deletion. The oracle does
not require total `CommitGraphReader` deletion.

The source gate rejects:

* an inline or repeated source acquisition at the read boundary;
* more than one facade construction in session, transaction, or test source
  factories, including a history-specific facade;
* provider calls to `ctx.changelog_query_source()`,
  `ctx.history_query_source()`, or `ctx.commit_graph()`;
* provider-local `ForkTreeReadFacade::new`, `CommitGraphContext::new().reader`,
  `begin_read`, raw `store`, cache/reset, compatibility, fallback, or writes;
* history/working-diff registration through a separately constructed source;
* chronology receiver/call arguments that are not the shared graph identity or
  the exact `query_source.forktree_reader` identity;
* `change.rs` legacy tracked-state/changelog/raw-space/index routes; and
* `diff.rs` closure-local facade construction or mismatched before/after reader.

The negative facade fixture has two constructor expressions in one read
boundary and must be rejected. The negative history fixture receives a shared
graph but constructs and calls a local `CommitGraphContext` reader; it must be
rejected. The positive fixture binds one facade, shares it with change/diff/
history, and uses the shared graph capability for chronology; it must pass its
local checks.

## c899 calibration

Command:

```text
python3 test-reports/stage2-sql-change-one-facade-oracle-c899/verify_one_facade.py \
  <detached-c899-or-successor-worktree>
```

Observed exact result on the detached c899 worktree:

```text
ONE_FACADE_ORACLE=RED
exit=1
```

The RED includes the expected defects: `HistoryQuerySource` retains raw
`store`; both SQL session boundaries acquire neither one bound source nor one
shared graph; `register_read`/`register_transaction` do not accept those
capabilities; catalog registration calls context source/graph constructors;
history/working-diff uses a separate source; and session, transaction, and
test history factories construct a second facade.

The package's `git diff --check` passes. No production path was changed. The
frozen c899 source positives remain only observations: `change.rs` uses the
typed ForkTree catalog methods and `diff.rs` propagates its reader through the
diff closure. They do not override the one-facade/shared-graph blocker.

## Deferred runtime contract

After a future immutable child passes this source gate and bounded compiler
checks, reuse f1dc's <=1200-second Memory/RocksDB/SlateDB cells. They must
preserve all ten v4 direct/derived/absence/missing/malformed/wrong-kind/
identity/duplicate/order-before-limit controls, cold-reopen digest parity for
public `lix_change`, history, and `lix_diff`, exactly one retained read and
zero read-phase writes. Any second read/facade/graph, digest drift, accepted
corruption, or timeout remains an immediate rejection.
