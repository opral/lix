# BranchRef and selector production-readiness package

Status: **TEST/REPORT-ONLY; e1af source gate RED by design**.

This package is anchored to the accepted e1af production source and binds the
approved semantic BranchRef v4 and H4 production map. It does not edit
production, compile Cargo, run adapters, or claim runtime qualification.

## Immutable provenance

Production anchor:

~~~text
commit e1af471b9ab0f598dafa7c2ddec7867667c81740
tree   bfa0d271a723da8250ab76ada16fda90926f1099
parent b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
~~~

Accepted transport ref containing the frozen e1af calibration:

~~~text
origin/codex/forktree-w3-e1af-selector-rebind-e1af
head 396f4a4b213ff1207a248599b356cf8accc09523
tree 29e9a0c3ece3b64edbf6f081a24bf153c96d862b
~~~

Approved semantic v4 binding:

~~~text
head 32200a21f4cb7a77276ff619179b2c05687ffd2a
tree 5eef6528fcde96417c0303d3c1df78c48e257ffb
review report SHA-256 3cca3a49a4578720dfac22b59e2412916fa90a73cecdf5c9aa5daaa4d4aedec4
~~~

H4 e1af production map report SHA-256:

~~~text
31f40c3d11db4baaac09f36263de2c55cdc356a912debcfe5d3e6aa2613fad66
~~~

## Authority contract

The future production successor must have exactly one authenticated
GlobalSelectorV1 in SELECTOR_SPACE and one BranchSelectorV1 per branch. A
caller owns one retained CoherentView/StorageRead. Every create, switch,
advance, delete, retire, checkpoint/GC transition uses one prepared
publication, one global epoch/selector CAS plane, and one backend commit.

Required behavior includes:

- canonical global and branch selector key/root/owner/generation authentication;
- create, switch, advance, delete, retire, GC, and cold-reopen chronology;
- same-owner stale CAS versus unrelated-owner rejection;
- missing root, malformed selector, cycle, epoch-gap, and catalog mismatch
  fail-closed before writes;
- retained-view roots survive publication/GC until release, then reclaim;
- empty undo/redo and unsupported cohorts make no durable writes;
- no BranchRefReader, BranchHeadControl, cache, flat-row writer/projection,
  compatibility reader, fallback, or second selector authority.

The standalone model encodes these controls and negative fixtures without
introducing a persisted format.

## Exact path policy

The readiness package itself is allowed only under:

~~~text
test-report/branch-ref-production-readiness-e1af/
~~~

Future production implementation may modify only the authorized ForkTree
selector/view/publication owners and their explicitly mapped callers. The
following are forbidden as new or retained authority owners:

~~~text
packages/lix/src/branch/refs.rs
packages/lix/src/branch/context.rs
packages/lix/src/branch/stage_rows.rs
packages/lix/src/sql2/branch_ref.rs
BranchRefReader
BranchHeadControl
BranchHeadControlCache
MUTATION_REVISION_SPACE
TRACKED_MUTATION_REVISION_SPACE
CachingBranchRefReader
BranchRefFallback
SecondBranchAuthority
DualSelectorAuthority
~~~

## e1af source RED calibration

Run the source-only verifier:

~~~sh
bash test-report/branch-ref-production-readiness-e1af/verify_readiness_source.sh \
  /tmp/lix-branch-ref-readiness-e1af-1786195688
~~~

Expected exit status is 1. The exact inherited e1af calibration is:

~~~text
legacy_control_generation 58
checkpoint_history 1139
snapshot_pin 16
selector_epoch 770
mutation_revision 24
~~~

The source gate prints RED because old control/generation, mutation/revision,
reader/cache, and flat-row authority residues remain. Broad checkpoint and
selector counts are inventory only; they are not silently treated as proof of
deletion.
