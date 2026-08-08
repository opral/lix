# Exact b59 source/authority map

Base under review: `b59e1f11a51153e0a787a81f0f25bf104d150aaf`.

This is a read-only map of the current production seams. It does not claim
that a future history-independence cut is implemented.

| Owner | Exact path | Observed symbols/anchors | Oracle obligation |
|---|---|---|---|
| selector and root identity | `packages/lix/src/forktree/model.rs` | `GlobalSelectorV1` (around lines 620–664), `BranchSelectorV1` (around lines 666–710), `RepositoryRootV1` (around lines 72–120) | Selector identity, epoch, branch, repository/catalog roots must be authenticated and compared before use. |
| transaction publication | `packages/lix/src/forktree/publication.rs` | `PreparedPublication` (around lines 66–77); staging methods (roughly 111–224) | One caller-owned prepared publication and the existing transaction/backend commit; no independent history/canonicalization writer. |
| coherent read | `packages/lix/src/forktree/view.rs` | `CoherentView` (around lines 30–44), `open_coherent_view_on_read` (around line 380), `state_point_at_roots` | All pair measurements and corruption checks use one retained coherent view; no fresh read/fallback/cache crossing the view. |
| authenticated objects | `packages/lix/src/forktree/tree.rs` | `ObjectId`, `ObjectDomain`, `validate_root_on_read` (around 462) | Object ID, domain, node kind, edge, and root validation precede output or publication. |
| logical row/blob identity | `packages/lix/src/forktree/state.rs` | `StateKey`, `StateKeyRef`, `blob_manifest_object_ids`, state-key encoders | Final row/file/blob identity is the semantic comparison key; ordering and tombstone/NULL distinctions are retained. |
| serving/history/diff | `packages/lix/src/forktree/serving.rs` | selector loading, `open_coherent_view_on_read`, catalog/root validation, history/diff serving | Required object absence/corruption fails closed; valid absence/tombstone remains a value, not a missing-object success. |
| final-reference GC | `packages/lix/src/forktree/reachability.rs` | `load_gc_snapshot`, `authenticate_progress_roots`, `advance_gc` | Roots are observed from the authenticated selector/progress view; shared final objects survive and unreachable objects alone may be reclaimed. |

## Forbidden candidate shortcuts

The future candidate must not add a persisted canonicalization index, mutable
global cache, second Blob/row authority, compatibility reader/writer,
fallback that masks corruption, caller-text identity, or independent commit.
It may use only existing rebuildable/object-derived structures and must keep
commit parents as chronology facts. If a physical root is history-dependent,
the result may report it rather than rewriting it into a semantic identity.

## Source-only controls for the future runnable landing

These checks are requirements for a future candidate package and are not run
here:

* every pair's final digest is formed from authenticated rows/files/blob IDs,
  not event order;
* all object IDs/domain tags and all selector/catalog roots are validated;
* every candidate publication reaches `PreparedPublication` and the existing
  transaction commit exactly once;
* no alternative persisted identity/cache/fallback appears;
* all old and new roots survive cold reopen and final-reference GC;
* corruption is typed fail-closed with no partial publication.
