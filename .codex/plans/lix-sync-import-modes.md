# Sync import modes

The importer now accepts one internal `SyncImport` variant instead of `SyncImportPurpose` plus independent optional history boundaries, replica publication state, and expected account identity. Public APIs, sync wire types and storage formats remain unchanged by this refactor.

The four supported operations are encoded directly:

| Variant | Required context | Publication contract |
| --- | --- | --- |
| Authority push | Request and explicit authorship policy | Admit authored objects and refs; append the authority event/cursor when needed. Production admission carries the authenticated account. The trusted unauthenticated helper and authorship policy are test-only. |
| History hydration | Commit bodies, boundaries and boundary rows | Admit immutable history without ref updates or live replica/authority cursor advancement. |
| Replica publication | Request and replica publication state, including expected receipt/cursor | Publish refs and replica receipt atomically, preserving reset and branch-set fences. |
| Replica ref repair | Ref updates only | Repair a local replica ref without advancing a publication receipt; invalidate the pending upload plan as today. This is a live upload-planning path, not dead code. |

Implemented: replace the purpose/options argument product with these internal request variants and migrate every caller. Keep one shared immutable-object admission pipeline. Scope account policy to authority pushes, boundaries to history, and receipt/reset context to replica publication. Preserve supported legacy migrations and the new retained-work recovery path.

Possible later follow-up: make validation produce an explicit publication action, then remove redundant purpose/option checks whose invariants are established by that action. Preserve both atomic fast-forward and staged immutable-object publication where their storage requirements differ.

Regression coverage must prove authorship and immutable identity, blob availability, sparse history topology, history-only cursor/ref isolation, branch CAS, receipt/cursor atomicity, reset fencing, retained recovery, and repair without publication.

The refactor preserves supported production behavior. History cannot carry ref updates, repair cannot carry new commit bodies or receipts, and receipt/reset context belongs only to replica publication. Three test fixtures that previously used receipt-less body import now preload commits through history hydration. Removing any supported sync operation remains a separate product decision.

Validation: 3,728 engine tests passed with `all-simulations,server-protocol` (69 skipped); all 10 doctests passed. Sub-agent review found no correctness issues. Its coverage recommendation was addressed with an active receipt-less ref-repair regression covering stale CAS rejection, both ref coordinates, unchanged receipt bytes and authority cursor, upload-plan invalidation and no-op replay.
