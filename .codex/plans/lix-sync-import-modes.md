# Sync import modes: proposed next step

The importer still accepts `SyncImportPurpose` plus independent optional history boundaries, replica publication state, and expected account identity. These inputs describe coupled operations, but their relationships must be rediscovered throughout admission and publication.

Preserve the valid operations and encode them directly:

| Variant | Required context | Publication contract |
| --- | --- | --- |
| Authority push | Request and explicit authorship policy | Admit authored objects and refs; append the authority event/cursor when needed. Production admission carries the authenticated account. Audit the trusted internal/test helper before narrowing it. |
| History hydration | Commit bodies, boundaries and boundary rows | Admit immutable history without ref updates or live replica/authority cursor advancement. |
| Replica publication | Request and replica publication state, including expected receipt/cursor | Publish refs and replica receipt atomically, preserving reset and branch-set fences. |
| Replica ref repair | Ref repair request | Repair a local replica ref without advancing a publication receipt; invalidate the pending upload plan as today. This is a live upload-planning path, not dead code. |

First PR: replace the purpose/options argument product with these internal request variants and migrate every caller. Keep one shared immutable-object admission pipeline. Scope account policy to authority pushes, boundaries to history, and receipt/reset context to replica publication. Preserve supported legacy migrations and the new retained-work recovery path.

Second PR: make validation produce an explicit publication action, then remove redundant purpose/option checks whose invariants are established by that action. Preserve both atomic fast-forward and staged immutable-object publication where their storage requirements differ.

Regression coverage must prove authorship and immutable identity, blob availability, sparse history topology, history-only cursor/ref isolation, branch CAS, receipt/cursor atomicity, reset fencing, retained recovery, and repair without publication.

Recommendation: make the first PR behavior-preserving. Removing a valid sync operation would be a separate product decision backed by caller and recovery evidence. No sync-mode implementation changes are part of the four current cleanup PRs.
