---
type: patch
---

Recover partial replicas inside awaited operations.

Cold SQL renews expired baseline leases, reconciles pending changes, and retries
against the recovered state without exposing internal recovery instructions.
Unchanged baselines retain local edits and frozen upload identities. Branch
switches await pending synchronization internally. Repeated coherent baseline
changes can restart an uncommitted operation without replaying completed writes.

Unsupported local reconciliation adopts server-authoritative state only after
pending publication identities are safely settled or fenced. Bounded opening
and network-free reads with valid resident inputs remain unchanged.

Online opening replaces incompatible replica caches with an authenticated partial
epoch without migrating their history. Old banks remain detached and intact.
Format-78 offline migration validates native checkpoint identities rather than
retired checkpoint marker rows.

Explicit transactions fetch missing immutable inputs while keeping mutable reads
pinned. A snapshot whose required inputs are no longer retained fails with a
normal transaction conflict instead of waiting for a publication it prevents.

Local partial snapshot exports now preserve the actual resident cache and pending
edits instead of downloading the authority snapshot. Partial snapshots carry an
explicit header flag and restore as partial replicas; older readers reject them.

Deploy SDK and server together for sync protocol 16. Older peers fail version
negotiation before attempting the new authenticated upload-abandonment fence.
Repository storage format remains unchanged.
