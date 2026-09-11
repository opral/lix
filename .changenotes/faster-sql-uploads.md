---
type: patch
---

Large SQL uploads use less redundant decoding, copying and request fingerprinting work. The server protocol no longer imposes a default 64 MiB request-body limit; hosts can still configure an explicit byte budget.

SQL requests keep the same public API. Retry keys recorded with the previous fingerprint format return `409 LIX_IDEMPOTENCY_KEY_REUSED` after upgrading, without re-executing the mutation. Reconcile uncertain pre-upgrade requests before issuing new keys. Existing repository content and storage formats are unchanged.
