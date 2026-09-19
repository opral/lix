---
type: patch
---

Browser OPFS repositories now share a dedicated worker that owns both the engine and storage, removing the separate storage-worker lifecycle.

Opening, credential callbacks, finite worker operations, and shutdown are bounded. After owner loss, surviving tabs elect a replacement and restore acknowledged session context and live observations. Interrupted transactions and snapshot streams fail explicitly and must be restarted. Recovery is bounded. Potentially writing operations whose acknowledgement is lost report `LIX_WRITE_OUTCOME_UNKNOWN` and must not be blindly retried. SharedWorker support is no longer required; OPFS, dedicated workers, BroadcastChannel, and Web Locks are required.
