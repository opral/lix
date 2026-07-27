---
type: patch
---

Speed up Git replay by reusing plugin actors across serialized commit batches
that fit the engine's live-Store bound. Import batches clear session
acknowledgements after committing, so evicted actors safely cold-open instead
of turning later authoritative imports into stale-observation errors.
