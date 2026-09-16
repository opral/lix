---
type: minor
---

Partial replicas load file history with fewer network round trips and retain admission and pending update watches during foreground reads.

History hydration batches independent dependencies, fetches bounded ancestor metadata, and includes small owner metadata dependencies with native objects. This release advances the sync protocol to version 19; upgrade clients and servers together. Existing repository storage does not require a reset.
