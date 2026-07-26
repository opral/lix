---
type: minor
---

Remote SQL writes now use an `Idempotency-Key` to safely recover from a lost response.

The remote SDK generates a key for each logical execute call and lets applications reuse one when retrying. SQL write requests made directly to the protocol must provide the header; reusing it for a different request or branch is rejected.
