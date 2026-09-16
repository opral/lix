---
type: minor
---

Automatic SQL transactions now accept a retry limit.

Set `maxAutoCommitRetries` on JavaScript `execute()` and `executeBatch()`, or
use Rust's `.with_max_auto_commit_retries(...)`, to cap whole-operation retries
after transaction contention or an expired snapshot. Zero disables these
retries. Without an override, Lix retains its default recovery budgets.
Explicit transactions remain caller-controlled, and unknown commit outcomes
are never automatically re-executed. Remote idempotent writes can now retry
known transaction conflicts after checking for a committed receipt.

Errors from the retry loop include the number of replays and why retrying stopped.
