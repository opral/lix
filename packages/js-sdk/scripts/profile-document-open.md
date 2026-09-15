# Selected document read profile

Build the SDK, then run `node packages/js-sdk/scripts/profile-document-open.mjs`
from the repository root. `PROFILE_FILE_COUNTS=16,1600` and `PROFILE_SAMPLES=5`
control fixture sizes and repeat count. The script creates and closes isolated
in-memory repositories. It does not contact or change a hosted repository.

It measures the metadata, prepared-document, and editor-content SQL shapes used
by Atelier, selecting a 105,611-byte CSV alongside unrelated files. Every read
must return exactly one file. Fixture creation is outside the timers. The first
sample is the first selected read after fixture creation, **not** a cold process
or a cold remote repository.

## 2026-09-15 measurement

Source: `6a8797283`, browser SDK built at the equivalent pre-merge `4cca4e40`,
Node WebAssembly fallback, in-memory storage, five samples per query.

| Unrelated files | Prepared read, first | Prepared read, warm median | Editor read, warm median |
| --- | ---: | ---: | ---: |
| 16 | 7.32 ms | 1.11 ms | 0.62 ms |
| 1,600 | 23.71 ms | 1.00 ms | 0.55 ms |

These numbers isolate local engine work. They exclude partial-replica hydration,
HTTP latency, OPFS, rendering, plugins, and the private production document.
They do not establish the cause of a slow production open.

Atelier's separate delayed-load regression reproduced two initial loader calls:
mount started one read, then the observer's initial frame restarted it. The
loader checks cancellation after SQL returns, so aborting it does not cancel the
SQL work. Loading from the first observed frame removes that duplicate;
coalescing updates also prevents overlapping reloads while a read is pending.
