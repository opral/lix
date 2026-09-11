# Public file-tree partial replica profile

This runs public `openLix`, real HTTP authority handlers, OPFS, and the production
on-demand/background worker. Run separately from the key/value profile.

1. Run ignored native test `handle::partial::browser_file_profile_authority::partial_browser_file_profile_authority`
   with `LIX_PARTIAL_PROFILE_MANIFEST=/tmp/partial-file-manifest.json` and
   `LIX_PARTIAL_PROFILE_STOP=/tmp/partial-file-stop`. Remove any stale stop file first.
2. Wait for `PARTIAL_BROWSER_AUTHORITY_READY`.
3. From `packages/storage-opfs`, run:
   ```sh
   LIX_PARTIAL_PROFILE_MANIFEST=/tmp/partial-file-manifest.json LIX_PARTIAL_PROFILE_RESULT=/tmp/partial-file-result.json npx vitest run --config vitest.partial-file-sync.config.ts --reporter=verbose
   ```
4. Create `/tmp/partial-file-stop` to stop listeners after the browser finishes.

The authority contains 16 or 1600 tiny files across up to 100 directories, a 96 KiB
selected file, and an unrelated 1 MiB file. Opening, first directory listing/count,
empty content lookup, selected content, remote empty-to-present publication, and
write preparation have separate timings. Thirty offline content updates, reads,
and counts are measured before closing/reopening OPFS offline. The directory
predicate is a path prefix; fixtures have no deeper descendants.

Transfers count decoded response bytes as the replica consumes them, including
attempts rejected before offline network I/O. A separate remote SQL session inserts
a file into the retained empty scope; its traffic is excluded from replica counters.
The harness blocks fetch and aborts active replica responses for disconnection;
it does not terminate the authority. Cold queries may hydrate broad metadata;
their cost is reported separately from bounded opening.

This is an integration benchmark, not a passing performance claim. It fails if
content needs foreground remote inputs after preparation or a retained empty
file query fails to acquire the newly inserted remote file.
