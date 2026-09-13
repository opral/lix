# Full replica conversion in a browser

This opt-in check uses a disposable canonical Memory authority and a complete
synthetic replica copied at a real acknowledged baseline. Native SQL creates
pending local changes and independent remote changes on two branches, including
binary files and a custom schema. The authority loses its first successful merge
response for each branch. No production repository or credential is used.

From the repository root, choose fresh output and stop paths, then start:

```sh
LIX_BROWSER_CONVERSION_MANIFEST=/tmp/lix-conversion-fixture.json \
LIX_BROWSER_CONVERSION_STOP=/tmp/lix-conversion-stop \
cargo test -p lix --features all-simulations,server-protocol,server-protocol-client \
  pending_browser_conversion_fixture_authority -- --ignored --nocapture
```

Wait for `Synthetic pending replica browser fixture ready`. The stop file must
not already exist. The fixture exports every registered storage space and its
real epoch pointer; it does not reconstruct approximate sync metadata.

Build the current browser SDK and OPFS package using their normal build commands.
In another terminal, from `packages/storage-opfs`:

```sh
LIX_BROWSER_CONVERSION_MANIFEST=/tmp/lix-conversion-fixture.json \
pnpm exec vitest run --config vitest.pending-conversion.config.ts
```

The browser installs those exact rows into a fresh OPFS database and calls the
public conversion API. After failures, it verifies every original source-bank
row byte-for-byte. After conversion and reopening, it verifies both branches,
local and remote file bytes, the custom-schema edit, and retained recovery data.
The Vite proxy only accepts a loopback authority.

Stop the fixture with `touch /tmp/lix-conversion-stop`. Start a fresh native
fixture for each run: successful conversion changes its authority. Delete only
the synthetic manifest and stop files when finished.

The ordinary browser suite separately runs `migration-read.browser.test.ts`.
That regression imports a frozen released snapshot through real OPFS while one
candidate page is slow enough for the real migration heartbeat to commit, then
checks all key/value rows and persistent reopening. It has no external server
prerequisite.
